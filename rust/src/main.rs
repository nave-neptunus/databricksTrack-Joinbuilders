// Shopping Sessions pipeline in Rust. This is the submitted pipeline (root Dockerfile).
// Env: DATA_DIR, OUTPUT_DIR, NCPU, TIMING (stage times on stderr), FORCE_SLOW (use the fallback).
//
// Fast engine: input files are memory-mapped; strings are identified by a 64-bit hash plus an
// independent 32-bit check instead of being interned. Valid event copies go straight into person
// partitions. A separate, compact event-ID stream identifies the copies to tombstone before
// sessions. Any detected hash collision (same 64-bit key, different check) or unsupported input
// sends the run to the previous exact engine in slow.rs, so the output never depends on luck.
mod slow;

use ahash::AHashMap as HashMap;
use rayon::prelude::*;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering::Relaxed};
use std::time::Instant;

// Linux defaults to THP=madvise on the scorer.  These large, dense tables are
// long-lived enough to benefit, but failure must never affect correctness.
#[cfg(target_os = "linux")]
unsafe extern "C" {
    fn madvise(addr: *mut std::ffi::c_void, length: usize, advice: i32) -> i32;
}

#[cfg(target_os = "linux")]
fn advise_huge<T>(v: &mut Vec<T>) {
    const MIN_BYTES: usize = 2 << 20;
    const PAGE: usize = 4096;
    const MADV_HUGEPAGE: i32 = 14;
    let bytes = v.capacity().saturating_mul(std::mem::size_of::<T>());
    if bytes < MIN_BYTES || bytes == 0 {
        return;
    }
    let start = (v.as_mut_ptr() as usize) & !(PAGE - 1);
    let end = (v.as_mut_ptr() as usize).saturating_add(bytes).saturating_add(PAGE - 1) & !(PAGE - 1);
    unsafe { let _ = madvise(start as *mut std::ffi::c_void, end - start, MADV_HUGEPAGE); }
}

#[cfg(not(target_os = "linux"))]
#[inline]
fn advise_huge<T>(_v: &mut Vec<T>) {}

const DAY: i64 = 86_400_000;
const GAP: i64 = 1_800_000;
const LOOK: i64 = 7 * DAY;

static mut T0: Option<Instant> = None;
fn lap(name: &str) {
    if std::env::var_os("TIMING").is_some() {
        #[allow(static_mut_refs)]
        let t = unsafe { T0.unwrap() };
        eprintln!("{:8.1} ms  {}", t.elapsed().as_secs_f64() * 1000.0, name);
    }
}

// ------------------------------------------------------------------ files
fn csv_files(dir: &str) -> Vec<String> {
    let mut v: Vec<String> = match std::fs::read_dir(dir) {
        Ok(rd) => rd
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .filter(|p| p.extension().map(|x| x == "csv").unwrap_or(false) && p.is_file())
            .map(|p| p.to_string_lossy().into_owned())
            .collect(),
        Err(_) => vec![],
    };
    v.sort();
    v
}

/// Memory-map every file (empty files map to nothing).
fn map_all(files: &[String]) -> Vec<Option<memmap2::Mmap>> {
    files
        .par_iter()
        .map(|f| {
            let file = std::fs::File::open(f).unwrap();
            if file.metadata().map(|m| m.len()).unwrap_or(0) == 0 {
                return None;
            }
            Some(unsafe { memmap2::Mmap::map(&file).unwrap() })
        })
        .collect()
}

fn as_slices(maps: &[Option<memmap2::Mmap>]) -> Vec<&[u8]> {
    maps.iter().map(|m| m.as_ref().map(|m| &m[..]).unwrap_or(&[])).collect()
}

/// Byte ranges (data lines only, header skipped) of roughly `target` bytes, split at '\n'.
fn chunks<'a>(bufs: &[&'a [u8]], target: usize) -> Vec<&'a [u8]> {
    let mut out = vec![];
    for &b in bufs {
        let start = match memchr::memchr(b'\n', b) {
            Some(i) => i + 1,
            None => continue,
        };
        let mut s = start;
        while s < b.len() {
            let mut e = (s + target).min(b.len());
            if e < b.len() {
                e = match memchr::memchr(b'\n', &b[e..]) {
                    Some(i) => e + i + 1,
                    None => b.len(),
                };
            }
            out.push(&b[s..e]);
            s = e;
        }
    }
    out
}

/// Iterate lines of a chunk ('\n' only; a trailing '\n' does not start a new line).
#[inline]
fn for_lines<'a>(c: &'a [u8], mut f: impl FnMut(&'a [u8])) {
    let mut s = 0;
    for i in memchr::memchr_iter(b'\n', c) {
        f(&c[s..i]);
        s = i + 1;
    }
    if s < c.len() {
        f(&c[s..]);
    }
}

/// Split a line into exactly N fields, unquoting; None if malformed.
#[inline]
fn fields<'a, const N: usize>(line: &'a [u8]) -> Option<[&'a [u8]; N]> {
    let mut out: [&[u8]; N] = [&[]; N];
    let mut k = 0;
    let mut s = 0;
    let n = line.len();
    let mut i = 0;
    loop {
        while i < n && line[i] != b',' {
            i += 1;
        }
        if k >= N {
            return None;
        }
        let mut f = &line[s..i];
        if memchr::memchr(b'"', f).is_some() {
            if f.len() < 2 || f[0] != b'"' || f[f.len() - 1] != b'"' {
                return None;
            }
            f = &f[1..f.len() - 1];
            if memchr::memchr(b'"', f).is_some() {
                return None;
            }
        }
        out[k] = f;
        k += 1;
        if i >= n {
            break;
        }
        i += 1;
        s = i;
    }
    if k == N && !line.is_empty() { Some(out) } else { None }
}

// A row ends at a newline regardless of quoting. Track separators with SIMD,
// then check the endpoints of each field and account for every quote in the row.
#[inline]
fn csv_rows<'a, const N: usize>(bytes: &'a [u8], mut row: impl FnMut(&'a [u8], [&'a [u8]; N])) {
    #[cfg(any(target_arch = "aarch64", all(target_arch = "x86_64", target_feature = "avx2")))]
    let bytes = {
        #[cfg(target_arch = "aarch64")]
        const WIDTH: usize = 16;
        #[cfg(target_arch = "aarch64")]
        const STRIDE: u32 = 4;
        #[cfg(target_arch = "x86_64")]
        const WIDTH: usize = 32;
        #[cfg(target_arch = "x86_64")]
        const STRIDE: u32 = 1;

        let mut start = 0;
        let mut field_start = 0;
        let mut count = 0;
        let mut quote_count = 0;
        let mut expected_quotes = 0;
        let mut values = [&[][..]; N];
        for base in (0..bytes.len() / WIDTH).map(|i| i * WIDTH) {
            // Each mask contains exactly one bit per matching byte, at STRIDE * index.
            // The loop takes only full vectors and every separator lies inside its vector.
            let (mut separators, mut quotes, newlines) = unsafe {
                #[cfg(target_arch = "aarch64")]
                {
                    use std::arch::aarch64::*;
                    let b = vld1q_u8(bytes.as_ptr().add(base));
                    let mask = |v| vget_lane_u64::<0>(vreinterpret_u64_u8(vshrn_n_u16::<4>(vreinterpretq_u16_u8(v)))) & 0x1111_1111_1111_1111;
                    let newline = vceqq_u8(b, vdupq_n_u8(b'\n'));
                    (mask(vorrq_u8(newline, vceqq_u8(b, vdupq_n_u8(b',')))), mask(vceqq_u8(b, vdupq_n_u8(b'"'))), mask(newline))
                }
                #[cfg(target_arch = "x86_64")]
                {
                    use std::arch::x86_64::*;
                    let b = _mm256_loadu_si256(bytes.as_ptr().add(base).cast());
                    let mask = |v| _mm256_movemask_epi8(v) as u32 as u64;
                    let newline = _mm256_cmpeq_epi8(b, _mm256_set1_epi8(b'\n' as i8));
                    (mask(_mm256_or_si256(newline, _mm256_cmpeq_epi8(b, _mm256_set1_epi8(b',' as i8)))), mask(_mm256_cmpeq_epi8(b, _mm256_set1_epi8(b'"' as i8))), mask(newline))
                }
            };
            while separators != 0 {
                let bit = separators & separators.wrapping_neg();
                let end = base + (bit.trailing_zeros() / STRIDE) as usize;
                separators ^= bit;
                if count < N {
                    // field_start is the previous separator + 1 (or the chunk start).
                    let mut field = unsafe { bytes.get_unchecked(field_start..end) };
                    if field.len() >= 2 && field[0] == b'"' && field[field.len() - 1] == b'"' {
                        field = unsafe { field.get_unchecked(1..field.len() - 1) };
                        expected_quotes += 2;
                    }
                    values[count] = field;
                    count += 1;
                } else {
                    count = N + 1;
                }
                field_start = end + 1;
                if newlines & bit != 0 {
                    let before = quotes & (bit - 1);
                    quotes ^= before;
                    if count == N && start != end && quote_count + before.count_ones() as usize == expected_quotes {
                        row(unsafe { bytes.get_unchecked(start..end) }, values);
                    }
                    start = end + 1;
                    count = 0;
                    quote_count = 0;
                    expected_quotes = 0;
                }
            }
            quote_count += quotes.count_ones() as usize;
        }
        &bytes[start..]
    };
    // The tail may start in the preceding vector, and may lack a final newline.
    for_lines(bytes, |line| {
        if let Some(values) = fields::<N>(line) {
            row(line, values);
        }
    });
}

/// Fixed-arity event rows already separated by the newline iterator.
/// Collect two masks for the whole row, then decode exactly six cuts.
#[cfg(any(all(target_arch = "aarch64", target_endian = "little"), all(target_arch = "x86_64", target_feature = "avx2")))]
#[inline]
fn event_fields(line: &[u8]) -> Option<[&[u8]; 7]> {
    #[cfg(target_arch = "aarch64")]
    const WIDTH: usize = 16;
    #[cfg(target_arch = "x86_64")]
    const WIDTH: usize = 32;

    if !(WIDTH..=128).contains(&line.len()) {
        return fields::<7>(line);
    }
    let masks = |offset: usize| {
        // The caller loads full vectors inside the row, overlapping the last
        // one when needed and discarding the overlapping lanes.
        unsafe {
            #[cfg(target_arch = "aarch64")]
            {
                use std::arch::aarch64::*;
                const WEIGHTS: [u8; 16] = [1, 2, 4, 8, 16, 32, 64, 128, 1, 2, 4, 8, 16, 32, 64, 128];
                let w = vld1q_u8(WEIGHTS.as_ptr());
                let v = vld1q_u8(line.as_ptr().add(offset));
                let comma = vandq_u8(w, vceqq_u8(v, vdupq_n_u8(b',')));
                let quote = vandq_u8(w, vceqq_u8(v, vdupq_n_u8(b'"')));
                let pairs = vpaddq_u8(comma, quote);
                let groups = vpaddq_u8(pairs, pairs);
                let bytes = vpaddq_u8(groups, groups);
                let both = vgetq_lane_u32::<0>(vreinterpretq_u32_u8(bytes));
                (both as u16 as u32, both >> 16)
            }
            #[cfg(target_arch = "x86_64")]
            {
                use std::arch::x86_64::*;
                let v = _mm256_loadu_si256(line.as_ptr().add(offset).cast());
                let comma = _mm256_movemask_epi8(_mm256_cmpeq_epi8(v, _mm256_set1_epi8(b',' as i8)));
                let quote = _mm256_movemask_epi8(_mm256_cmpeq_epi8(v, _mm256_set1_epi8(b'"' as i8)));
                (comma as u32, quote as u32)
            }
        }
    };
    let mut commas = 0u128;
    let mut quotes = 0u128;
    let mut offset = 0;
    while offset + WIDTH <= line.len() {
        let (c, q) = masks(offset);
        commas |= u128::from(c) << offset;
        quotes |= u128::from(q) << offset;
        offset += WIDTH;
    }
    if offset < line.len() {
        let start = line.len() - WIDTH;
        let (c, q) = masks(start);
        let overlap = offset - start;
        commas |= u128::from(c >> overlap) << offset;
        quotes |= u128::from(q >> overlap) << offset;
    }
    if commas.count_ones() != 6 {
        return None;
    }
    let mut cuts = [0; 8];
    for cut in &mut cuts[1..7] {
        *cut = commas.trailing_zeros() as usize + 1;
        commas &= commas - 1;
    }
    cuts[7] = line.len() + 1;
    if quotes == 0 {
        return Some(std::array::from_fn(|i| {
            // Six distinct cuts precede the terminal offset.
            unsafe { line.get_unchecked(cuts[i]..cuts[i + 1] - 1) }
        }));
    }
    let mut expected = 0;
    let values = std::array::from_fn(|i| {
        let start = cuts[i];
        let end = cuts[i + 1] - 1;
        // Both endpoint reads are in bounds even for empty edge fields.
        // An empty or single-byte field can never satisfy the length check.
        let first = unsafe { *line.get_unchecked(start.min(line.len() - 1)) };
        let last = unsafe { *line.get_unchecked(end.saturating_sub(1)) };
        let wrapped = ((end - start >= 2) & (first == b'"') & (last == b'"')) as usize;
        expected += 2 * wrapped as u32;
        unsafe { line.get_unchecked(start + wrapped..end - wrapped) }
    });
    (quotes.count_ones() == expected).then_some(values)
}

#[inline]
fn event_rows<'a>(bytes: &'a [u8], mut row: impl FnMut(&'a [u8], [&'a [u8]; 7])) {
    #[cfg(any(all(target_arch = "aarch64", target_endian = "little"), all(target_arch = "x86_64", target_feature = "avx2")))]
    for_lines(bytes, |line| {
        if let Some(values) = event_fields(line) {
            row(line, values);
        }
    });
    #[cfg(not(any(all(target_arch = "aarch64", target_endian = "little"), all(target_arch = "x86_64", target_feature = "avx2"))))]
    csv_rows(bytes, row);
}

#[cfg(test)]
#[test]
fn event_row_masks_match_scalar_at_simd_and_tail_boundaries() {
    let mut data = Vec::new();
    for width in 0..=145 {
        let text = "x".repeat(width);
        for row in [
            format!("{text},u,t,i,view,p,\n"),
            format!("\"{text}\",u,\"t\",i,view,p,\"7\"\n"),
            format!("e,u,t,i,view,p,{text}\"\n"),
            format!("e,\"{text}\"x,t,i,view,p,\n"),
            format!("e,u,t,i,view,p,{text},\n"),
            format!("e,u,{text},i,view,\n"),
        ] {
            data.extend_from_slice(row.as_bytes());
        }
    }
    // Exercise endpoint clamping with quotes elsewhere in the row, as well
    // as a single quote at the first or last field and an unterminated row.
    data.extend_from_slice(b"\n,u,\"2027-12-13T18:12:18Z\",i,view,p,\n");
    data.extend_from_slice(b"\",u,2027-12-13T18:12:18Z,i,view,p,\n");
    data.extend_from_slice(b"e,u,2027-12-13T18:12:18Z,i,view,p,\"\n");
    data.extend_from_slice(b"e,u,\"2027-12-13T18:12:18Z\",i,view,p,");

    let mut expected = Vec::new();
    for_lines(&data, |line| {
        if let Some(values) = fields::<7>(line) {
            expected.push((line, values));
        }
    });
    let mut actual = Vec::new();
    event_rows(&data, |line, values| actual.push((line, values)));
    assert_eq!(actual, expected);
}

/// Address ranges of the mapped files, to recover a whole line from a pointer into it.
struct Lines {
    ranges: Vec<(usize, usize)>,
}
impl Lines {
    fn new(bufs: &[&[u8]]) -> Lines {
        let mut ranges: Vec<(usize, usize)> =
            bufs.iter().filter(|b| !b.is_empty()).map(|b| (b.as_ptr() as usize, b.as_ptr() as usize + b.len())).collect();
        ranges.sort();
        Lines { ranges }
    }
    /// The line that starts at `p` (p must be a line start inside a mapped file).
    fn line<'a>(&self, p: u64) -> &'a [u8] {
        let p = p as usize;
        let i = self.ranges.partition_point(|r| r.0 <= p) - 1;
        let end = self.ranges[i].1;
        let s = unsafe { std::slice::from_raw_parts(p as *const u8, end - p) };
        match memchr::memchr(b'\n', s) {
            Some(j) => &s[..j],
            None => s,
        }
    }
}

// ------------------------------------------------------------------ timestamps
#[inline]
fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let doy = (153 * ((m + 9) % 12) + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146097 + doe - 719468
}
#[inline]
fn leap(y: i64) -> bool {
    y % 4 == 0 && (y % 100 != 0 || y % 400 == 0)
}
#[inline]
fn d2(s: &[u8], i: usize) -> Option<i64> {
    let a = s[i].wrapping_sub(b'0');
    let b = s[i + 1].wrapping_sub(b'0');
    if a < 10 && b < 10 { Some((a * 10 + b) as i64) } else { None }
}

// One small cache per parsing task. Store all ten date bytes, so a hit also
// verifies the calendar date and punctuation validated by the cold path.
#[derive(Default)]
struct Dates([([u8; 10], i64); 32]);

impl Dates {
    #[inline]
    fn parse(&mut self, s: &[u8]) -> Option<i64> {
        // The date separator excludes the all-zero key used by empty slots.
        if s.len() == 20 && s[19] == b'Z' && s[4] == b'-' && s[10] == b'T' && s[13] == b':' && s[16] == b':' {
            let date: [u8; 10] = s[..10].try_into().unwrap();
            let index = (date[8] as usize * 10 + date[9] as usize) & 31;
            let entry = &mut self.0[index];
            let h = d2(s, 11)?;
            let m = d2(s, 14)?;
            let sec = d2(s, 17)?;
            if h > 23 || m > 59 || sec > 59 {
                return None;
            }
            let time = (h * 3600 + m * 60 + sec) * 1000;
            if entry.0 == date {
                return Some(entry.1 + time);
            }
            let t = parse_ts(s)?;
            *entry = (date, t - time);
            return Some(t);
        }
        if !s.is_empty() && s.len() <= 18 {
            let mut value = 0;
            for &c in s {
                let digit = c.wrapping_sub(b'0');
                if digit > 9 {
                    return None;
                }
                value = value * 10 + i64::from(digit);
            }
            return Some(value);
        }
        parse_ts(s)
    }
}

#[cfg(test)]
#[test]
fn empty_date_cache_rejects_invalid_date() {
    let mut cache = Dates::default();
    assert_eq!(cache.parse(b"\0\0\0\0\0\0\0\0\0\0T00:00:00Z"), None);
}

/// ms since epoch, or None if bad. Overlong F3 saturates to i64::MAX (out of any window).
#[inline]
fn parse_ts(s: &[u8]) -> Option<i64> {
    if s.is_empty() {
        return None;
    }
    if s.iter().all(|c| c.is_ascii_digit()) {
        let mut v: i64 = 0;
        for &c in s {
            v = match v.checked_mul(10).and_then(|x| x.checked_add((c - b'0') as i64)) {
                Some(x) => x,
                None => return Some(i64::MAX),
            };
        }
        return Some(v);
    }
    let off = match s.len() {
        20 if s[19] == b'Z' => 0,
        25 if (s[19] == b'+' || s[19] == b'-') && s[22] == b':' => {
            let oh = d2(s, 20)?;
            let om = d2(s, 23)?;
            if oh > 23 || om > 59 {
                return None;
            }
            let o = (oh * 60 + om) * 60_000;
            if s[19] == b'+' { o } else { -o }
        }
        _ => return None,
    };
    if s[4] != b'-' || s[7] != b'-' || s[10] != b'T' || s[13] != b':' || s[16] != b':' {
        return None;
    }
    let y = d2(s, 0)? * 100 + d2(s, 2)?;
    let mo = d2(s, 5)?;
    let d = d2(s, 8)?;
    let h = d2(s, 11)?;
    let mi = d2(s, 14)?;
    let se = d2(s, 17)?;
    if !(1..=12).contains(&mo) || h > 23 || mi > 59 || se > 59 || d < 1 {
        return None;
    }
    let dim = [31, if leap(y) { 29 } else { 28 }, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31][(mo - 1) as usize];
    if d > dim {
        return None;
    }
    Some((days_from_civil(y, mo, d) * 86400 + h * 3600 + mi * 60 + se) * 1000 - off)
}

fn manifest_ts(m: &str, key: &str) -> i64 {
    let i = m.find(key).unwrap() + key.len();
    let rest = &m[i..];
    let a = rest.find('"').unwrap();
    let rest = &rest[a + 1..];
    let b = rest.find('"').unwrap();
    parse_ts(rest[..b].as_bytes()).unwrap()
}

/// price_cents of a purchase: digits only, 1..=10,000,000.
#[inline]
fn parse_price(p: &[u8]) -> Option<u32> {
    if p.is_empty() || !p.iter().all(|c| c.is_ascii_digit()) {
        return None;
    }
    let z = p.iter().take_while(|&&c| c == b'0').count();
    let p = &p[z..];
    if p.len() > 8 {
        return None;
    }
    let v: u32 = p.iter().fold(0, |a, &c| a * 10 + (c - b'0') as u32);
    if (1..=10_000_000).contains(&v) { Some(v) } else { None }
}

// ------------------------------------------------------------------ hashing
#[inline]
fn mum(a: u64, b: u64) -> u64 {
    let r = (a as u128) * (b as u128);
    (r as u64) ^ ((r >> 64) as u64)
}
#[inline]
fn rd8(b: &[u8], i: usize) -> u64 {
    u64::from_le_bytes(b[i..i + 8].try_into().unwrap())
}
#[inline]
fn rd4(b: &[u8], i: usize) -> u64 {
    u32::from_le_bytes(b[i..i + 4].try_into().unwrap()) as u64
}
const K0: u64 = 0xa076_1d64_78bd_642f;
const K1: u64 = 0xe703_7ed1_a0b4_28db;
const K2: u64 = 0x8ebc_6af0_9c88_c6e3;
const K3: u64 = 0x5899_65cc_7537_4cc3;
const K4: u64 = 0x1d8e_4e27_c47d_124f;

/// (64-bit key, independent 32-bit check) of a byte string. For strings up to 16 bytes the
/// (a, c, len) triple is the string itself, so a false match needs both mixes to collide.
#[inline]
fn hs(b: &[u8], seed: u64) -> (u64, u32) {
    let n = b.len();
    let (a, c);
    if n <= 16 {
        if n >= 8 {
            a = rd8(b, 0);
            c = rd8(b, n - 8);
        } else if n >= 4 {
            a = rd4(b, 0);
            c = rd4(b, n - 4);
        } else if n > 0 {
            a = ((b[0] as u64) << 16) | ((b[n >> 1] as u64) << 8) | b[n - 1] as u64;
            c = 0;
        } else {
            a = 0;
            c = 0;
        }
    } else {
        // two independent lanes over 16-byte blocks, then the last 16 bytes
        let mut s1 = seed ^ K0;
        let mut s2 = seed ^ K3;
        let mut i = 0;
        while i + 16 < n {
            let x = rd8(b, i);
            let y = rd8(b, i + 8);
            s1 = mum(x ^ s1 ^ K1, y ^ K2);
            s2 = mum(y ^ s2 ^ K3, x ^ K4);
            i += 16;
        }
        a = s1 ^ rd8(b, n - 16);
        c = s2 ^ rd8(b, n - 8);
    }
    let l = n as u64;
    let key = mum(a ^ seed ^ K0, c ^ seed.rotate_left(17) ^ K1 ^ l);
    let chk = mum(a ^ seed.rotate_left(31) ^ K2, c ^ K3 ^ l.rotate_left(32)) as u32;
    (key, chk)
}

/// Open-addressing map from a 64-bit key (plus check) to a u32 value.
struct Tab {
    slots: Vec<(u64, u64)>, // (key, (chk << 32) | val); key 0 = empty
    mask: usize,
}
impl Tab {
    fn new(n: usize) -> Tab {
        let cap = (n * 2).next_power_of_two().max(16);
        let mut slots = vec![(0, 0); cap];
        advise_huge(&mut slots);
        Tab { slots, mask: cap - 1 }
    }
    #[inline]
    fn prefetch(&self, key: u64) {
        let index = (if key == 0 { 1 } else { key }) as usize & self.mask;
        let ptr = self.slots.as_ptr().wrapping_add(index);
        #[cfg(target_arch = "aarch64")]
        unsafe {
            std::arch::asm!("prfm pldl1keep, [{ptr}]", ptr = in(reg) ptr, options(nomem, nostack, preserves_flags));
        }
        #[cfg(target_arch = "x86_64")]
        unsafe {
            std::arch::x86_64::_mm_prefetch::<{ std::arch::x86_64::_MM_HINT_T0 }>(ptr.cast());
        }
        #[cfg(not(any(target_arch = "aarch64", target_arch = "x86_64")))]
        let _ = ptr;
    }
    #[inline]
    fn get(&self, k: u64, c: u32, coll: &AtomicBool) -> Option<u32> {
        let k = if k == 0 { 1 } else { k };
        let mut i = (k as usize) & self.mask;
        loop {
            let (kk, vv) = self.slots[i];
            if kk == 0 {
                return None;
            }
            if kk == k {
                if (vv >> 32) as u32 != c {
                    coll.store(true, Relaxed);
                    return None;
                }
                return Some(vv as u32);
            }
            i = (i + 1) & self.mask;
        }
    }
}

// ------------------------------------------------------------------ records
const PERSON_LOW_MASK: u64 = (1 << (64 - PB)) - 1;
const LINE_REF_MASK: u64 = (1 << 40) - 1;
const TOMBSTONE: u32 = u32::MAX;
const UNPARSED_INGEST: u64 = 1 << 63;
const NO_EVENT: u64 = UNPARSED_INGEST - 1;

#[repr(C)]
#[derive(Clone, Copy)]
struct PersonData {
    key_check: u64,
    line_check: u64,
}

impl PersonData {
    #[inline]
    fn new(key: u64, check: u32, line_ref: u64) -> Self {
        Self {
            key_check: (key & PERSON_LOW_MASK) | (u64::from(check) << (64 - PB)),
            line_check: line_ref | (u64::from(check >> PB) << 40),
        }
    }

    #[inline]
    fn key_low(self) -> u64 {
        self.key_check & PERSON_LOW_MASK
    }

    #[inline]
    fn check(self) -> u32 {
        (self.key_check >> (64 - PB)) as u32 | ((self.line_check >> 40) as u32) << PB
    }

    #[inline]
    fn prefetch_line(self, files: &[&[u8]]) {
        let line_ref = self.line_check & LINE_REF_MASK;
        let file = files[(line_ref >> 32) as usize];
        let ptr = file.as_ptr().wrapping_add(line_ref as u32 as usize);
        #[cfg(target_arch = "aarch64")]
        unsafe {
            std::arch::asm!("prfm pldl1keep, [{ptr}]", ptr = in(reg) ptr, options(nomem, nostack, preserves_flags));
        }
        #[cfg(target_arch = "x86_64")]
        unsafe {
            std::arch::x86_64::_mm_prefetch::<{ std::arch::x86_64::_MM_HINT_T0 }>(ptr.cast());
        }
        #[cfg(not(any(target_arch = "aarch64", target_arch = "x86_64")))]
        let _ = ptr;
    }

    #[inline]
    fn line<'a>(self, files: &[&'a [u8]]) -> &'a [u8] {
        let line_ref = self.line_check & LINE_REF_MASK;
        let file = files[(line_ref >> 32) as usize];
        let tail = &file[line_ref as u32 as usize..];
        &tail[..memchr::memchr(b'\n', tail).unwrap_or(tail.len())]
    }
}

/// One valid event copy in its final person partition. Only the timestamp
/// can change during dedup, and only by the shard owning this copy's event ID.
#[repr(C)]
struct EvP {
    person: PersonData,
    ts: AtomicU32,
    price: u32,
}

impl EvP {
    #[inline]
    fn live(&self) -> Option<LiveEvent> {
        let ts = self.ts.load(Relaxed);
        (ts != TOMBSTONE).then(|| LiveEvent { person: self.person, ts, price: self.price })
    }
}

/// Session-local sortable copy. The person partition supplies the top key bits.
#[repr(C)]
#[derive(Clone, Copy)]
struct LiveEvent {
    person: PersonData,
    ts: u32,
    price: u32,
}

/// Every well-formed event ID has a record, even when its copy is invalid.
#[repr(C)]
#[derive(Clone, Copy)]
struct D {
    hid: u64,
    loc: u64, // partition in bits 32..42, row in low 32; NO_EVENT if invalid
    ingest: i32,
    hchk: u32,
}

impl D {
    #[inline]
    fn order(self) -> (i32, bool) {
        (self.ingest, self.loc & UNPARSED_INGEST != 0)
    }

    #[inline]
    fn tombstone(self, fold: &EventFold) {
        if self.loc < NO_EVENT {
            let partition = (self.loc >> 32) as usize;
            let index = self.loc as u32 as usize;
            fold.people[partition][index].ts.store(TOMBSTONE, Relaxed);
        }
    }
}

/// Reversibly represent a common ID shape. The high bit of each ASCII byte
/// is spare, so the prefix's case and the tail length can be kept in-band.
/// All other IDs continue through the original hash-plus-check dedup path.
#[inline]
fn short_event_key(id: &[u8]) -> Option<u64> {
    let prefix = *id.first()?;
    if prefix | 32 != b'e' {
        return None;
    }
    let (tail, long) = match id.len() {
        8 => (rd8(id, 0) >> 8, false),
        9 => (rd8(id, 1), true),
        _ => return None,
    };
    if tail & 0x8080_8080_8080_8080 != 0 {
        return None;
    }
    let tagged = tail | (u64::from(prefix == b'E') << 63) | (u64::from(long) << 55);
    Some(tagged.wrapping_mul(0x9e37_79b9_7f4a_7c15).rotate_left(23))
}

const SHORT_ROW_BITS: u32 = 31 - PB;
const SHORT_ROW_LIMIT: usize = 1 << SHORT_ROW_BITS;
const SHORT_NO_EVENT: u32 = 1 << 31;
const SHORT_UNPARSED: u32 = SHORT_NO_EVENT | 1;

#[repr(C)]
#[derive(Clone, Copy)]
struct ShortD {
    key: u64,
    loc: u32,
    ingest: i32,
}

impl ShortD {
    #[inline]
    fn pack(copy: D) -> Option<Self> {
        let loc = if copy.loc < NO_EVENT {
            let part = (copy.loc >> 32) as u32;
            let row = copy.loc as u32;
            if row as usize >= SHORT_ROW_LIMIT {
                return None;
            }
            (part << SHORT_ROW_BITS) | row
        } else if copy.loc & UNPARSED_INGEST != 0 {
            SHORT_UNPARSED
        } else {
            SHORT_NO_EVENT
        };
        Some(Self { key: copy.hid, loc, ingest: copy.ingest })
    }
}

impl From<ShortD> for D {
    #[inline]
    fn from(copy: ShortD) -> Self {
        let loc = if copy.loc < SHORT_NO_EVENT {
            let part = u64::from(copy.loc >> SHORT_ROW_BITS);
            let row = u64::from(copy.loc) & (SHORT_ROW_LIMIT as u64 - 1);
            (part << 32) | row
        } else if copy.loc == SHORT_UNPARSED {
            NO_EVENT | UNPARSED_INGEST
        } else {
            NO_EVENT
        };
        // In this stream the full key is a bijection of the ID bytes.
        D { hid: copy.key, loc, ingest: copy.ingest, hchk: 0 }
    }
}

const _: () = assert!(std::mem::size_of::<EvP>() == 24);
const _: () = assert!(std::mem::size_of::<LiveEvent>() == 24);
const _: () = assert!(std::mem::size_of::<D>() == 24);
const _: () = assert!(std::mem::size_of::<ShortD>() == 16);

/// The hot dedup stream has one anonymous mapping per worker. Each shard
/// owns an initialized prefix of its reservation and a growable overflow.
struct ShortArena {
    storage: Option<memmap2::MmapMut>,
    stride: usize,
    lengths: [usize; SH],
    overflow: Vec<Vec<ShortD>>,
}

impl ShortArena {
    fn new(reservation: usize) -> Self {
        let storage = if reservation == 0 {
            None
        } else {
            memmap2::MmapMut::map_anon(reservation * SH * std::mem::size_of::<ShortD>()).ok()
        };
        #[cfg(target_os = "linux")]
        if let Some(mapping) = &storage {
            let _ = mapping.advise(memmap2::Advice::HugePage);
        }
        let stride = if storage.is_some() { reservation } else { 0 };
        Self { storage, stride, lengths: [0; SH], overflow: (0..SH).map(|_| Vec::new()).collect() }
    }

    fn writer(&mut self) -> ShortWriter<'_> {
        let base = self.storage.as_mut().map_or(std::ptr::null_mut(), |m| m.as_mut_ptr().cast());
        ShortWriter { base, stride: self.stride, lengths: &mut self.lengths, overflow: &mut self.overflow }
    }

    #[inline]
    fn records(&self, shard: usize) -> [&[ShortD]; 2] {
        let prefix = self.storage.as_ref().map_or(&[][..], |mapping| {
            let ptr = mapping.as_ptr().cast::<ShortD>();
            // Only the recorded prefix has been initialized. No writes can
            // occur through this shared borrow.
            unsafe { std::slice::from_raw_parts(ptr.add(shard * self.stride), self.lengths[shard]) }
        });
        [prefix, &self.overflow[shard]]
    }
}

struct ShortWriter<'a> {
    base: *mut ShortD,
    stride: usize,
    lengths: &'a mut [usize; SH],
    overflow: &'a mut [Vec<ShortD>],
}

impl ShortWriter<'_> {
    #[inline]
    fn push(&mut self, shard: usize, row: ShortD) {
        let len = self.lengths[shard];
        if len < self.stride {
            // A nonzero stride requires a page-aligned mapping owned by the
            // borrowed arena. This worker exclusively owns the next slot.
            unsafe { self.base.add(shard * self.stride + len).write(row) };
            self.lengths[shard] += 1;
        } else {
            self.overflow[shard].push(row);
        }
    }
}

struct EventFold {
    copies: Vec<Vec<D>>,
    short_copies: ShortArena,
    people: Vec<Vec<EvP>>,
}

impl EventFold {
    fn new(shard_capacity: usize) -> Self {
        let person_capacity = shard_capacity * SH / P;
        Self {
            copies: (0..SH).map(|_| Vec::new()).collect(),
            short_copies: ShortArena::new(shard_capacity),
            people: (0..P).map(|_| Vec::with_capacity(person_capacity)).collect(),
        }
    }
}

/// Temporary event shape, used only for the prefetch batch.
struct ParsedEvent {
    copy: D,
    line_ref: u64,
    pkey: u64,
    pchk: u32,
    ts: u32,
    price: u32,
    short_id: bool,
}

#[inline]
fn compact_ingest(ms: i64, window_seconds: i64) -> Option<i32> {
    if ms % 1000 != 0 {
        return None;
    }
    (ms / 1000 - window_seconds).try_into().ok()
}

fn event_sources_fit(count: usize) -> bool {
    count <= 256
}

/// Per-worker scratch. Each cell holds an epoch, a five-bit hash tag and
/// the packed fold/row of the winning copy. Tags only filter; compare full hashes.
const DEDUP_INDEX_BITS: u32 = 20;
const DEDUP_INDEX_LIMIT: usize = 1 << DEDUP_INDEX_BITS;
const DEDUP_EPOCH_SHIFT: u32 = 25;

#[derive(Default)]
struct Dedup {
    cells: Vec<u32>,
    epoch: u8,
    mask: usize,
}

impl Dedup {
    fn start(&mut self, n: usize) {
        let cap = (n * 2).next_power_of_two().max(16);
        if self.cells.len() < cap {
            self.cells.resize(cap, 0);
        }
        self.epoch += 1;
        if self.epoch == 128 {
            self.cells.fill(0);
            self.epoch = 1;
        }
        self.mask = cap - 1;
    }

    fn shard<R: Copy + Into<D>>(
        &mut self, folds: &[EventFold], shard: usize, coll: &AtomicBool,
        records: impl Fn(&EventFold, usize) -> [&[R]; 2],
    ) {
        let mut count = 0;
        let mut max_fold = 1;
        for fold in folds {
            let [prefix, overflow] = records(fold, shard);
            let n = prefix.len() + overflow.len();
            count += n;
            max_fold = max_fold.max(n);
        }
        if count == 0 {
            return;
        }
        if count > DEDUP_INDEX_LIMIT {
            coll.store(true, Relaxed);
            return;
        }
        let stride = max_fold.next_power_of_two();
        if folds.len() > DEDUP_INDEX_LIMIT / stride {
            coll.store(true, Relaxed);
            return;
        }
        let fold_shift = stride.trailing_zeros();
        let row_mask = stride - 1;
        self.start(count);

        let epoch = u32::from(self.epoch);
        for (fold_index, fold) in folds.iter().enumerate() {
            let fold_base = fold_index << fold_shift;
            for (row_index, &event) in records(fold, shard).into_iter().flatten().enumerate() {
                let event: D = event.into();
                let tag = (epoch << DEDUP_EPOCH_SHIFT)
                    | (((event.hid >> 32) as u32 & 31) << DEDUP_INDEX_BITS);
                let new_entry = tag | (fold_base | row_index) as u32;
                let mut i = event.hid as usize & self.mask;
                loop {
                    let entry = self.cells[i];
                    if entry >> DEDUP_EPOCH_SHIFT != epoch {
                        self.cells[i] = new_entry;
                        break;
                    }
                    if entry & !((DEDUP_INDEX_LIMIT as u32) - 1) == tag {
                        let index = entry as usize & (DEDUP_INDEX_LIMIT - 1);
                        let winner_fold = &folds[index >> fold_shift];
                        let [prefix, overflow] = records(winner_fold, shard);
                        let row = index & row_mask;
                        let winner: D = prefix.get(row).copied().unwrap_or_else(|| overflow[row - prefix.len()]).into();
                        if winner.hid == event.hid {
                            if winner.hchk != event.hchk {
                                coll.store(true, Relaxed);
                            } else if event.order() < winner.order() {
                                winner.tombstone(winner_fold);
                                self.cells[i] = new_entry;
                            } else {
                                event.tombstone(fold);
                            }
                            break;
                        }
                    }
                    i = (i + 1) & self.mask;
                }
            }
        }
    }
}

#[cfg(test)]
mod single_scatter_tests {
    use super::*;

    fn copy(fold: &mut EventFold, ingest: i32, partition: Option<usize>) {
        let loc = if let Some(p) = partition {
            let index = fold.people[p].len();
            fold.people[p].push(EvP {
                person: PersonData::new((p as u64) << (64 - PB), 0, 0),
                ts: AtomicU32::new(1),
                price: 0,
            });
            ((p as u64) << 32) | index as u64
        } else {
            NO_EVENT
        };
        fold.copies[0].push(D { hid: 7, hchk: 9, ingest, loc });
    }

    fn dedup(folds: &[EventFold]) {
        let coll = AtomicBool::new(false);
        Dedup::default().shard(folds, 0, &coll, |f, s| [&f.copies[s], &[]]);
        assert!(!coll.load(Relaxed));
    }

    #[test]
    fn earliest_invalid_copy_tombstones_all_valid_copies() {
        let mut folds = [EventFold::new(0), EventFold::new(0)];
        copy(&mut folds[0], 2, Some(3));
        copy(&mut folds[0], 3, Some(5));
        copy(&mut folds[1], 1, None);
        dedup(&folds);
        assert!(folds[0].people[3][0].live().is_none());
        assert!(folds[0].people[5][0].live().is_none());
    }

    #[test]
    fn later_duplicate_with_different_person_is_tombstoned() {
        let mut folds = [EventFold::new(0), EventFold::new(0)];
        copy(&mut folds[0], 2, Some(1000));
        copy(&mut folds[1], 1, Some(1));
        dedup(&folds);
        assert!(folds[0].people[1000][0].live().is_none());
        assert!(folds[1].people[1][0].live().is_some());
    }

    #[test]
    fn equal_ingest_valid_duplicates_leave_exactly_one_live_copy() {
        let mut folds = [EventFold::new(0), EventFold::new(0)];
        copy(&mut folds[0], 1, Some(3));
        copy(&mut folds[1], 1, Some(3));
        copy(&mut folds[1], 1, Some(3));
        dedup(&folds);
        assert_eq!(folds.iter().flat_map(|f| &f.people[3]).filter_map(EvP::live).count(), 1);
    }

    #[test]
    fn unparseable_ingest_loses_to_largest_parsed_ingest() {
        let mut folds = [EventFold::new(0)];
        folds[0].copies[0].push(D { hid: 7, hchk: 9, ingest: i32::MAX, loc: NO_EVENT | UNPARSED_INGEST });
        copy(&mut folds[0], i32::MAX, Some(3));
        dedup(&folds);
        assert!(folds[0].people[3][0].live().is_some());
        assert_eq!(compact_ingest(i64::from(i32::MAX) * 1000, 0), Some(i32::MAX));
        assert_eq!(compact_ingest(i64::from(i32::MAX) * 1000 + 1000, 0), None);
        assert_eq!(compact_ingest(1001, 0), None);
    }

    #[test]
    fn short_key_preserves_case_length_and_all_ascii_bytes() {
        let lower = short_event_key(b"eabcdefg").unwrap();
        assert_ne!(Some(lower), short_event_key(b"Eabcdefg"));
        assert_ne!(Some(lower), short_event_key(b"eabcdefg\0"));
        assert_ne!(Some(lower), short_event_key(b"eabcdefh"));
        assert_eq!(short_event_key(b"eabcdef\x80"), None);
        assert_eq!(short_event_key(b"uabcdefg"), None);
    }

    #[test]
    fn short_dedup_preserves_winner_and_target_partition() {
        let mut folds = [EventFold::new(0), EventFold::new(0)];
        copy(&mut folds[0], 20, Some(P - 1));
        copy(&mut folds[1], 10, Some(1));
        for fold in &mut folds {
            let mut record = fold.copies[0].pop().unwrap();
            record.hid = short_event_key(b"e01234567").unwrap();
            fold.short_copies.writer().push(0, ShortD::pack(record).unwrap());
        }
        let coll = AtomicBool::new(false);
        Dedup::default().shard(&folds, 0, &coll, |f, s| f.short_copies.records(s));
        assert!(!coll.load(Relaxed));
        assert!(folds[0].people[P - 1][0].live().is_none());
        assert!(folds[1].people[1][0].live().is_some());
    }

    #[test]
    fn short_arena_reads_reservation_before_overflow() {
        let mut arena = ShortArena::new(1);
        {
            let mut writer = arena.writer();
            for key in 0..3 {
                writer.push(SH - 1, ShortD { key, loc: SHORT_NO_EVENT, ingest: 0 });
            }
        }
        let [prefix, overflow] = arena.records(SH - 1);
        if arena.storage.is_some() {
            assert_eq!((prefix.len(), overflow.len()), (1, 2));
        }
        assert_eq!(prefix.iter().chain(overflow).map(|r| r.key).collect::<Vec<_>>(), [0, 1, 2]);
    }
}

/// A valid click, routed to its person's partition.
#[derive(Clone, Copy)]
struct CkB {
    pkey: u64,
    ts: i64,
    line: u64,
    pchk: u32,
    camp: u32,
}

const SHB: u32 = 8; // dedup shards by event-id hash
const SH: usize = 1 << SHB;
const PB: u32 = 10; // person partitions
const P: usize = 1 << PB;

struct UdRow {
    // Pointer and length packed as ptr | len << 48.  Kept only after aggregation so
    // parquet needn't parse and hash a representative event line for every output row.
    // `fast` declines if either part cannot be represented.
    name: u64,
    day: i32,
    ns: i64,
    ne: i64,
    nr: i64,
    top: u32, // category index, u32::MAX = null
}

#[inline]
fn pack_ref(s: &[u8]) -> Option<u64> {
    let p = s.as_ptr() as u64;
    if p >> 48 != 0 || s.len() >= (1 << 16) { None } else { Some(p | ((s.len() as u64) << 48)) }
}
#[inline]
fn unpack_ref(r: u64) -> &'static [u8] {
    unsafe { std::slice::from_raw_parts((r & ((1u64 << 48) - 1)) as *const u8, (r >> 48) as usize) }
}

fn main() {
    unsafe { T0 = Some(Instant::now()) };
    if std::env::var_os("FORCE_SLOW").is_some() || !fast(0x243f_6a88_85a3_08d3) {
        if std::env::var_os("TIMING").is_some() {
            eprintln!("fast engine declined; running the exact fallback");
        }
        slow::run();
    }
    std::process::exit(0);
}

/// Runs the fast engine. Returns false (having written nothing) if it cannot guarantee exactness.
fn fast(seed: u64) -> bool {
    let data = std::env::var("DATA_DIR").unwrap_or("/data".into());
    let out = std::env::var("OUTPUT_DIR").unwrap_or("/output".into());
    let ncpu: usize = std::env::var("NCPU").ok().and_then(|s| s.parse().ok()).unwrap_or(8);
    rayon::ThreadPoolBuilder::new().num_threads(ncpu).build_global().ok();

    let man = std::fs::read_to_string(format!("{data}/manifest.json")).unwrap();
    let w0 = manifest_ts(&man, "\"window_start\"");
    let w1 = manifest_ts(&man, "\"window_end\"");
    if w1 <= w0 || w1 - w0 >= u32::MAX as i64 {
        return false;
    }
    let day0 = w0.div_euclid(DAY);
    let ndays = ((w1 - 1).div_euclid(DAY) - day0 + 1) as usize;

    let evf = csv_files(&format!("{data}/events"));
    if !event_sources_fit(evf.len()) {
        return false;
    }
    let ckf = csv_files(&format!("{data}/clicks"));
    let lkf = csv_files(&format!("{data}/links"));
    let evm = map_all(&evf);
    let ckm = map_all(&ckf);
    let lkm = map_all(&lkf);
    let smm = map_all(&[format!("{data}/products.csv"), format!("{data}/campaigns.csv")]);
    let evb = as_slices(&evm);
    if evb.iter().any(|file| file.len() > u32::MAX as usize) {
        return false;
    }
    let ckb = as_slices(&ckm);
    let lkb = as_slices(&lkm);
    let smb = as_slices(&smm);
    let ck_lines = Lines::new(&ckb);
    lap("mapped files");

    // products / campaigns
    let skip_header = |b: &[u8]| -> usize { memchr::memchr(b'\n', b).map(|i| i + 1).unwrap_or(b.len()) };
    let mut cat_names: Vec<&[u8]> = vec![];
    let mut cat_idx: HashMap<&[u8], u32> = HashMap::new();
    let mut prod: HashMap<&[u8], u32> = HashMap::new();
    for_lines(&smb[0][skip_header(smb[0])..], |l| {
        if let Some([p, c]) = fields::<2>(l) {
            let n = cat_names.len() as u32;
            let ci = *cat_idx.entry(c).or_insert_with(|| {
                cat_names.push(c);
                n
            });
            prod.entry(p).or_insert(ci);
        }
    });
    let unknown = cat_names.len() as u32;
    cat_names.push(b"unknown");
    if cat_names.len() > 255 {
        return false; // category must fit in 8 bits next to the price
    }
    let mut camp_names: Vec<&[u8]> = vec![];
    let mut camp_chan: Vec<&[u8]> = vec![];
    let mut camp: HashMap<&[u8], u32> = HashMap::new();
    for_lines(&smb[1][skip_header(smb[1])..], |l| {
        if let Some([c, ch]) = fields::<2>(l) {
            if !c.is_empty() && !camp.contains_key(c) {
                camp.insert(c, camp_names.len() as u32);
                camp_names.push(c);
                camp_chan.push(ch);
            }
        }
    });
    let ncamp = camp_names.len();

    // links: union-find over hashed ids
    let lch = chunks(&lkb, 1 << 20);
    // parse and hash both ends in parallel; the serial union-find then only probes the table
    struct End<'a> {
        s: &'a [u8],
        k: u64,
        c: u32,
    }
    let edges: Vec<[End; 2]> = lch
        .par_iter()
        .flat_map_iter(|c| {
            let mut v = vec![];
            for_lines(c, |l| {
                if let Some([a, b]) = fields::<2>(l) {
                    if !a.is_empty() && !b.is_empty() {
                        let (ka, ca) = hs(a, seed);
                        let (kb, cb) = hs(b, seed);
                        v.push([End { s: a, k: ka, c: ca }, End { s: b, k: kb, c: cb }]);
                    }
                }
            });
            v
        })
        .collect();
    lap("  links parsed+hashed");
    // Parallel, lock-free build. Each distinct id owns one table slot (claimed by CAS on the
    // key), and the slot index is its union-find node. Unions link the larger root under the
    // smaller by CAS; the components do not depend on the order. Capacity > 2 * edges >= ids,
    // so the table always keeps an empty slot (sized for a low load: most event lookups miss).
    let mut ltab = Tab::new(edges.len() * 2 + 16);
    let cap = ltab.slots.len();
    let mask = ltab.mask;
    let lcoll = AtomicBool::new(false);
    let (nids, pname) = {
        use std::sync::atomic::{AtomicU32, AtomicU64, Ordering::{Acquire, Release}};
        // the slots as atomics: (key, (chk << 32) | 1) while building; AtomicU64 has u64's layout
        let sl: &[AtomicU64] = unsafe { std::slice::from_raw_parts(ltab.slots.as_mut_ptr() as *const AtomicU64, cap * 2) };
        // zeroed (calloc'd, so untouched pages cost nothing) arrays viewed as atomics
        fn zeroed<A>(n: usize) -> &'static [A] {
            let mut v: Vec<u64> = vec![0; n];
            advise_huge(&mut v);
            let p = v.as_ptr() as *const A;
            std::mem::forget(v);
            unsafe { std::slice::from_raw_parts(p, n * 8 / std::mem::size_of::<A>()) }
        }
        let refs: &[AtomicU64] = zeroed(cap);
        // parent[x] = 0: x is a root; else parent is node parent[x] - 1 (always < x)
        let parent: &[AtomicU32] = zeroed(cap.div_ceil(2));
        let best: &[AtomicU32] = zeroed(cap.div_ceil(2)); // slot + 1 of the smallest id, per root
        let pnum: &[AtomicU32] = zeroed(cap.div_ceil(2)); // person number, per root
        let insert = |e: &End| -> u32 {
            let k = if e.k == 0 { 1 } else { e.k };
            let tag = ((e.c as u64) << 32) | 1;
            let mut i = (k as usize) & mask;
            loop {
                let kk = sl[2 * i].load(Acquire);
                if kk == 0 {
                    match sl[2 * i].compare_exchange(0, k, Acquire, Acquire) {
                        Ok(_) => {
                            match pack_ref(e.s) {
                                Some(r) => refs[i].store(r, Relaxed),
                                None => lcoll.store(true, Relaxed),
                            }
                            sl[2 * i + 1].store(tag, Release);
                            return i as u32;
                        }
                        Err(_) => continue, // re-read this slot
                    }
                }
                if kk == k {
                    let mut v = sl[2 * i + 1].load(Acquire);
                    while v == 0 {
                        std::hint::spin_loop();
                        v = sl[2 * i + 1].load(Acquire);
                    }
                    if v != tag {
                        lcoll.store(true, Relaxed);
                    }
                    return i as u32;
                }
                i = (i + 1) & mask;
            }
        };
        let find = |mut x: u32| -> u32 {
            loop {
                let p = parent[x as usize].load(Relaxed);
                if p == 0 {
                    return x;
                }
                let g = parent[(p - 1) as usize].load(Relaxed);
                if g == 0 {
                    return p - 1;
                }
                // path halving; a stale write still points at an ancestor
                let _ = parent[x as usize].compare_exchange(p, g, Relaxed, Relaxed);
                x = g - 1;
            }
        };
        edges.par_chunks(4096).for_each(|ch| {
            for (ei, e) in ch.iter().enumerate() {
                if let Some(f) = ch.get(ei + 16) {
                    ltab_prefetch(sl, mask, f[0].k);
                    ltab_prefetch(sl, mask, f[1].k);
                }
                let (a, b) = (insert(&e[0]), insert(&e[1]));
                let (mut ra, mut rb) = (find(a), find(b));
                while ra != rb {
                    let (lo, hi) = (ra.min(rb), ra.max(rb));
                    match parent[hi as usize].compare_exchange(0, lo + 1, Relaxed, Relaxed) {
                        Ok(_) => break,
                        Err(_) => {
                            ra = find(lo);
                            rb = find(hi);
                        }
                    }
                }
            }
        });
        if lcoll.load(Relaxed) {
            return false;
        }
        lap("  links union-find");
        // one pass: root of every id (kept as root + 1, roots stay 0), the smallest id of each
        // component, and per-chunk counts of ids and roots
        let occ = |i: usize| sl[2 * i].load(Relaxed) != 0;
        let str_of = |i: u32| unpack_ref(refs[i as usize].load(Relaxed));
        const CH: usize = 1 << 14;
        let nch = cap.div_ceil(CH);
        let counts: Vec<(u32, u32)> = (0..nch)
            .into_par_iter()
            .map(|c| {
                let (mut ids, mut roots) = (0u32, 0u32);
                for i in c * CH..((c + 1) * CH).min(cap) {
                    if !occ(i) {
                        continue;
                    }
                    ids += 1;
                    let r = find(i as u32);
                    if r == i as u32 {
                        roots += 1;
                    } else {
                        parent[i].store(r + 1, Relaxed);
                    }
                    let me = str_of(i as u32);
                    let b = &best[r as usize];
                    let mut cur = b.load(Relaxed);
                    while cur == 0 || me < str_of(cur - 1) {
                        match b.compare_exchange_weak(cur, i as u32 + 1, Relaxed, Relaxed) {
                            Ok(_) => break,
                            Err(x) => cur = x,
                        }
                    }
                }
                (ids, roots)
            })
            .collect();
        lap("  links rooted");
        let nids: u32 = counts.iter().map(|x| x.0).sum();
        let mut offs = Vec::with_capacity(nch);
        let mut acc = 0u32;
        for x in &counts {
            offs.push(acc);
            acc += x.1;
        }
        let npersons = acc as usize;
        // number the persons (roots) in slot order
        let mut pname: Vec<&[u8]> = vec![&[]; npersons];
        {
            let pn = pname.as_mut_ptr() as usize;
            (0..nch).into_par_iter().for_each(|c| {
                let mut p = offs[c];
                for i in c * CH..((c + 1) * CH).min(cap) {
                    if occ(i) && parent[i].load(Relaxed) == 0 {
                        pnum[i].store(p, Relaxed);
                        // each person index is written by exactly one chunk
                        unsafe { *(pn as *mut &[u8]).add(p as usize) = str_of(best[i].load(Relaxed) - 1) };
                        p += 1;
                    }
                }
            });
        }
        lap("  links numbered");
        // re-point the table: id -> person (check kept in the high half)
        (0..nch).into_par_iter().for_each(|c| {
            for i in c * CH..((c + 1) * CH).min(cap) {
                if occ(i) {
                    let r = match parent[i].load(Relaxed) {
                        0 => i,
                        p => (p - 1) as usize,
                    };
                    let v = sl[2 * i + 1].load(Relaxed);
                    sl[2 * i + 1].store((v & !0xffff_ffff) | pnum[r].load(Relaxed) as u64, Relaxed);
                }
            }
        });
        (nids as usize, pname)
    };
    lap("  links repointed");
    let pkeys: Vec<(u64, u32)> = pname.par_iter().map(|n| hs(n, seed)).collect();
    let coll = AtomicBool::new(false);
    // person key of a user id: linked ids share their person's key; others are their own person
    let resolve_person = |k, c| {
        match ltab.get(k, c, &coll) {
            Some(p) => pkeys[p as usize],
            None => (k, c),
        }
    };
    // name of the person (key `pk`, check `pc`) an event line belongs to. If the line's own
    // user id hashes to the person's key, that id is the name (it is unlinked, or it is the
    // smallest id of its person), so the link table is only probed for other linked ids.
    let name_of = |person: PersonData, pk: u64, pc: u32| -> &[u8] {
        let u = fields::<7>(person.line(&evb)).unwrap()[1];
        let (k, c) = hs(u, seed);
        if k == pk && c == pc {
            return u;
        }
        match ltab.get(k, c, &coll) {
            Some(p) => pname[p as usize],
            None => u,
        }
    };
    lap(&format!("links: {} edges, {} ids, {} persons", edges.len(), nids, pname.len()));

    // Events phase A: route valid copies directly to people, and record
    // every eligible event ID for the global earliest-ingest decision.
    let ech: Vec<_> = evb.iter().enumerate().flat_map(|(file, &bytes)| {
        let line_bias = ((file as u64) << 32).wrapping_sub(bytes.as_ptr() as u64);
        chunks(&[bytes], 1 << 20).into_iter().map(move |chunk| (line_bias, chunk))
    }).collect();
    let shard_capacity = ech.iter().find(|(_, chunk)| chunk.len() >= 1 << 16).map_or(0, |(_, sample)| {
        let rows = memchr::memchr_iter(b'\n', sample).count();
        let avg_bytes = (sample.len() / rows.max(1)).max(64);
        let input_bytes: usize = ech.iter().map(|(_, chunk)| chunk.len()).sum();
        let per_shard = (input_bytes / avg_bytes).div_ceil(SH * rayon::current_num_threads());
        (per_shard + per_shard / 8).min(32_768)
    });
    let window_seconds = w0.div_euclid(1000);
    let folds: Vec<EventFold> = pt(ech.par_iter(),
            || EventFold::new(shard_capacity),
            |mut acc, &(line_bias, c)| {
                let mut dates = Dates::default();
                let mut batch = Vec::<ParsedEvent>::with_capacity(16);
                let mut short_writer = acc.short_copies.writer();
                let mut flush = |batch: &mut Vec<ParsedEvent>| {
                    for mut r in batch.drain(..) {
                        if r.ts != TOMBSTONE {
                            let (pkey, pchk) = resolve_person(r.pkey, r.pchk);
                            let partition = (pkey >> (64 - PB)) as usize;
                            let events = &mut acc.people[partition];
                            let Ok(index) = u32::try_from(events.len()) else {
                                coll.store(true, Relaxed);
                                continue;
                            };
                            events.push(EvP {
                                person: PersonData::new(pkey, pchk, r.line_ref),
                                ts: AtomicU32::new(r.ts),
                                price: r.price,
                            });
                            r.copy.loc = ((partition as u64) << 32) | u64::from(index);
                        }
                        let shard = (r.copy.hid >> (64 - SHB)) as usize;
                        if r.short_id {
                            if let Some(copy) = ShortD::pack(r.copy) {
                                short_writer.push(shard, copy);
                            } else {
                                coll.store(true, Relaxed);
                            }
                        } else {
                            acc.copies[shard].push(r.copy);
                        }
                    }
                };
                event_rows(c, |line, [id, user, ts, ing, et, pr, pc]| {
                    if id.is_empty() {
                        return;
                    }
                    let short_key = short_event_key(id);
                    let (hid, hchk) = short_key.map_or_else(|| hs(id, seed), |key| (key, 0));
                    let ingest = dates.parse(ing);
                    let (order, loc) = match ingest {
                        None => (i32::MAX, NO_EVENT | UNPARSED_INGEST),
                        Some(value) => {
                            let Some(order) = compact_ingest(value, window_seconds) else {
                                coll.store(true, Relaxed);
                                return;
                            };
                            (order, NO_EVENT)
                        }
                    };
                    let mut r = ParsedEvent {
                        copy: D { hid, hchk, ingest: order, loc },
                        line_ref: (line.as_ptr() as u64).wrapping_add(line_bias),
                        pkey: 0, pchk: 0, ts: TOMBSTONE, price: 0,
                        short_id: short_key.is_some(),
                    };
                    'v: {
                        let Some(ingest) = ingest else { break 'v };
                        if user.is_empty() || pr.is_empty() {
                            break 'v;
                        }
                        let purchase = match et {
                            b"view" | b"cart" => false,
                            b"purchase" => true,
                            _ => break 'v,
                        };
                        let t = match dates.parse(ts) {
                            Some(t) if t >= w0 && t < w1 => t,
                            _ => break 'v,
                        };
                        if ingest - t > DAY {
                            break 'v;
                        }
                        if purchase {
                            let Some(p) = parse_price(pc) else { break 'v };
                            let cat = *prod.get(pr).unwrap_or(&unknown);
                            r.price = p | (cat << 24);
                        }
                        (r.pkey, r.pchk) = hs(user, seed);
                        ltab.prefetch(r.pkey);
                        r.ts = (t - w0) as u32;
                    }
                    batch.push(r);
                    if batch.len() == 16 {
                        flush(&mut batch);
                    }
                });
                flush(&mut batch);
                acc
            },
        );
    lap(&format!("events parsed ({} folds)", folds.len()));
    if coll.load(Relaxed) {
        return false;
    }

    // Events phase B: keep the earliest copy and tombstone valid losers.
    // Every EvP location belongs to exactly one event-ID shard.
    pt((0..SH).into_par_iter(), Dedup::default, |mut table, shard| {
        table.shard(&folds, shard, &coll, |f, s| f.short_copies.records(s));
        table.shard(&folds, shard, &coll, |f, s| [&f.copies[s], &[]]);
        table
    });
    if coll.load(Relaxed) {
        return false;
    }
    let paccs: Vec<_> = folds.into_par_iter().map(|fold| fold.people).collect();
    lap("events dedup + validate");

    // clicks: parse + validate, route by person. Copies of a click are identical, so a duplicate
    // can change neither the chosen click nor the set of assisting campaigns: no dedup needed.
    let cch = chunks(&ckb, 1 << 20);
    let caccs: Vec<Vec<Vec<CkB>>> = pt(cch.par_iter(),
            || (0..P).map(|_| Vec::new()).collect::<Vec<Vec<CkB>>>(),
            |mut acc, c| {
                let mut dates = Dates::default();
                let mut batch = Vec::<CkB>::with_capacity(16);
                let mut flush = |batch: &mut Vec<CkB>| {
                    for mut click in batch.drain(..) {
                        (click.pkey, click.pchk) = resolve_person(click.pkey, click.pchk);
                        acc[(click.pkey >> (64 - PB)) as usize].push(click);
                    }
                };
                csv_rows(c, |l, [cid, user, cmp, ts]| {
                    if cid.is_empty() || user.is_empty() {
                        return;
                    }
                    let Some(&ci) = camp.get(cmp) else { return };
                    let t = match dates.parse(ts) {
                        Some(t) if t >= w0 - LOOK && t < w1 => t,
                        _ => return,
                    };
                    let (pkey, pchk) = hs(user, seed);
                    ltab.prefetch(pkey);
                    batch.push(CkB { pkey, ts: t, line: l.as_ptr() as u64, pchk, camp: ci });
                    if batch.len() == 16 {
                        flush(&mut batch);
                    }
                });
                flush(&mut batch);
                acc
            },
        );
    lap("clicks parsed");

    // per partition: sort, sessionize, credit, aggregate
    let ncs = ncamp + 1; // index ncamp = organic
    struct St {
        cd: Vec<[i64; 4]>,
        cnt: Vec<u32>,
        inact: Vec<bool>,
        active: Vec<u32>,
        catsum: Vec<i64>,
        touched: Vec<u32>,
        rows: Vec<UdRow>,
        ev: Vec<LiveEvent>,
        ck: Vec<CkB>,
        rs: RadixScratch<LiveEvent>,
        rc: RadixScratch<CkB>,
    }
    let ncat = cat_names.len();
    let cid_of = |line: u64| -> &[u8] { fields::<4>(ck_lines.line(line)).unwrap()[0] };
    let states: Vec<St> = pt((0..P).into_par_iter(),
            || St {
                cd: vec![[0; 4]; ncs * ndays],
                cnt: vec![0; ncamp],
                inact: vec![false; ncamp],
                active: vec![],
                catsum: vec![0; ncat],
                touched: vec![],
                rows: vec![],
                ev: vec![],
                ck: vec![],
                rs: RadixScratch::default(),
                rc: RadixScratch::default(),
            },
            |mut st, p| {
                st.ev.clear();
                for a in &paccs {
                    st.ev.extend(a[p].iter().filter_map(EvP::live));
                }
                st.ck.clear();
                for a in &caccs {
                    st.ck.extend_from_slice(&a[p]);
                }
                radix_sort(&mut st.ev, &mut st.rs, |e| e.person.key_low(), |e| e.ts);

                if w1 - (w0 - LOOK) <= u32::MAX as i64 {
                    radix_sort(&mut st.ck, &mut st.rc, |c| c.pkey, |c| (c.ts - (w0 - LOOK)) as u32);
                } else {
                    st.ck.sort_unstable_by_key(|c| (c.pkey, c.ts));
                }
                let St { cd, cnt, inact, active, catsum, touched, rows, ev, ck, .. } = &mut st;
                let (ev, ck) = (&ev[..], &ck[..]);
                let mut ci = 0;
                let mut i = 0;
                while i < ev.len() {
                    let local_key = ev[i].person.key_low();
                    let pk = ((p as u64) << (64 - PB)) | local_key;
                    let pc = ev[i].person.check();
                    // Resolve once per person rather than once per output day while writing.
                    // Pointer references are into immutable mmaps, which outlive parquet.
                    let mut j = i;
                    while j < ev.len() && ev[j].person.key_low() == local_key {
                        if ev[j].person.check() != pc {
                            coll.store(true, Relaxed);
                        }
                        j += 1;
                    }
                    if j < ev.len() {
                        ev[j].person.prefetch_line(&evb); // the next person's name lookup
                    }
                    let Some(name) = pack_ref(name_of(ev[i].person, pk, pc)) else {
                        coll.store(true, Relaxed);
                        return st;
                    };
                    while ci < ck.len() && ck[ci].pkey < pk {
                        ci += 1;
                    }
                    let c0 = ci;
                    while ci < ck.len() && ck[ci].pkey == pk {
                        if ck[ci].pchk != pc {
                            coll.store(true, Relaxed);
                        }
                        ci += 1;
                    }
                    let c1 = ci;
                    // this person's sessions
                    let (mut lo, mut hi) = (c0, c0);
                    let mut row: Option<UdRow> = None;
                    let mut k = i;
                    while k < j {
                        let start = ev[k].ts;
                        let mut m = k + 1;
                        while m < j && ev[m].ts - ev[m - 1].ts <= GAP as u32 {
                            m += 1;
                        }
                        let nev = (m - k) as i64;
                        let sabs = w0 + start as i64;
                        let day = sabs.div_euclid(DAY);
                        let di = (day - day0) as usize;
                        // user_daily: sessions in start order, so days only move forward.
                        // Flush the previous day before this session's purchases are summed.
                        if row.as_ref().map(|r| r.day as i64 != day).unwrap_or(false) {
                            let mut r = row.take().unwrap();
                            r.top = top_cat(catsum, touched, &cat_names);
                            rows.push(r);
                        }
                        let mut rev = 0i64;
                        for e in &ev[k..m] {
                            if e.price != 0 {
                                rev += (e.price & 0xff_ffff) as i64;
                                let c = e.price >> 24;
                                if catsum[c as usize] == 0 {
                                    touched.push(c);
                                }
                                catsum[c as usize] += (e.price & 0xff_ffff) as i64;
                            }
                        }
                        let purch = rev > 0;
                        let r = row.get_or_insert(UdRow { name, day: day as i32, ns: 0, ne: 0, nr: 0, top: u32::MAX });
                        r.ns += 1;
                        r.ne += nev;
                        r.nr += rev;
                        // sliding 7-day click window [sabs - LOOK, sabs]
                        while hi < c1 && ck[hi].ts <= sabs {
                            let c = ck[hi].camp as usize;
                            cnt[c] += 1;
                            if !inact[c] {
                                inact[c] = true;
                                active.push(c as u32);
                            }
                            hi += 1;
                        }
                        while lo < hi && ck[lo].ts < sabs - LOOK {
                            cnt[ck[lo].camp as usize] -= 1;
                            lo += 1;
                        }
                        let mut credit = ncamp;
                        if hi > lo {
                            let t = ck[hi - 1].ts;
                            let mut best = hi - 1;
                            let mut x = hi - 1;
                            while x > lo && ck[x - 1].ts == t {
                                x -= 1;
                                if ck[x].line != ck[best].line && cid_of(ck[x].line) < cid_of(ck[best].line) {
                                    best = x;
                                }
                            }
                            credit = ck[best].camp as usize;
                        }
                        let e = &mut cd[credit * ndays + di];
                        e[0] += 1;
                        e[1] += purch as i64;
                        e[2] += rev;
                        active.retain(|&a| {
                            if cnt[a as usize] > 0 {
                                true
                            } else {
                                inact[a as usize] = false;
                                false
                            }
                        });
                        for &a in active.iter() {
                            cd[a as usize * ndays + di][3] += 1;
                        }
                        k = m;
                    }
                    if let Some(mut r) = row.take() {
                        r.top = top_cat(catsum, touched, &cat_names);
                        rows.push(r);
                    }
                    for &a in active.iter() {
                        cnt[a as usize] = 0;
                        inact[a as usize] = false;
                    }
                    for x in &ck[lo..hi] {
                        cnt[x.camp as usize] = 0;
                    }
                    active.clear();
                    i = j;
                }
                st
            },
        );
    std::mem::forget(paccs);
    std::mem::forget(caccs);
    if coll.load(Relaxed) {
        return false;
    }
    lap("sessions + credit + aggregate");

    // merge campaign_daily
    let mut cd = vec![[0i64; 4]; ncs * ndays];
    for s in &states {
        for (a, b) in cd.iter_mut().zip(&s.cd) {
            for k in 0..4 {
                a[k] += b[k];
            }
        }
    }
    let nrows: usize = states.iter().map(|s| s.rows.len()).sum();
    lap(&format!("merged: user_daily {}", nrows));

    // write parquet: user_daily in parallel parts, campaign_daily in one file
    use arrow_array::*;
    use arrow_array::builder::StringBuilder;
    use arrow_schema::{DataType, Field, Schema};
    std::fs::create_dir_all(format!("{out}/user_daily")).unwrap();
    std::fs::create_dir_all(format!("{out}/campaign_daily")).unwrap();
    let props = parquet::file::properties::WriterProperties::builder()
        .set_compression(parquet::basic::Compression::UNCOMPRESSED)
        .set_dictionary_enabled(false)
        .set_statistics_enabled(parquet::file::properties::EnabledStatistics::None)
        .build();
    let write = |path: String, batch: RecordBatch| {
        let f = std::fs::File::create(path).unwrap();
        let mut w = parquet::arrow::ArrowWriter::try_new(f, batch.schema(), Some(props.clone())).unwrap();
        w.write(&batch).unwrap();
        w.close().unwrap();
    };
    // "any integer width": counts go out as Int32 when every value fits (sessions <= events)
    let narrow = states.par_iter().all(|s| s.rows.iter().all(|r| r.ne <= i32::MAX as i64));
    let cnt_t = if narrow { DataType::Int32 } else { DataType::Int64 };
    let s1 = Arc::new(Schema::new(vec![
        Field::new("user_id", DataType::Utf8, false),
        Field::new("day", DataType::Date32, false),
        Field::new("sessions", cnt_t.clone(), false),
        Field::new("events", cnt_t.clone(), false),
        Field::new("revenue_cents", DataType::Int64, false),
        Field::new("top_category", DataType::Utf8, true),
    ]));
    let s2 = Arc::new(Schema::new(vec![
        Field::new("campaign_id", DataType::Utf8, false),
        Field::new("channel", DataType::Utf8, false),
        Field::new("day", DataType::Date32, false),
        Field::new("sessions", DataType::Int64, false),
        Field::new("purchasing_sessions", DataType::Int64, false),
        Field::new("revenue_cents", DataType::Int64, false),
        Field::new("assisted_sessions", DataType::Int64, false),
    ]));
    fn st(b: &[u8]) -> &str { unsafe { std::str::from_utf8_unchecked(b) } }
    let nparts = ncpu.max(1);
    // parts straight from each worker's rows (no merged copy), about nparts in all
    let per = nrows.div_ceil(nparts).max(1);
    let parts: Vec<&[UdRow]> = states.iter().flat_map(|s| s.rows.chunks(per)).collect();
    let counts = |part: &[UdRow], f: fn(&UdRow) -> i64| -> ArrayRef {
        if narrow {
            Arc::new(Int32Array::from(part.iter().map(|r| f(r) as i32).collect::<Vec<_>>()))
        } else {
            Arc::new(Int64Array::from(part.iter().map(|r| f(r)).collect::<Vec<_>>()))
        }
    };
    let user_job = || {
        parts.par_iter().enumerate().for_each(|(pi, part)| {
            let mut uid = StringBuilder::with_capacity(part.len(), part.len() * 12);
            let mut top = StringBuilder::with_capacity(part.len(), part.len() * 8);
            for r in part.iter() {
                uid.append_value(st(unpack_ref(r.name)));
                if r.top == u32::MAX {
                    top.append_null();
                } else {
                    top.append_value(st(cat_names[r.top as usize]));
                }
            }
            let b = RecordBatch::try_new(
                s1.clone(),
                vec![
                    Arc::new(uid.finish()),
                    Arc::new(Date32Array::from(part.iter().map(|r| r.day).collect::<Vec<_>>())),
                    counts(part, |r| r.ns),
                    counts(part, |r| r.ne),
                    Arc::new(Int64Array::from(part.iter().map(|r| r.nr).collect::<Vec<_>>())),
                    Arc::new(top.finish()),
                ],
            )
            .unwrap();
            write(format!("{out}/user_daily/part-{pi}.parquet"), b);
        });
        if parts.is_empty() {
            write(format!("{out}/user_daily/part-0.parquet"), RecordBatch::new_empty(s1.clone()));
        }
    };
    let camp_job = || {
        let (mut cid, mut cch2, mut cday) = (StringBuilder::new(), StringBuilder::new(), vec![]);
        let mut cv: [Vec<i64>; 4] = Default::default();
        for c in 0..ncs {
            for d in 0..ndays {
                let v = cd[c * ndays + d];
                if v[0] == 0 && v[3] == 0 {
                    continue;
                }
                let (n, ch): (&[u8], &[u8]) = if c == ncamp { (b"organic", b"organic") } else { (camp_names[c], camp_chan[c]) };
                cid.append_value(st(n));
                cch2.append_value(st(ch));
                cday.push((day0 + d as i64) as i32);
                for k in 0..4 {
                    cv[k].push(v[k]);
                }
            }
        }
        let [a, b, c, d] = cv;
        let batch = RecordBatch::try_new(
            s2.clone(),
            vec![
                Arc::new(cid.finish()),
                Arc::new(cch2.finish()),
                Arc::new(Date32Array::from(cday)),
                Arc::new(Int64Array::from(a)),
                Arc::new(Int64Array::from(b)),
                Arc::new(Int64Array::from(c)),
                Arc::new(Int64Array::from(d)),
            ],
        )
        .unwrap();
        write(format!("{out}/campaign_daily/part-0.parquet"), batch);
    };
    rayon::join(user_job, camp_job);
    if coll.load(Relaxed) {
        // a name lookup hit a collision while writing: discard and redo exactly
        let _ = std::fs::remove_dir_all(format!("{out}/user_daily"));
        let _ = std::fs::remove_dir_all(format!("{out}/campaign_daily"));
        return false;
    }
    lap("parquet written");
    true
}

/// Like rayon's fold, but exactly one accumulator per worker thread (fold makes one per split,
/// hundreds here), with dynamic load balancing over the items.
fn pt<I, T: Send>(items: impl ParallelIterator<Item = I>, init: impl Fn() -> T + Sync + Send, f: impl Fn(T, I) -> T + Sync + Send) -> Vec<T> {
    let n = rayon::current_num_threads();
    let slots: Vec<std::sync::Mutex<Option<T>>> = (0..n).map(|_| std::sync::Mutex::new(None)).collect();
    items.for_each(|it| {
        let t = rayon::current_thread_index().unwrap_or(0) % n;
        let mut g = slots[t].lock().unwrap();
        let acc = g.take().unwrap_or_else(&init);
        *g = Some(f(acc, it));
    });
    slots.into_iter().filter_map(|m| m.into_inner().unwrap()).collect()
}

#[inline]
fn ltab_prefetch(sl: &[std::sync::atomic::AtomicU64], mask: usize, key: u64) {
    let index = (if key == 0 { 1 } else { key }) as usize & mask;
    let ptr = sl.as_ptr().wrapping_add(2 * index);
    #[cfg(target_arch = "aarch64")]
    unsafe {
        std::arch::asm!("prfm pstl1keep, [{ptr}]", ptr = in(reg) ptr, options(nomem, nostack, preserves_flags));
    }
    #[cfg(target_arch = "x86_64")]
    unsafe {
        std::arch::x86_64::_mm_prefetch::<{ std::arch::x86_64::_MM_HINT_T0 }>(ptr.cast());
    }
    #[cfg(not(any(target_arch = "aarch64", target_arch = "x86_64")))]
    let _ = ptr;
}

/// Reused per-worker buffers for `radix_sort`.
struct RadixScratch<T> {
    slots: Vec<(u64, u32, u32)>, // (key_low, stamp, local id): stamped, so never cleared
    stamp: u32,
    keys: Vec<u64>,    // distinct person keys, by local id
    rank: Vec<u32>,    // local id -> rank of its key
    order: Vec<u32>,   // local ids sorted by key
    lid: Vec<u32>,     // local id of each event
    a: Vec<u64>,
    b: Vec<u64>,
    out: Vec<T>,
}

/// Sorts a partition's records by (person key, ts), as `sort_unstable_by_key` would (equal
/// pairs in any order). Persons are ranked by key, then (rank, ts, index) packed in a u64 is
/// LSD-radix sorted on 11-bit digits and the events gathered once. Falls back to a comparison
/// sort when the packed key would not fit in 64 bits.
fn radix_sort<T: Copy>(ev: &mut Vec<T>, rs: &mut RadixScratch<T>, key: impl Fn(&T) -> u64, tsf: impl Fn(&T) -> u32) {
    let n = ev.len();
    if n < 64 {
        ev.sort_unstable_by_key(|e| (key(e), tsf(e)));
        return;
    }
    // distinct persons -> local ids
    let cap = (n.next_power_of_two() * 2).max(64);
    if rs.slots.len() < cap {
        rs.slots = vec![(0, 0, 0); cap];
        rs.stamp = 0;
    }
    rs.stamp = rs.stamp.wrapping_add(1);
    if rs.stamp == 0 {
        rs.slots.iter_mut().for_each(|s| s.1 = 0);
        rs.stamp = 1;
    }
    let mask = rs.slots.len() - 1;
    let stamp = rs.stamp;
    rs.keys.clear();
    rs.lid.clear();
    rs.lid.reserve(n);
    let mut tmax = 0u32;
    for e in ev.iter() {
        let k = key(e);
        tmax = tmax.max(tsf(e));
        let mut i = (k.wrapping_mul(0x9E37_79B9_7F4A_7C15) >> 32) as usize & mask;
        let id = loop {
            let s = &mut rs.slots[i];
            if s.1 != stamp {
                *s = (k, stamp, rs.keys.len() as u32);
                rs.keys.push(k);
                break s.2;
            }
            if s.0 == k {
                break s.2;
            }
            i = (i + 1) & mask;
        };
        rs.lid.push(id);
    }
    let np = rs.keys.len();
    rs.order.clear();
    rs.order.extend(0..np as u32);
    let keys = &rs.keys;
    rs.order.sort_unstable_by_key(|&i| keys[i as usize]);
    rs.rank.clear();
    rs.rank.resize(np, 0);
    for (r, &i) in rs.order.iter().enumerate() {
        rs.rank[i as usize] = r as u32;
    }
    let bits = |x: u64| 64 - x.leading_zeros();
    let (ib, tb, rb) = (bits(n as u64 - 1), bits(tmax as u64), bits(np as u64 - 1));
    if ib + tb + rb > 64 {
        ev.sort_unstable_by_key(|e| (key(e), tsf(e)));
        return;
    }
    rs.a.clear();
    rs.a.extend(ev.iter().zip(&rs.lid).enumerate().map(|(i, (e, &l))| {
        ((((rs.rank[l as usize] as u64) << tb) | tsf(e) as u64) << ib) | i as u64
    }));
    rs.b.clear();
    rs.b.resize(n, 0);
    const DB: u32 = 11;
    let mut cnt = [0u32; 1 << DB];
    let mut shift = ib;
    let top = ib + tb + rb;
    while shift < top {
        cnt.iter_mut().for_each(|c| *c = 0);
        for &x in rs.a.iter() {
            cnt[((x >> shift) as usize) & ((1 << DB) - 1)] += 1;
        }
        let mut sum = 0;
        for c in cnt.iter_mut() {
            let t = *c;
            *c = sum;
            sum += t;
        }
        for &x in rs.a.iter() {
            let d = ((x >> shift) as usize) & ((1 << DB) - 1);
            rs.b[cnt[d] as usize] = x;
            cnt[d] += 1;
        }
        std::mem::swap(&mut rs.a, &mut rs.b);
        shift += DB;
    }
    let im = (1u64 << ib) - 1;
    rs.out.clear();
    rs.out.extend(rs.a.iter().map(|&x| ev[(x & im) as usize]));
    std::mem::swap(ev, &mut rs.out);
}

impl<T> Default for RadixScratch<T> {
    fn default() -> Self {
        RadixScratch { slots: vec![], stamp: 0, keys: vec![], rank: vec![], order: vec![], lid: vec![], a: vec![], b: vec![], out: vec![] }
    }
}

/// Highest summed price; ties to the byte-wise smallest name. Resets the sums.
fn top_cat(catsum: &mut [i64], touched: &mut Vec<u32>, names: &[&[u8]]) -> u32 {
    let mut top = u32::MAX;
    let mut best = -1i64;
    for &c in touched.iter() {
        let v = catsum[c as usize];
        if v > best || (v == best && names[c as usize] < names[top as usize]) {
            best = v;
            top = c;
        }
        catsum[c as usize] = 0;
    }
    touched.clear();
    top
}
