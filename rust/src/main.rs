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
/// True if all 8 bytes are ASCII digits (a '9' after a byte >= 0xFA can read as false,
/// which only sends the caller to the scalar path).
#[inline]
fn swar_digits(w: u64) -> bool {
    const HI: u64 = 0xF0F0_F0F0_F0F0_F0F0;
    (w & HI) | ((w.wrapping_add(0x0606_0606_0606_0606) & HI) >> 4) == 0x3333_3333_3333_3333
}

/// Value of 8 ASCII digits, first digit in the lowest byte.
#[inline]
fn swar_value(w: u64) -> u64 {
    let v = w - 0x3030_3030_3030_3030;
    let v = v * 10 + (v >> 8);
    (((v & 0x0000_00FF_0000_00FF) * (100 + (1_000_000 << 32)))
        + (((v >> 16) & 0x0000_00FF_0000_00FF) * (1 + (10_000 << 32))))
        >> 32
}

#[cfg(test)]
#[test]
fn swar_f3_matches_scalar() {
    let mut x = 0x2545_f491_4f6c_dd1du64;
    for n in 0..200_000u64 {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        let mut s = format!("{:013}", x % 10_000_000_000_000).into_bytes();
        if n % 3 == 0 {
            let i = (x >> 50) as usize % 13;
            s[i] = [b'/', b':', b'a', b' ', 0xff, b'.'][(x >> 40) as usize % 6];
        }
        let mut d = Dates::default();
        let want = if s.iter().all(|c| c.is_ascii_digit()) {
            Some(std::str::from_utf8(&s).unwrap().parse::<i64>().unwrap())
        } else {
            None
        };
        assert_eq!(d.parse(&s), want, "{:?}", std::str::from_utf8(&s));
    }
}

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
        if s.len() == 13 {
            // F3 epoch milliseconds: 5 + 8 digits, word at a time.
            let hi = (rd8(s, 0) << 24) | 0x30_3030;
            let lo = rd8(s, 5);
            if swar_digits(hi) && swar_digits(lo) {
                return Some((swar_value(hi) * 100_000_000 + swar_value(lo)) as i64);
            }
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
        Tab { slots: vec![(0, 0); cap], mask: cap - 1 }
    }
    #[inline]
    #[allow(dead_code)]
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
    #[allow(dead_code)]
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
const SHB: u32 = 8;
const SH: usize = 1 << SHB;
const PB: u32 = 10;
const P: usize = 1 << PB;

const TOMBSTONE: u32 = u32::MAX;
const UNPARSED_INGEST: u64 = 1 << 63;
const NO_EVENT: u64 = UNPARSED_INGEST - 1;

/// One valid event copy in its final person partition. Only the timestamp
/// can change during dedup, and only by the shard owning this copy's event ID.
#[repr(C)]
struct EvP {
    // The dense person id from the all-uid table: partition = person % P, local = person / P.
    person: u32,
    ts: AtomicU32,
    price: u32,
}

impl EvP {
    // Keep the session reader independent of the in-memory ingest layout.
    // The 12-byte ingest record can supply the same ID and packed payload.
    #[inline]
    fn session_input(&self) -> Option<(u64, u64)> {
        let ts = self.ts.load(Relaxed);
        (ts != TOMBSTONE).then(|| {
            (u64::from(self.person), (u64::from(ts) << 32) | u64::from(self.price))
        })
    }

    #[cfg(test)]
    fn live(&self) -> Option<u32> {
        let ts = self.ts.load(Relaxed);
        (ts != TOMBSTONE).then_some(ts)
    }
}

const _: () = assert!(std::mem::size_of::<EvP>() == 12);

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
            fold.people.get(partition, index).ts.store(TOMBSTONE, Relaxed);
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

const PERSON_SET: u64 = 1 << 31;

/// The all-uid table over the link table's slots (see `fast`): key, then
/// (check << 32) | PERSON_SET | person. A claimed key whose value is still 0 is being
/// inserted by another thread, which publishes the value right after its key.
struct Users<'a> {
    sl: &'a [std::sync::atomic::AtomicU64],
    mask: usize,
    limit: usize,
    used: std::sync::atomic::AtomicUsize,
    next: std::sync::atomic::AtomicU32,
    names: &'a [std::sync::atomic::AtomicU64],
}

impl<'a> Users<'a> {
    fn new(tab: &'a mut Tab, nids: usize, npersons: usize) -> Self {
        use std::sync::atomic::{AtomicU32, AtomicU64, AtomicUsize};
        let cap = tab.mask + 1;
        // The slots are not otherwise accessed until the table is dropped; AtomicU64 has u64's layout.
        let sl = unsafe { std::slice::from_raw_parts(tab.slots.as_mut_ptr() as *const AtomicU64, cap * 2) };
        // Dense ids never exceed: linked persons, one block of padding, one id per slot,
        // and one partial block per worker.
        let first = npersons.div_ceil(P) * P;
        let ids = first + cap + (rayon::current_num_threads() + 1) * P;
        let v: Vec<u64> = vec![0; ids];
        let names = unsafe { std::slice::from_raw_parts(v.as_ptr() as *const AtomicU64, ids) };
        std::mem::forget(v);
        Users { sl, mask: tab.mask, limit: cap / 4 * 3, used: AtomicUsize::new(nids), next: AtomicU32::new(first as u32), names }
    }

    #[inline]
    fn fresh(&self, block: &mut (u32, u32), coll: &AtomicBool) -> u32 {
        if block.0 == block.1 {
            let b = self.next.fetch_add(P as u32, Relaxed);
            *block = (b, b.wrapping_add(P as u32));
            if self.used.fetch_add(P, Relaxed) > self.limit || b as u64 + P as u64 >= PERSON_SET {
                coll.store(true, Relaxed);
            }
        }
        block.0 += 1;
        block.0 - 1
    }

    #[inline]
    #[allow(dead_code)]
    fn prefetch(&self, key: u64) {
        let index = (if key == 0 { 1 } else { key }) as usize & self.mask;
        let ptr = self.sl.as_ptr().wrapping_add(2 * index);
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

    /// The person of a user id on a valid event copy, inserting an unlinked id.
    #[inline]
    fn person(&self, k: u64, c: u32, name: u64, block: &mut (u32, u32), coll: &AtomicBool) -> u32 {
        use std::sync::atomic::Ordering::{AcqRel, Acquire, Release};
        let k = if k == 0 { 1 } else { k };
        let mut i = (k as usize) & self.mask;
        loop {
            let mut kk = self.sl[2 * i].load(Acquire);
            if kk == 0 {
                match self.sl[2 * i].compare_exchange(0, k, AcqRel, Acquire) {
                    Ok(_) => {
                        let id = self.fresh(block, coll);
                        if let Some(slot) = self.names.get(id as usize) {
                            slot.store(name, Relaxed);
                        } else {
                            coll.store(true, Relaxed);
                        }
                        let v = (u64::from(c) << 32) | PERSON_SET | u64::from(id & (PERSON_SET as u32 - 1));
                        self.sl[2 * i + 1].store(v, Release);
                        return id & (PERSON_SET as u32 - 1);
                    }
                    Err(x) => kk = x,
                }
            }
            if kk == k {
                let mut v = self.sl[2 * i + 1].load(Acquire);
                while v == 0 {
                    std::hint::spin_loop();
                    v = self.sl[2 * i + 1].load(Acquire);
                }
                if (v >> 32) as u32 != c {
                    coll.store(true, Relaxed);
                }
                return (v as u32) & (PERSON_SET as u32 - 1);
            }
            i = (i + 1) & self.mask;
        }
    }

    /// The person of a user id, if it is linked or on a valid event (lookup only; every
    /// insert has finished).
    #[inline]
    fn find(&self, k: u64, c: u32, coll: &AtomicBool) -> Option<u32> {
        let k = if k == 0 { 1 } else { k };
        let mut i = (k as usize) & self.mask;
        loop {
            let kk = self.sl[2 * i].load(Relaxed);
            if kk == 0 {
                return None;
            }
            if kk == k {
                let v = self.sl[2 * i + 1].load(Relaxed);
                if (v >> 32) as u32 != c {
                    coll.store(true, Relaxed);
                    return None;
                }
                return Some((v as u32) & (PERSON_SET as u32 - 1));
            }
            i = (i + 1) & self.mask;
        }
    }

    /// Names of all dense ids (padded to whole blocks; unused ids are empty and have no
    /// events) and the per-partition local id bound.
    fn names(&self, linked: &[&'static [u8]], coll: &AtomicBool) -> (u32, Vec<&'static [u8]>) {
        let n = (self.next.load(Relaxed) as usize).min(self.names.len());
        if n >= PERSON_SET as usize {
            coll.store(true, Relaxed);
        }
        let all: Vec<&'static [u8]> = (0..n).into_par_iter().map(|id| {
            if id < linked.len() {
                linked[id]
            } else {
                match self.names[id].load(Relaxed) {
                    0 => &[][..],
                    r => unpack_ref(r),
                }
            }
        }).collect();
        ((n.div_ceil(P)) as u32, all)
    }
}

/// Valid event copies of one worker, by person partition. Each partition owns an initialized
/// prefix of one anonymous mapping (huge pages on Linux, so the 1,024 write streams share a few
/// TLB entries and fault in 2 MB steps) plus a growable overflow.
struct Parts<T> {
    storage: Option<memmap2::MmapMut>,
    stride: usize,
    lengths: Vec<usize>,
    overflow: Vec<Vec<T>>,
}

// The mapping is only written through &mut self; shared access reads initialized prefixes.
unsafe impl<T: Send> Send for Parts<T> {}
unsafe impl<T: Sync> Sync for Parts<T> {}

impl<T> Parts<T> {
    fn new(reservation: usize) -> Self {
        let storage = if reservation == 0 || std::env::var_os("BLUE2_NO_PEOPLE_MAP").is_some() {
            None
        } else {
            memmap2::MmapMut::map_anon(reservation * P * std::mem::size_of::<T>()).ok()
        };
        #[cfg(target_os = "linux")]
        if let Some(mapping) = &storage {
            let _ = mapping.advise(memmap2::Advice::HugePage);
        }
        let stride = if storage.is_some() { reservation } else { 0 };
        let overflow = (0..P).map(|_| if stride == 0 { Vec::with_capacity(reservation) } else { Vec::new() }).collect();
        Self { storage, stride, lengths: vec![0; P], overflow }
    }

    #[inline]
    fn len(&self, part: usize) -> usize {
        self.lengths[part] + self.overflow[part].len()
    }

    #[inline]
    fn push(&mut self, part: usize, row: T) {
        let len = self.lengths[part];
        if len < self.stride {
            let base = self.storage.as_mut().unwrap().as_mut_ptr().cast::<T>();
            // This partition's next prefix slot is in the mapping and not yet initialized.
            unsafe { base.add(part * self.stride + len).write(row) };
            self.lengths[part] = len + 1;
        } else {
            self.overflow[part].push(row);
        }
    }

    #[inline]
    fn records(&self, part: usize) -> [&[T]; 2] {
        let prefix = self.storage.as_ref().map_or(&[][..], |mapping| {
            // Only the recorded prefix is initialized; no writes occur through &self.
            unsafe { std::slice::from_raw_parts(mapping.as_ptr().cast::<T>().add(part * self.stride), self.lengths[part]) }
        });
        [prefix, &self.overflow[part][..]]
    }

    #[inline]
    fn get(&self, part: usize, index: usize) -> &T {
        let [prefix, overflow] = self.records(part);
        prefix.get(index).unwrap_or_else(|| &overflow[index - prefix.len()])
    }
}

type People = Parts<EvP>;

struct EventFold {
    copies: Vec<Vec<D>>,
    short_copies: ShortArena,
    people: People,
}

impl EventFold {
    fn new(shard_capacity: usize) -> Self {
        let person_capacity = shard_capacity * SH / P;
        Self {
            copies: (0..SH).map(|_| Vec::new()).collect(),
            short_copies: ShortArena::new(shard_capacity),
            people: People::new(person_capacity),
        }
    }
}

/// Temporary event shape, used only for the prefetch batch.
struct ParsedEvent {
    copy: D,
    name_ref: u64,
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
            let index = fold.people.len(p);
            fold.people.push(p, EvP {
                person: 0,
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
        assert!(folds[0].people.get(3, 0).live().is_none());
        assert!(folds[0].people.get(5, 0).live().is_none());
    }

    #[test]
    fn later_duplicate_with_different_person_is_tombstoned() {
        let mut folds = [EventFold::new(0), EventFold::new(0)];
        copy(&mut folds[0], 2, Some(1000));
        copy(&mut folds[1], 1, Some(1));
        dedup(&folds);
        assert!(folds[0].people.get(1000, 0).live().is_none());
        assert!(folds[1].people.get(1, 0).live().is_some());
    }

    #[test]
    fn equal_ingest_valid_duplicates_leave_exactly_one_live_copy() {
        let mut folds = [EventFold::new(0), EventFold::new(0)];
        copy(&mut folds[0], 1, Some(3));
        copy(&mut folds[1], 1, Some(3));
        copy(&mut folds[1], 1, Some(3));
        dedup(&folds);
        assert_eq!(folds.iter().flat_map(|f| f.people.records(3).into_iter().flatten()).filter_map(EvP::live).count(), 1);
    }

    #[test]
    fn unparseable_ingest_loses_to_largest_parsed_ingest() {
        let mut folds = [EventFold::new(0)];
        folds[0].copies[0].push(D { hid: 7, hchk: 9, ingest: i32::MAX, loc: NO_EVENT | UNPARSED_INGEST });
        copy(&mut folds[0], i32::MAX, Some(3));
        dedup(&folds);
        assert!(folds[0].people.get(3, 0).live().is_some());
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
        assert!(folds[0].people.get(P - 1, 0).live().is_none());
        assert!(folds[1].people.get(1, 0).live().is_some());
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

/// A validated click, with exact click ID available for tie-breaking.
#[derive(Clone, Copy)]
struct Click {
    person: u64,
    id: u64,
    ts: u32,
    campaign: u32,
}

struct PendingClick {
    click: Click,
    key: u64,
    check: u32,
}

#[derive(Clone, Copy)]
struct ClickMeta {
    id: u64,
    campaign: u32,
}

// Within a dense local person, the high word sorts by time and then
// clicks before events. The low word is price/category or a click index.
#[inline]
fn timeline_payload(ts: u32, event: bool, payload: u32) -> u64 {
    let time_key = (ts << 1) | u32::from(event);
    (u64::from(time_key) << 32) | u64::from(payload)
}

#[derive(Default)]
struct UserColumns {
    name: arrow_array::builder::StringBuilder,
    day: Vec<i32>,
    sessions: Vec<i64>,
    events: Vec<i64>,
    revenue: Vec<i64>,
    category: arrow_array::builder::StringBuilder,
}

#[derive(Default)]
struct UserDay {
    day: i64,
    sessions: i64,
    events: i64,
    revenue: i64,
}

impl UserColumns {
    fn append(&mut self, name: &[u8], row: &UserDay, category: Option<&[u8]>) {
        self.name.append_value(csv_str(name));
        self.day.push(row.day as i32);
        self.sessions.push(row.sessions);
        self.events.push(row.events);
        self.revenue.push(row.revenue);
        if let Some(category) = category {
            self.category.append_value(csv_str(category));
        } else {
            self.category.append_null();
        }
    }

    fn finish(mut self, schema: std::sync::Arc<arrow_schema::Schema>) -> arrow_array::RecordBatch {
        use arrow_array::{Date32Array, Int64Array, RecordBatch};
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(self.name.finish()),
                Arc::new(Date32Array::from(self.day)),
                Arc::new(Int64Array::from(self.sessions)),
                Arc::new(Int64Array::from(self.events)),
                Arc::new(Int64Array::from(self.revenue)),
                Arc::new(self.category.finish()),
            ],
        ).unwrap()
    }
}

#[inline]
fn csv_str(bytes: &[u8]) -> &str {
    // CSV fields are UTF-8 by contract, and slicing only strips ASCII syntax.
    unsafe { std::str::from_utf8_unchecked(bytes) }
}

#[inline]
fn dense_person(
    token: u64,
    linked_count: u32,
    unlinked: &mut HashMap<&'static [u8], u32>,
    names: &mut Vec<&'static [u8]>,
) -> Option<u32> {
    if token >> 48 == 0 {
        return Some((token as u32) >> PB);
    }
    use std::collections::hash_map::Entry;
    let name = unpack_ref(token);
    match unlinked.entry(name) {
        Entry::Occupied(entry) => Some(*entry.get()),
        Entry::Vacant(entry) => {
            let id = linked_count.checked_add(u32::try_from(names.len()).ok()?)?;
            names.push(name);
            entry.insert(id);
            Some(id)
        }
    }
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
    let click_start = w0 - LOOK;
    if w1 <= w0 || w1 - click_start > (1i64 << 31) {
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
            csv_rows(c, |_, [a, b]| {
                if !a.is_empty() && !b.is_empty() {
                    let (ka, ca) = hs(a, seed);
                    let (kb, cb) = hs(b, seed);
                    v.push([End { s: a, k: ka, c: ca }, End { s: b, k: kb, c: cb }]);
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
    // The table also takes every unlinked user id seen on a valid event (inserted during the
    // event parse), so size it for both; a fuller table than 3/4 declines to slow.rs.
    let est_users = evb.iter().find(|b| b.len() >= 1 << 16).map_or(0, |b| {
        let rows = memchr::memchr_iter(b'\n', &b[..1 << 16]).count().max(1);
        let total: usize = evb.iter().map(|b| b.len()).sum();
        total / ((1 << 16) / rows).max(1) / 12
    });
    let mut ltab = Tab::new((edges.len() * 2 + 16).max(est_users));
    let cap = ltab.slots.len();
    let mask = ltab.mask;
    let lcoll = AtomicBool::new(false);
    let (nids, pname) = {
        use std::sync::atomic::{AtomicU32, AtomicU64, Ordering::{Acquire, Release}};
        // the slots as atomics: (key, (chk << 32) | 1) while building; AtomicU64 has u64's layout
        let sl: &[AtomicU64] = unsafe { std::slice::from_raw_parts(ltab.slots.as_mut_ptr() as *const AtomicU64, cap * 2) };
        // zeroed (calloc'd, so untouched pages cost nothing) arrays viewed as atomics
        fn zeroed<A>(n: usize) -> &'static [A] {
            let v: Vec<u64> = vec![0; n];
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
                    sl[2 * i + 1].store((v & !0xffff_ffff) | PERSON_SET | pnum[r].load(Relaxed) as u64, Relaxed);
                }
            }
        });
        (nids as usize, pname)
    };
    lap("  links repointed");
    let coll = AtomicBool::new(false);
    // All-uid table: linked ids already map to persons 0..npersons. Each unlinked user id
    // seen on a valid event copy gets the next dense id when first inserted, from per-thread
    // blocks of P ids so every partition receives one id per block.
    let users = Users::new(&mut ltab, nids, pname.len());
    lap(&format!("links: {} edges, {} ids, {} persons", edges.len(), nids, pname.len()));

    // Events phase A: route valid copies directly to people, and record
    // every eligible event ID for the global earliest-ingest decision.
    let ech = chunks(&evb, 1 << 20);
    let shard_capacity = ech.iter().find(|chunk| chunk.len() >= 1 << 16).map_or(0, |sample| {
        let rows = memchr::memchr_iter(b'\n', sample).count();
        let avg_bytes = (sample.len() / rows.max(1)).max(64);
        let input_bytes: usize = ech.iter().map(|chunk| chunk.len()).sum();
        let per_shard = (input_bytes / avg_bytes).div_ceil(SH * rayon::current_num_threads());
        (per_shard + per_shard / 8).min(32_768)
    });
    let window_seconds = w0.div_euclid(1000);
    let folds: Vec<EventFold> = pt(ech.par_iter(),
            || EventFold::new(shard_capacity),
            |mut acc, c| {
                let mut dates = Dates::default();
                let mut batch = Vec::<ParsedEvent>::with_capacity(64);
                let mut short_writer = acc.short_copies.writer();
                let mut block = (0u32, 0u32);
                let mut flush = |batch: &mut Vec<ParsedEvent>| {
                    for mut r in batch.drain(..) {
                        if r.ts != TOMBSTONE {
                            let person = users.person(r.pkey, r.pchk, r.name_ref, &mut block, &coll);
                            let partition = person as usize & (P - 1);
                            let events = &mut acc.people;
                            let Ok(index) = u32::try_from(events.len(partition)) else {
                                coll.store(true, Relaxed);
                                continue;
                            };
                            events.push(partition, EvP {
                                person,
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
                event_rows(c, |_, [id, user, ts, ing, et, pr, pc]| {
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
                        name_ref: 0,
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
                        let Some(name) = pack_ref(user) else {
                            coll.store(true, Relaxed);
                            break 'v;
                        };
                        r.name_ref = name;
                        (r.pkey, r.pchk) = hs(user, seed);
                        users.prefetch(r.pkey);
                        r.ts = (t - w0) as u32;
                    }
                    batch.push(r);
                    if batch.len() == 64 {
                        flush(&mut batch);
                    }
                });
                flush(&mut batch);
                acc
            },
        );
    let (linked_count, pname) = users.names(&pname, &coll);
    lap(&format!("events parsed ({} folds, {} persons)", folds.len(), pname.len()));
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
    // Per-worker click partitions in one huge-page mapping, sized from a row sample.
    let click_capacity = cch.iter().find(|chunk| chunk.len() >= 1 << 16).map_or(0, |sample| {
        let rows = memchr::memchr_iter(b'\n', &sample[..1 << 16]).count().max(1);
        let input_bytes: usize = cch.iter().map(|chunk| chunk.len()).sum();
        let per = (input_bytes / ((1 << 16) / rows).max(1)).div_ceil(P * rayon::current_num_threads());
        (per + per / 8 + 4).min(1 << 16)
    });
    let caccs: Vec<Parts<Click>> = pt(
        cch.par_iter(),
        || Parts::<Click>::new(click_capacity),
        |mut acc, chunk| {
            let mut dates = Dates::default();
            let mut batch = Vec::with_capacity(16);
            let mut flush = |batch: &mut Vec<PendingClick>| {
                for PendingClick { mut click, key, check } in batch.drain(..) {
                    // A user id not in the table has no valid event, so no session to credit.
                    let Some(person) = users.find(key, check, &coll) else { continue };
                    click.person = u64::from(person);
                    acc.push(person as usize & (P - 1), click);
                }
            };
            csv_rows(chunk, |_, [cid, user, cmp, ts]| {
                if cid.is_empty() || user.is_empty() {
                    return;
                }
                let Some(&campaign) = camp.get(cmp) else { return };
                let ts = match dates.parse(ts) {
                    Some(t) if t >= click_start && t < w1 => (t - click_start) as u32,
                    _ => return,
                };
                let (Some(person), Some(id)) = (pack_ref(user), pack_ref(cid)) else {
                    coll.store(true, Relaxed);
                    return;
                };
                let (key, check) = hs(user, seed);
                users.prefetch(key);
                batch.push(PendingClick { click: Click { person, id, ts, campaign }, key, check });
                if batch.len() == 16 {
                    flush(&mut batch);
                }
            });
            flush(&mut batch);
            acc
        },
    );
    lap("clicks parsed");
    if coll.load(Relaxed) {
        return false;
    }

    // write parquet: user_daily in parallel parts, campaign_daily in one file
    use arrow_array::*;
    use arrow_array::builder::StringBuilder;
    use arrow_schema::{DataType, Field, Schema};
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
    let s1 = Arc::new(Schema::new(vec![
        Field::new("user_id", DataType::Utf8, false),
        Field::new("day", DataType::Date32, false),
        Field::new("sessions", DataType::Int64, false),
        Field::new("events", DataType::Int64, false),
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
    // Precompute row bounds and schedule large partitions first. These
    // bounds also cover the dense-ID and click-index widths before writing.
    let ncs = ncamp + 1;
    let mut order = Vec::with_capacity(P);
    for partition in 0..P {
        let events: usize = paccs.iter().map(|fold| fold.len(partition)).sum();
        let clicks: usize = caccs.iter().map(|fold| fold.len(partition)).sum();
        if events > (u32::MAX - linked_count) as usize || clicks > u32::MAX as usize {
            return false;
        }
        order.push((std::cmp::Reverse(events + clicks), partition));
    }
    std::fs::create_dir_all(format!("{out}/user_daily")).unwrap();
    std::fs::create_dir_all(format!("{out}/campaign_daily")).unwrap();
    order.sort_unstable();
    let next = std::sync::atomic::AtomicUsize::new(0);
    // Each existing Rayon worker handles whole partitions and flushes its
    // own Parquet file as soon as it runs out of work.
    let states = rayon::broadcast(|worker| {
        let mut state = PartitionState::new(ncs, ndays, cat_names.len());
        while let Some(&(_, partition)) = order.get(next.fetch_add(1, Relaxed)) {
            state.group(partition, linked_count, &paccs, &caccs);
            state.aggregate(partition, linked_count, &pname, &cat_names, click_start, day0, ndays, ncamp);
        }
        let nrows = state.user.day.len();
        if nrows != 0 {
            let columns = std::mem::take(&mut state.user);
            let path = format!("{out}/user_daily/part-{}.parquet", worker.index());
            write(path, columns.finish(s1.clone()));
        }
        (nrows, state.cd)
    });
    std::mem::forget(paccs);
    std::mem::forget(caccs);
    lap("sessions + credit + aggregate + user parquet");

    let mut cd = vec![[0i64; 4]; ncs * ndays];
    let mut nrows = 0;
    for (count, totals) in states {
        nrows += count;
        for (a, b) in cd.iter_mut().zip(totals) {
            for k in 0..4 {
                a[k] += b[k];
            }
        }
    }
    if nrows == 0 {
        write(format!("{out}/user_daily/part-0.parquet"), RecordBatch::new_empty(s1));
    }
    lap(&format!("merged: user_daily {}", nrows));

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
                cid.append_value(csv_str(n));
                cch2.append_value(csv_str(ch));
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
    camp_job();
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
#[derive(Default)]
struct Session {
    last_event: u32,
    campaign_day: usize,
    revenue: i64,
}

impl Session {
    fn credit(self, totals: &mut [[i64; 4]]) {
        let total = &mut totals[self.campaign_day];
        total[0] += 1;
        total[1] += i64::from(self.revenue > 0);
        total[2] += self.revenue;
    }
}

struct PartitionState {
    cd: Vec<[i64; 4]>,
    counts: Vec<usize>,
    in_active: Vec<bool>,
    active: Vec<u32>,
    catsum: Vec<i64>,
    touched_categories: Vec<u32>,
    unlinked: HashMap<&'static [u8], u32>,
    names: Vec<&'static [u8]>,
    event_people: Vec<u32>,
    click_people_times: Vec<(u32, u32)>,
    click_meta: Vec<ClickMeta>,
    ends: Vec<usize>,
    rows: Vec<u64>,
    sort_scratch: Vec<u64>,
    click_window: Vec<(u32, u32)>,
    user: UserColumns,
}

impl PartitionState {
    fn new(campaigns: usize, days: usize, categories: usize) -> Self {
        Self {
            cd: vec![[0; 4]; campaigns * days],
            counts: vec![0; campaigns],
            in_active: vec![false; campaigns],
            active: Vec::new(),
            catsum: vec![0; categories],
            touched_categories: Vec::new(),
            unlinked: HashMap::new(),
            names: Vec::new(),
            event_people: Vec::new(),
            click_people_times: Vec::new(),
            click_meta: Vec::new(),
            ends: Vec::new(),
            rows: Vec::new(),
            sort_scratch: Vec::new(),
            click_window: Vec::new(),
            user: UserColumns::default(),
        }
    }

    fn group(
        &mut self,
        partition: usize,
        linked_count: u32,
        events: &[People],
        clicks: &[Parts<Click>],
    ) {
        self.unlinked.clear();
        self.names.clear();
        self.event_people.clear();
        self.click_people_times.clear();
        self.click_meta.clear();
        self.ends.clear();
        self.ends.resize(linked_count as usize, 0);

        // The linked namespace is dense in each partition. This one-time
        // exact-name interning for unlinked people is the bridge until the
        // all-UID table supplies their dense IDs at ingest.
        for fold in events {
            for event in fold.records(partition).into_iter().flatten() {
                if let Some((token, _)) = event.session_input() {
                    let person = dense_person(token, linked_count, &mut self.unlinked, &mut self.names)
                        .expect("partition row bound checked");
                    self.event_people.push(person);
                    if person as usize == self.ends.len() {
                        self.ends.push(0);
                    }
                    self.ends[person as usize] += 1;
                }
            }
        }
        for fold in clicks {
            for click in fold.records(partition).into_iter().flatten() {
                let person = if click.person >> 48 == 0 {
                    (click.person as u32) >> PB
                } else if let Some(&id) = self.unlinked.get(unpack_ref(click.person)) {
                    id
                } else {
                    continue;
                };
                if self.ends[person as usize] != 0 {
                    self.click_people_times.push((person, click.ts));
                    self.click_meta.push(ClickMeta { id: click.id, campaign: click.campaign });
                }
            }
        }
        for &(person, _) in &self.click_people_times {
            self.ends[person as usize] += 1;
        }
        let mut total = 0;
        for count in &mut self.ends {
            total += std::mem::replace(count, total);
        }
        self.rows.resize(total, 0);
        let mut emit = |person: u32, payload: u64| {
            let next = &mut self.ends[person as usize];
            self.rows[*next] = payload;
            *next += 1;
        };
        let mut people = self.event_people.iter().copied();
        for fold in events {
            for event in fold.records(partition).into_iter().flatten() {
                if let Some((_, payload)) = event.session_input() {
                    let ts = (payload >> 32) as u32 + LOOK as u32;
                    emit(people.next().unwrap(), timeline_payload(ts, true, payload as u32));
                }
            }
        }
        for (idx, &(person, ts)) in self.click_people_times.iter().enumerate() {
            emit(person, timeline_payload(ts, false, idx as u32));
        }
    }

    fn aggregate(
        &mut self,
        partition: usize,
        linked_count: u32,
        linked_names: &[&[u8]],
        categories: &[&[u8]],
        origin: i64,
        day0: i64,
        days: usize,
        organic: usize,
    ) {
        let Self {
            cd, counts, in_active, active, catsum, touched_categories, names,
            click_meta, ends, rows, sort_scratch, click_window, user, ..
        } = self;
        let mut flush_day = |name: &[u8], row: &UserDay, sums: &mut [i64], touched: &mut Vec<u32>| {
            let category = top_cat(sums, touched, categories);
            user.append(name, row, categories.get(category as usize).copied());
        };
        let mut start = 0;
        for (person, &end) in ends.iter().enumerate() {
            let group = &mut rows[start..end];
            start = end;
            if group.is_empty() {
                continue;
            }
            sort_person(group, sort_scratch);
            let person = person as u32;
            let name = if person < linked_count {
                linked_names[(person as usize) * P + partition]
            } else {
                names[(person - linked_count) as usize]
            };
            let mut session: Option<Session> = None;
            let mut day = UserDay::default();
            let mut head = 0;
            let mut best_click: Option<(u32, u64, usize)> = None;
            click_window.clear();
            for &row in group.iter() {
                let time_key = (row >> 32) as u32;
                let time = time_key >> 1;
                let payload = row as u32;
                if time_key & 1 == 0 {
                    let ClickMeta { id, campaign } = click_meta[payload as usize];
                    let campaign = campaign as usize;
                    click_window.push((time, campaign as u32));
                    counts[campaign] += 1;
                    if !in_active[campaign] {
                        in_active[campaign] = true;
                        active.push(campaign as u32);
                    }
                    if best_click.is_none_or(|(t, old_id, _)| {
                        time > t || (id != old_id && unpack_ref(id) < unpack_ref(old_id))
                    }) {
                        best_click = Some((time, id, campaign));
                    }
                    continue;
                }

                if session.as_ref().is_none_or(|s| time - s.last_event > GAP as u32) {
                    if let Some(previous) = session.take() {
                        previous.credit(cd);
                    }
                    let session_day = (origin + i64::from(time)).div_euclid(DAY);
                    if day.sessions != 0 && day.day != session_day {
                        flush_day(name, &day, catsum, touched_categories);
                        day = UserDay::default();
                    }
                    day.day = session_day;
                    day.sessions += 1;

                    let lower = time - LOOK as u32;
                    while let Some(&(t, campaign)) = click_window.get(head) {
                        if t >= lower {
                            break;
                        }
                        counts[campaign as usize] -= 1;
                        head += 1;
                    }
                    let day_index = (session_day - day0) as usize;
                    active.retain(|&campaign| {
                        let i = campaign as usize;
                        if counts[i] == 0 {
                            in_active[i] = false;
                            false
                        } else {
                            cd[i * days + day_index][3] += 1;
                            true
                        }
                    });
                    let campaign = best_click.filter(|&(t, _, _)| t >= lower)
                        .map_or(organic, |(_, _, campaign)| campaign);
                    session = Some(Session {
                        last_event: time,
                        campaign_day: campaign * days + day_index,
                        revenue: 0,
                    });
                }

                let session = session.as_mut().unwrap();
                session.last_event = time;
                day.events += 1;
                if payload != 0 {
                    let price = (payload & 0xff_ffff) as i64;
                    let category = (payload >> 24) as usize;
                    if catsum[category] == 0 {
                        touched_categories.push(category as u32);
                    }
                    catsum[category] += price;
                    day.revenue += price;
                    session.revenue += price;
                }
            }
            if let Some(session) = session {
                session.credit(cd);
                flush_day(name, &day, catsum, touched_categories);
            }
            for campaign in active.drain(..) {
                counts[campaign as usize] = 0;
                in_active[campaign as usize] = false;
            }
        }
    }
}

fn sort_person(rows: &mut [u64], scratch: &mut Vec<u64>) {
    if rows.len() <= 4096 || rows.len() > u32::MAX as usize {
        rows.sort_unstable();
        return;
    }
    // Only time/type needs sorting. Equal keys of the same type are
    // interchangeable here, including tied clicks handled by their IDs.
    const STEP: u32 = 11;
    const BUCKETS: usize = 1 << STEP;
    scratch.resize(rows.len(), 0);
    let scatter = |input: &[u64], output: &mut [u64], shift: u32| {
        let buckets = 1 << (64 - shift).min(STEP);
        let mask = buckets - 1;
        let mut counts = [0u32; BUCKETS];
        let digit = |key: u64| (key >> shift) as usize & mask;
        for &key in input {
            counts[digit(key)] += 1;
        }
        let mut offset = 0;
        for count in &mut counts[..buckets] {
            let count = std::mem::replace(count, offset);
            offset += count;
        }
        for &key in input {
            let next = &mut counts[digit(key)];
            output[*next as usize] = key;
            *next += 1;
        }
    };
    scatter(rows, scratch, 32);
    scatter(scratch, rows, 43);
    scatter(rows, scratch, 54);
    rows.copy_from_slice(scratch);
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

#[cfg(test)]
mod all_uid_tests {
    use super::*;

    // A linked id with no events must still resolve to its person for clicks, and an unlinked
    // event user gets a fresh dense id that clicks then find.
    #[test]
    fn linked_ids_without_events_resolve_for_clicks() {
        let mut tab = Tab::new(64);
        let (kl, cl) = hs(b"u_linked_no_events", 7);
        let (ke, ce) = hs(b"u_event_user", 7);
        // As the links build leaves it: key, then (check << 32) | PERSON_SET | person.
        let i = (kl as usize) & tab.mask;
        tab.slots[i] = (kl, (u64::from(cl) << 32) | PERSON_SET | 3);
        let coll = AtomicBool::new(false);
        let users = Users::new(&mut tab, 1, 5);
        let mut block = (0, 0);
        let fresh = users.person(ke, ce, pack_ref(b"u_event_user").unwrap(), &mut block, &coll);
        assert!(fresh as usize >= P, "unlinked ids start after the linked block");
        assert_eq!(users.person(ke, ce, 0, &mut block, &coll), fresh);
        assert_eq!(users.find(kl, cl, &coll), Some(3));
        assert_eq!(users.find(ke, ce, &coll), Some(fresh));
        let (kn, cn) = hs(b"u_never_seen", 7);
        assert_eq!(users.find(kn, cn, &coll), None);
        assert!(!coll.load(Relaxed));
        let (_, names) = users.names(&[&b"a"[..], b"b", b"c", b"u_linked_no_events", b"e"], &coll);
        assert_eq!(names[3], b"u_linked_no_events");
        assert_eq!(names[fresh as usize], b"u_event_user");
    }
}
