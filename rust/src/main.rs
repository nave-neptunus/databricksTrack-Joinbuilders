// Shopping Sessions pipeline in Rust. This is the submitted pipeline (root Dockerfile).
// Env: DATA_DIR, OUTPUT_DIR, NCPU, TIMING (stage times on stderr), FORCE_SLOW (use the fallback).
//
// Fast engine: input files are memory-mapped; strings are identified by a 64-bit hash plus an
// independent 32-bit check instead of being interned. Valid event copies go straight into person
// partitions. A separate, compact event-ID stream identifies the copies to tombstone before
// sessions. Any detected hash collision (same 64-bit key, different check) or unsupported input
// sends the run to the previous exact engine in slow.rs, so the output never depends on luck.
mod nova;
mod slow;
mod user_parquet;

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

/// Chunks of about `target` bytes whose line boundaries are found only when a worker takes
/// them, so a cold input is not read serially up front. Chunk k of a file holds the lines
/// that start in [k * target, (k + 1) * target); the header line is skipped.
struct LazyChunks<'a> {
    bufs: Vec<&'a [u8]>,
    raw: Vec<(u32, usize, usize)>,
}

impl<'a> LazyChunks<'a> {
    fn new(bufs: &[&'a [u8]], target: usize) -> Self {
        let mut raw = vec![];
        for (f, b) in bufs.iter().enumerate() {
            let mut s = 0;
            while s < b.len() {
                let e = (s + target).min(b.len());
                raw.push((f as u32, s, e));
                s = e;
            }
        }
        LazyChunks { bufs: bufs.to_vec(), raw }
    }
    fn len(&self) -> usize {
        self.raw.len()
    }
    /// The first line start at or after `at` (position 0 is the header, never a data line).
    #[inline]
    fn line_start(b: &[u8], at: usize) -> usize {
        if at >= b.len() {
            return b.len();
        }
        let from = at.saturating_sub(1);
        match memchr::memchr(b'\n', &b[from..]) {
            Some(i) => from + i + 1,
            None => b.len(),
        }
    }
    /// The raw byte range of chunk i (its lines start inside it).
    fn raw(&self, i: usize) -> &'a [u8] {
        let (f, s, e) = self.raw[i];
        &self.bufs[f as usize][s..e]
    }
    fn get(&self, i: usize) -> &'a [u8] {
        let (f, s, e) = self.raw[i];
        let b = self.bufs[f as usize];
        let start = Self::line_start(b, s);
        let end = Self::line_start(b, e).max(start);
        &b[start..end]
    }
    fn total(&self) -> usize {
        self.bufs.iter().map(|b| b.len()).sum()
    }
}

/// Like `pt`, but workers take items in index order from a shared counter, so input is
/// consumed front to back, right behind the read-ahead pool.
fn pt_ordered<T: Send>(n: usize, init: impl Fn() -> T + Sync, f: impl Fn(T, usize) -> T + Sync) -> Vec<T> {
    let next = std::sync::atomic::AtomicUsize::new(0);
    rayon::broadcast(|_| {
        let mut acc = init();
        loop {
            let i = next.fetch_add(1, Relaxed);
            if i >= n {
                break acc;
            }
            acc = f(acc, i);
        }
    })
}

/// Cold-cache read-ahead: threads populate the page cache for 16 MiB ranges of the input
/// mappings in the order the parse consumes them (links, events, clicks). Each range has a
/// ready flag, so a parse worker that gets ahead waits for its range instead of faulting
/// pages in one small read at a time. Flags only order work; correctness never depends on them.
struct ReadAhead {
    // per mapping (by address): start address, length, index of its first range
    maps: Vec<(usize, usize, usize)>,
    ready: Vec<std::sync::atomic::AtomicBool>,
}

const RA_PIECE: usize = 16 << 20;

impl ReadAhead {
    fn start(maps: Vec<&'static memmap2::Mmap>, paths: Vec<&'static str>) -> &'static ReadAhead {
        let threads: usize = std::env::var("BLUE2_IO_THREADS").ok().and_then(|s| s.parse().ok()).unwrap_or(16);
        let mut table = vec![];
        let mut ranges = vec![];
        for (m, path) in maps.iter().zip(&paths) {
            table.push((m.as_ptr() as usize, m.len(), ranges.len()));
            let mut off = 0;
            while off < m.len() {
                ranges.push((*m, *path, off, RA_PIECE.min(m.len() - off)));
                off += RA_PIECE;
            }
        }
        table.sort_unstable();
        let on = threads > 0;
        let ra: &'static ReadAhead = Box::leak(Box::new(ReadAhead {
            maps: table,
            ready: (0..ranges.len()).map(|_| std::sync::atomic::AtomicBool::new(!on)).collect(),
        }));
        if on {
            let ranges: &'static [(&'static memmap2::Mmap, &'static str, usize, usize)] = Box::leak(ranges.into_boxed_slice());
            let populate = std::env::var_os("BLUE2_WARM_POPULATE").is_some();
            let next: &'static std::sync::atomic::AtomicUsize = Box::leak(Box::new(std::sync::atomic::AtomicUsize::new(0)));
            for _ in 0..threads {
                std::thread::spawn(move || {
                    // sendfile to /dev/null fills the page cache without copying to user space
                    // (std::io::copy picks sendfile/splice for a file reader on Linux).
                    let mut null = std::fs::OpenOptions::new().write(true).open("/dev/null").ok();
                    let mut open: Option<(&str, std::fs::File)> = None;
                    loop {
                        let i = next.fetch_add(1, Relaxed);
                        let Some(&(m, path, off, len)) = ranges.get(i) else { break };
                        let mut done = false;
                        if !populate && cfg!(target_os = "linux") {
                            if let Some(sink) = null.as_mut() {
                                if open.as_ref().map(|o| o.0) != Some(path) {
                                    open = std::fs::File::open(path).ok().map(|f| (path, f));
                                }
                                if let Some((_, f)) = open.as_mut() {
                                    use std::io::{Read, Seek, SeekFrom};
                                    if f.seek(SeekFrom::Start(off as u64)).is_ok() {
                                        let reader: &std::fs::File = f;
                                        done = std::io::copy(&mut reader.take(len as u64), sink).is_ok();
                                    }
                                }
                            }
                        }
                        if !done {
                            #[cfg(target_os = "linux")]
                            if m.advise_range(memmap2::Advice::PopulateRead, off, len).is_err() {
                                let _ = m.advise_range(memmap2::Advice::WillNeed, off, len);
                            }
                            #[cfg(not(target_os = "linux"))]
                            let _ = m.advise_range(memmap2::Advice::WillNeed, off, len);
                        }
                        ra.ready[i].store(true, std::sync::atomic::Ordering::Release);
                    }
                });
            }
        }
        ra
    }

    /// Wait (briefly yielding) until the ranges under `b` have been read.
    fn wait(&self, b: &[u8]) {
        if b.is_empty() {
            return;
        }
        let p = b.as_ptr() as usize;
        let k = self.maps.partition_point(|m| m.0 <= p);
        if k == 0 {
            return;
        }
        let (start, len, first) = self.maps[k - 1];
        if p >= start + len {
            return;
        }
        let lo = (p - start) / RA_PIECE;
        let hi = (p + b.len() - 1 - start).min(len - 1) / RA_PIECE;
        for r in first + lo..=first + hi {
            let mut spins = 0u32;
            while !self.ready[r].load(std::sync::atomic::Ordering::Acquire) {
                spins += 1;
                if spins < 64 {
                    std::hint::spin_loop();
                } else {
                    std::thread::sleep(std::time::Duration::from_micros(20));
                }
            }
        }
    }
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

/// Fixed-arity rows already separated by the newline iterator.
/// Collect two masks for the whole row, then decode the field boundaries.
#[cfg(any(all(target_arch = "aarch64", target_endian = "little"), all(target_arch = "x86_64", target_feature = "avx2")))]
#[inline]
fn row_fields<const N: usize>(line: &[u8]) -> Option<[&[u8]; N]> {
    #[cfg(target_arch = "aarch64")]
    const WIDTH: usize = 16;
    #[cfg(target_arch = "x86_64")]
    const WIDTH: usize = 32;

    if !(WIDTH..=128).contains(&line.len()) {
        return fields::<N>(line);
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
    if commas.count_ones() as usize != N - 1 {
        return None;
    }
    const { assert!(N >= 2 && N < 16) };
    let mut cuts = [0; 16];
    for cut in &mut cuts[1..N] {
        *cut = commas.trailing_zeros() as usize + 1;
        commas &= commas - 1;
    }
    cuts[N] = line.len() + 1;
    if quotes == 0 {
        return Some(std::array::from_fn(|i| {
            // Distinct cuts precede the terminal offset.
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
fn mask_rows<'a, const N: usize>(bytes: &'a [u8], mut row: impl FnMut(&'a [u8], [&'a [u8]; N])) {
    #[cfg(any(all(target_arch = "aarch64", target_endian = "little"), all(target_arch = "x86_64", target_feature = "avx2")))]
    for_lines(bytes, |line| {
        if let Some(values) = row_fields::<N>(line) {
            row(line, values);
        }
    });
    #[cfg(not(any(all(target_arch = "aarch64", target_endian = "little"), all(target_arch = "x86_64", target_feature = "avx2"))))]
    csv_rows::<N>(bytes, row);
}

#[inline]
fn event_rows<'a>(bytes: &'a [u8], row: impl FnMut(&'a [u8], [&'a [u8]; 7])) {
    mask_rows::<7>(bytes, row);
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
    mask_rows::<7>(&data, |line, values| actual.push((line, values)));
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
struct People {
    storage: Option<memmap2::MmapMut>,
    stride: usize,
    lengths: Vec<usize>,
    overflow: Vec<Vec<EvP>>,
}

// The mapping is only written through &mut self; shared access reads initialized prefixes.
unsafe impl Send for People {}
unsafe impl Sync for People {}

impl People {
    fn new(reservation: usize) -> Self {
        let storage = if reservation == 0 || std::env::var_os("BLUE2_NO_PEOPLE_MAP").is_some() {
            None
        } else {
            memmap2::MmapMut::map_anon(reservation * P * std::mem::size_of::<EvP>()).ok()
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
    fn push(&mut self, part: usize, row: EvP) {
        let len = self.lengths[part];
        if len < self.stride {
            let base = self.storage.as_mut().unwrap().as_mut_ptr().cast::<EvP>();
            // This partition's next prefix slot is in the mapping and not yet initialized.
            unsafe { base.add(part * self.stride + len).write(row) };
            self.lengths[part] = len + 1;
        } else {
            self.overflow[part].push(row);
        }
    }

    #[inline]
    fn records(&self, part: usize) -> [&[EvP]; 2] {
        let prefix = self.storage.as_ref().map_or(&[][..], |mapping| {
            // Only the recorded prefix is initialized; no writes occur through &self.
            unsafe { std::slice::from_raw_parts(mapping.as_ptr().cast::<EvP>().add(part * self.stride), self.lengths[part]) }
        });
        [prefix, &self.overflow[part]]
    }

    #[inline]
    fn get(&self, part: usize, index: usize) -> &EvP {
        let [prefix, overflow] = self.records(part);
        prefix.get(index).unwrap_or_else(|| &overflow[index - prefix.len()])
    }
}

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

/// n for an id spelled exactly c_<n> with ASCII digits, no leading zero, n < 10^9.
#[inline]
fn campaign_number(c: &[u8]) -> Option<usize> {
    let d = c.strip_prefix(b"c_")?;
    if d.is_empty() || d.len() > 9 || (d[0] == b'0' && d.len() > 1) {
        return None;
    }
    let mut n = 0usize;
    for &b in d {
        let x = b.wrapping_sub(b'0');
        if x > 9 {
            return None;
        }
        n = n * 10 + x as usize;
    }
    Some(n)
}

/// The dense person ID and time are explicit. The validated click ID's
/// position is only dereferenced for a timestamp tie.
#[derive(Clone, Copy)]
struct Click {
    tag: u64,
    person: u32,
    ts: u32,
}

const _: () = assert!(std::mem::size_of::<Click>() == 16);

impl Click {
    #[inline]
    fn campaign(self) -> usize {
        (self.tag >> 48) as usize
    }

    fn id(self) -> &'static [u8] {
        let ptr = (self.tag & ((1 << 48) - 1)) as *const u8;
        let mut len = 0;
        // The validated first field is followed by a comma or closing quote
        // inside the mapped file. No separator occurs inside the ID.
        unsafe {
            while !matches!(*ptr.add(len), b',' | b'"') {
                len += 1;
            }
            std::slice::from_raw_parts(ptr, len)
        }
    }
}

struct PendingClick {
    click: Click,
    key: u64,
    check: u32,
}

#[derive(Default)]
struct UserDay {
    day: i64,
    sessions: i64,
    events: i64,
    revenue: i64,
}

#[inline]
fn csv_str(bytes: &[u8]) -> &str {
    // CSV fields are UTF-8 by contract, and slicing only strips ASCII syntax.
    unsafe { std::str::from_utf8_unchecked(bytes) }
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
    let nova = std::env::var_os("NOVA").as_deref() != Some(std::ffi::OsStr::new("0"));
    if std::env::var_os("FORCE_SLOW").is_some() || !fast(0x243f_6a88_85a3_08d3, nova) {
        if std::env::var_os("TIMING").is_some() {
            eprintln!("fast engine declined; running the exact fallback");
        }
        slow::run();
    }
    std::process::exit(0);
}

// Link endpoints are bucketed by the home slot of the all-uid table. Each
// worker can then build its range with ordinary loads and stores. A chain
// crossing the end of a range is completed after all range owners finish.
const LINK_SHARDS: usize = 64;

#[derive(Clone, Copy)]
struct LinkEnd {
    key: u64,
    name: u64,
    check: u32,
    endpoint: u32,
}

#[derive(Default)]
struct LinkName {
    reference: std::sync::atomic::AtomicU64,
    prefix: std::sync::atomic::AtomicU64,
}

impl LinkName {
    #[inline]
    fn set(&self, reference: u64) {
        let bytes = unpack_ref(reference);
        let mut prefix = [0; 8];
        let len = bytes.len().min(prefix.len());
        prefix[..len].copy_from_slice(&bytes[..len]);
        self.reference.store(reference, Relaxed);
        self.prefix.store(u64::from_be_bytes(prefix), Relaxed);
    }

    #[inline]
    fn bytes(&self) -> &'static [u8] {
        unpack_ref(self.reference.load(Relaxed))
    }
}

enum LinkSlot {
    Inserted(usize),
    Duplicate(u32),
    Spill,
}

#[inline]
fn link_slot(
    slots: &mut [(u64, u64)],
    mut i: usize,
    mask: usize,
    e: LinkEnd,
    node: u32,
    names: &[LinkName],
    collision: &AtomicBool,
) -> LinkSlot {
    while let Some(slot) = slots.get_mut(i) {
        if slot.0 == 0 {
            names[node as usize].set(e.name);
            *slot = (e.key, (u64::from(e.check) << 32) | u64::from(node));
            return LinkSlot::Inserted(i);
        }
        if slot.0 == e.key {
            let other = slot.1 as u32;
            let other_name = names[other as usize].reference.load(Relaxed);
            if (slot.1 >> 32) as u32 != e.check
                || (other_name != e.name && unpack_ref(other_name) != unpack_ref(e.name))
            {
                collision.store(true, Relaxed);
            }
            return LinkSlot::Duplicate(other);
        }
        i = (i + 1) & mask;
    }
    LinkSlot::Spill
}

#[inline]
fn link_root(parent: &[AtomicU32], mut node: u32) -> u32 {
    loop {
        let next = parent[node as usize].load(Relaxed);
        if next == 0 {
            return node;
        }
        let grandparent = parent[(next - 1) as usize].load(Relaxed);
        if grandparent == 0 {
            return next - 1;
        }
        // Path halving keeps the replacement on the same ancestry chain.
        let _ = parent[node as usize].compare_exchange(next, grandparent, Relaxed, Relaxed);
        node = grandparent - 1;
    }
}

fn build_links(
    input: &[&[u8]],
    estimated_users: usize,
    seed: u64,
    nova: bool,
) -> Option<(Tab, usize, usize, Vec<&'static [u8]>)> {
    let chunks = chunks(input, 1 << 18);
    // Physical rows give an upper bound on valid links, including a final
    // unterminated line. Size the table before hashing and bucketing.
    let rows: usize = chunks.par_iter().map(|chunk| {
        memchr::memchr_iter(b'\n', chunk).count() + usize::from(chunk.last() != Some(&b'\n'))
    }).sum();
    let endpoint_bound = rows.checked_mul(2)?;
    if endpoint_bound >= PERSON_SET as usize {
        return None;
    }
    let capacity_hint = (endpoint_bound + 16).max(estimated_users).max(LINK_SHARDS / 2);
    if capacity_hint > 1 << 31 {
        return None;
    }
    let mut table = Tab::new(capacity_hint);
    let range_len = (table.mask + 1) / LINK_SHARDS;
    let shift = range_len.trailing_zeros();
    let collision = AtomicBool::new(false);

    let batches: Vec<_> = chunks.par_iter().map(|chunk| {
        let estimate = (chunk.len() / (12 * LINK_SHARDS)).min(1024);
        let mut ends: [Vec<LinkEnd>; LINK_SHARDS] =
            std::array::from_fn(|_| Vec::with_capacity(estimate));
        let mut count = 0;
        csv_rows(chunk, |_, [a, b]| {
            if a.is_empty() || b.is_empty() {
                return;
            }
            for s in [a, b] {
                let Some(name) = pack_ref(s) else {
                    collision.store(true, Relaxed);
                    return;
                };
                let (key, check) = if nova {
                    let Some(key) = nova::user_key(s) else {
                        collision.store(true, Relaxed);
                        return;
                    };
                    (key, 0)
                } else {
                    hs(s, seed)
                };
                let key = if key == 0 { 1 } else { key };
                let shard = ((key as usize) & table.mask) >> shift;
                ends[shard].push(LinkEnd { key, name, check, endpoint: count });
                count += 1;
            }
        });
        (ends, count)
    }).collect();
    if collision.load(Relaxed) {
        return None;
    }
    lap("  links parsed+hashed");

    // Keep resolved endpoints in their parsed buffers until an entire input
    // batch can be gathered by one worker. Insertion writes only shard-local names.
    let sizes: Vec<_> = batches.iter().map(|(_, count)| *count).collect();
    let nend: u32 = sizes.iter().sum();
    let mut buckets: [Vec<Vec<LinkEnd>>; LINK_SHARDS] =
        std::array::from_fn(|_| Vec::with_capacity(batches.len()));
    for (ends, _) in batches {
        for (bucket, batch) in buckets.iter_mut().zip(ends) {
            bucket.push(batch);
        }
    }
    let mut node_start = 0u32;
    let starts: Vec<_> = buckets.iter().map(|bucket| {
        let start = node_start;
        node_start += bucket.iter().map(|batch| batch.len() as u32).sum::<u32>();
        start
    }).collect();
    let names: Vec<_> = (0..nend).map(|_| LinkName::default()).collect();
    let range_mask = range_len - 1;
    let mut ranges: Vec<_> = table.slots.par_chunks_mut(range_len).zip(&mut buckets)
        .enumerate().map(|(shard, (slots, bucket))| {
            let mut nodes = Vec::new();
            let mut spill = Vec::new();
            for (batch_id, batch) in bucket.iter_mut().enumerate() {
                for (end_id, e) in batch.iter_mut().enumerate() {
                    let node = starts[shard] + nodes.len() as u32;
                    let i = e.key as usize & range_mask;
                    match link_slot(slots, i, usize::MAX, *e, node, &names, &collision) {
                        LinkSlot::Inserted(local) => {
                            nodes.push(((shard * range_len + local) as u32, node));
                            e.key = u64::from(node);
                        }
                        LinkSlot::Duplicate(other) => e.key = u64::from(other),
                        LinkSlot::Spill => spill.push((batch_id, end_id)),
                    }
                }
            }
            (nodes, spill)
        }).collect();
    let mut next: Vec<_> = starts.iter().zip(&ranges)
        .map(|(start, (nodes, _))| start + nodes.len() as u32)
        .collect();
    for (shard, bucket) in buckets.iter_mut().enumerate() {
        for (batch_id, end_id) in std::mem::take(&mut ranges[shard].1) {
            let e = &mut bucket[batch_id][end_id];
            let node = next[shard];
            let i = e.key as usize & table.mask;
            match link_slot(&mut table.slots, i, table.mask, *e, node, &names, &collision) {
                LinkSlot::Inserted(slot) => {
                    ranges[slot >> shift].0.push((slot as u32, node));
                    next[shard] += 1;
                    e.key = u64::from(node);
                }
                LinkSlot::Duplicate(other) => e.key = u64::from(other),
                LinkSlot::Spill => unreachable!("all-uid table has spare slots"),
            }
        }
    }
    if collision.load(Relaxed) {
        return None;
    }
    lap("  links inserted");

    let empty_batch = |_| std::array::from_fn(|_| Vec::new());
    let mut batches: Vec<[Vec<LinkEnd>; LINK_SHARDS]> = sizes.iter().map(empty_batch).collect();
    for (shard, bucket) in buckets.into_iter().enumerate() {
        for (batch, ends) in batches.iter_mut().zip(bucket) {
            batch[shard] = ends;
        }
    }

    let parent: Vec<_> = (0..nend).map(|_| AtomicU32::new(0)).collect();
    batches.into_par_iter().zip(sizes).for_each(|(batch, size)| {
        let mut ends = vec![0u32; size as usize];
        for lane in &batch {
            for e in lane {
                ends[e.endpoint as usize] = e.key as u32;
            }
        }
        for edge in ends.chunks_exact(2) {
            let mut a = link_root(&parent, edge[0]);
            let mut b = link_root(&parent, edge[1]);
            while a != b {
                // Root order is immutable, so concurrent unions cannot form a
                // cycle. The root itself is also the smallest ID in its component.
                let an = &names[a as usize];
                let bn = &names[b as usize];
                let ap = an.prefix.load(Relaxed);
                let bp = bn.prefix.load(Relaxed);
                let (lo, hi) = if ap < bp || (ap == bp && an.bytes() < bn.bytes()) {
                    (a, b)
                } else {
                    (b, a)
                };
                if parent[hi as usize].compare_exchange(0, lo + 1, Relaxed, Relaxed).is_ok() {
                    break;
                }
                a = link_root(&parent, lo);
                b = link_root(&parent, hi);
            }
        }
    });
    lap("  links union-find");

    let roots: Vec<Vec<u32>> = ranges.par_iter().map(|(nodes, _)| {
        let mut roots = Vec::new();
        for &(_, node) in nodes {
            let r = link_root(&parent, node);
            if r == node {
                roots.push(r);
            } else {
                parent[node as usize].store(r + 1, Relaxed);
            }
        }
        roots
    }).collect();
    lap("  links rooted");

    let mut npersons = 0u32;
    let offsets: Vec<_> = roots.iter().map(|roots| {
        let start = npersons;
        npersons += roots.len() as u32;
        start
    }).collect();
    let person: Vec<_> = (0..nend).map(|_| AtomicU32::new(0)).collect();
    let name_groups: Vec<Vec<_>> = roots.par_iter().zip(offsets).map(|(roots, start)| {
        roots.iter().enumerate().map(|(i, &r)| {
            person[r as usize].store(start + i as u32, Relaxed);
            names[r as usize].bytes()
        }).collect()
    }).collect();
    let pname = name_groups.into_iter().flatten().collect();
    lap("  links numbered");

    let nids = ranges.iter().map(|(nodes, _)| nodes.len()).sum();
    table.slots.par_chunks_mut(range_len).zip(&ranges).for_each(|(slots, (nodes, _))| {
        for &(slot, node) in nodes {
            let r = match parent[node as usize].load(Relaxed) {
                0 => node,
                p => p - 1,
            };
            let value = &mut slots[slot as usize & range_mask].1;
            *value = (*value & !0xffff_ffff) | PERSON_SET | u64::from(person[r as usize].load(Relaxed));
        }
    });
    lap("  links repointed");
    Some((table, nend as usize / 2, nids, pname))
}

/// Runs the fast engine. Returns false (having written nothing) if it cannot guarantee exactness.
fn fast(seed: u64, nova: bool) -> bool {
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
    // Mappings live for the whole run (the process exits right after), so the read-ahead
    // threads can hold them.
    let evm: &'static [Option<memmap2::Mmap>] = Box::leak(map_all(&evf).into_boxed_slice());
    let ckm: &'static [Option<memmap2::Mmap>] = Box::leak(map_all(&ckf).into_boxed_slice());
    let lkm: &'static [Option<memmap2::Mmap>] = Box::leak(map_all(&lkf).into_boxed_slice());
    // Start the cold-cache read-ahead before anything else touches the inputs.
    let ra = {
        let files: Vec<&'static str> = lkf.iter().chain(evf.iter()).chain(ckf.iter()).map(|f| &*Box::leak(f.clone().into_boxed_str())).collect();
        let maps: Vec<(&'static memmap2::Mmap, &'static str)> = lkm.iter().chain(evm.iter()).chain(ckm.iter()).zip(files)
            .filter_map(|(m, f)| m.as_ref().map(|m| (m, f))).collect();
        let (maps, paths) = maps.into_iter().unzip();
        ReadAhead::start(maps, paths)
    };
    let smm = map_all(&[format!("{data}/products.csv"), format!("{data}/campaigns.csv")]);
    let evb = as_slices(evm);
    if evb.iter().any(|file| file.len() > u32::MAX as usize) {
        return false;
    }
    let ckb = as_slices(ckm);
    let lkb = as_slices(lkm);
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
    if ncamp >= 1 << 16 {
        return false;
    }
    // Canonical c_<decimal> IDs have unique numeric keys. The bounded
    // direct index avoids hashing these IDs; other spellings still use bytes.
    const MAX_INDEXED_CAMPAIGN: usize = 1 << 20;
    let mut by_number: Vec<u32> = Vec::new();
    for (i, c) in camp_names.iter().enumerate() {
        if let Some(n) = campaign_number(c).filter(|&n| n < MAX_INDEXED_CAMPAIGN) {
            if by_number.len() <= n {
                by_number.resize(n + 1, u32::MAX);
            }
            by_number[n] = i as u32;
        }
    }
    let campaign_of = |c: &[u8]| -> Option<u32> {
        match campaign_number(c).filter(|&n| n < MAX_INDEXED_CAMPAIGN) {
            Some(n) => by_number.get(n).copied().filter(|&i| i != u32::MAX),
            None => camp.get(c).copied(),
        }
    };

    let est_users = evb.iter().find(|b| b.len() >= 1 << 16).map_or(0, |b| {
        let rows = memchr::memchr_iter(b'\n', &b[..1 << 16]).count().max(1);
        let total: usize = evb.iter().map(|b| b.len()).sum();
        total / ((1 << 16) / rows).max(1) / 12
    });
    for b in &lkb {
        ra.wait(b);
    }
    lap("  links read");
    let Some((mut ltab, nedges, nids, pname)) = build_links(&lkb, est_users, seed, nova) else {
        return false;
    };
    let coll = AtomicBool::new(false);
    let users = Users::new(&mut ltab, nids, pname.len());
    lap(&format!("links: {} edges, {} ids, {} persons", nedges, nids, pname.len()));

    // Produce person partitions with exactly the retained, valid events.
    let ech = LazyChunks::new(&evb, 1 << 20);
    let (linked_count, pname, paccs) = if nova {
        let input = nova::EventInput { products: &prod, unknown, start: w0, end: w1 };
        let Some(result) = nova::run_events(&ech, ra, &users, &input, &pname, &coll) else {
            return false;
        };
        result
    } else {
        let shard_capacity = evb.iter().find(|b| b.len() >= 1 << 17).map_or(0, |b| {
            let sample = &b[1 << 16..1 << 17];
            let rows = memchr::memchr_iter(b'\n', sample).count();
            let avg_bytes = (sample.len() / rows.max(1)).max(64);
            let input_bytes: usize = ech.total();
            let per_shard = (input_bytes / avg_bytes).div_ceil(SH * rayon::current_num_threads());
            (per_shard + per_shard / 8).min(32_768)
        });
        let window_seconds = w0.div_euclid(1000);
        let folds: Vec<EventFold> = pt_ordered(ech.len(),
                || EventFold::new(shard_capacity),
                |mut acc, ci| {
                    ra.wait(ech.raw(ci));
                    let c = ech.get(ci);
                    let mut dates = Dates::default();
                    let mut batch = Vec::<ParsedEvent>::with_capacity(16);
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
                        if batch.len() == 16 {
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
        (linked_count, pname, paccs)
    };

    // clicks: parse + validate, route by person. Copies of a click are identical, so a duplicate
    // can change neither the chosen click nor the set of assisting campaigns: no dedup needed.
    let cch = LazyChunks::new(&ckb, 1 << 20);
    let caccs: Vec<Vec<Vec<Click>>> = pt_ordered(
        cch.len(),
        || (0..P).map(|_| Vec::new()).collect::<Vec<Vec<Click>>>(),
        |mut acc, ci| {
            ra.wait(cch.raw(ci));
            let chunk = cch.get(ci);
            let mut dates = Dates::default();
            let mut batch = Vec::with_capacity(16);
            let mut flush = |batch: &mut Vec<PendingClick>| {
                for PendingClick { mut click, key, check } in batch.drain(..) {
                    // A user id not in the table has no valid event, so no session to credit.
                    let Some(person) = users.find(key, check, &coll) else { continue };
                    click.person = person;
                    acc[person as usize & (P - 1)].push(click);
                }
            };
            mask_rows::<4>(chunk, |_, [cid, user, cmp, ts]| {
                if cid.is_empty() || user.is_empty() {
                    return;
                }
                let Some(campaign) = campaign_of(cmp) else { return };
                let ts = match dates.parse(ts) {
                    Some(t) if t >= click_start && t < w1 => (t - click_start) as u32,
                    _ => return,
                };
                let ptr = cid.as_ptr() as u64;
                if ptr >> 48 != 0 {
                    coll.store(true, Relaxed);
                    return;
                };
                let (key, check) = if nova {
                    // A supported link or event user always has a short key.
                    let Some(key) = nova::user_key(user) else { return };
                    (key, 0)
                } else {
                    hs(user, seed)
                };
                users.prefetch(key);
                let tag = ptr | (u64::from(campaign) << 48);
                batch.push(PendingClick { click: Click { tag, person: 0, ts }, key, check });
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
    let props = Arc::new(parquet::file::properties::WriterProperties::builder()
        .set_compression(parquet::basic::Compression::UNCOMPRESSED)
        .set_dictionary_enabled(false)
        .set_statistics_enabled(parquet::file::properties::EnabledStatistics::None)
        .build());
    let write = |path: String, batch: RecordBatch| {
        let f = std::fs::File::create(path).unwrap();
        let mut w = parquet::arrow::ArrowWriter::try_new(f, batch.schema(), Some(props.as_ref().clone())).unwrap();
        w.write(&batch).unwrap();
        w.close().unwrap();
    };
    let user_schema = user_parquet::schema();
    let s2 = Arc::new(Schema::new(vec![
        Field::new("campaign_id", DataType::Utf8, false),
        Field::new("channel", DataType::Utf8, false),
        Field::new("day", DataType::Date32, false),
        Field::new("sessions", DataType::Int64, false),
        Field::new("purchasing_sessions", DataType::Int64, false),
        Field::new("revenue_cents", DataType::Int64, false),
        Field::new("assisted_sessions", DataType::Int64, false),
    ]));
    // Schedule large partitions first.
    let ncs = ncamp + 1;
    let mut order = Vec::with_capacity(P);
    for partition in 0..P {
        let events: usize = paccs.iter().map(|fold| fold.len(partition)).sum();
        let clicks: usize = caccs.iter().map(|fold| fold[partition].len()).sum();
        order.push((std::cmp::Reverse(events + clicks), partition));
    }
    std::fs::create_dir_all(format!("{out}/user_daily")).unwrap();
    std::fs::create_dir_all(format!("{out}/campaign_daily")).unwrap();
    order.sort_unstable();
    let next = std::sync::atomic::AtomicUsize::new(0);
    // Each existing Rayon worker handles whole partitions and flushes its
    // own Parquet file as soon as it runs out of work.
    let states = rayon::broadcast(|worker| {
        let path = format!("{out}/user_daily/part-{}.parquet", worker.index());
        let user = user_parquet::UserParquet::new(path, user_schema.clone());
        let mut state = PartitionState::new(ncs, ndays, cat_names.len(), user);
        while let Some(&(_, partition)) = order.get(next.fetch_add(1, Relaxed)) {
            state.group(partition, linked_count, &paccs, &caccs);
            state.aggregate(partition, &pname, &cat_names, w0, day0, ndays, ncamp);
        }
        state.user.close();
        (state.user_rows, state.cd)
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

struct PartitionState {
    cd: Vec<[i64; 4]>,
    counts: Vec<usize>,
    in_active: Vec<bool>,
    active: Vec<u32>,
    catsum: Vec<i64>,
    touched_categories: Vec<u32>,
    event_ends: Vec<usize>,
    click_ends: Vec<usize>,
    events: Vec<u64>,
    clicks: Vec<Click>,
    /// One pass over the shared EvP records extracts (local person, packed
    /// payload) once; the scatter then reads this cache-resident buffer
    /// instead of traversing the folds and atomically reloading each EvP.
    gather: Vec<(u32, u64)>,
    event_scratch: Vec<u64>,
    click_scratch: Vec<Click>,
    buckets: EventBuckets,
    user: user_parquet::UserParquet,
    user_rows: usize,
}

impl PartitionState {
    fn new(campaigns: usize, days: usize, categories: usize, user: user_parquet::UserParquet) -> Self {
        Self {
            cd: vec![[0; 4]; campaigns * days],
            counts: vec![0; campaigns],
            in_active: vec![false; campaigns],
            active: Vec::new(),
            catsum: vec![0; categories],
            touched_categories: Vec::new(),
            event_ends: Vec::new(),
            click_ends: Vec::new(),
            events: Vec::new(),
            clicks: Vec::new(),
            gather: Vec::new(),
            event_scratch: Vec::new(),
            click_scratch: Vec::new(),
            buckets: EventBuckets::default(),
            user,
            user_rows: 0,
        }
    }

    fn group(
        &mut self,
        partition: usize,
        people: u32,
        event_folds: &[People],
        click_folds: &[Vec<Vec<Click>>],
    ) {
        let Self { event_ends, click_ends, events, clicks, gather, .. } = self;
        event_ends.clear();
        event_ends.resize(people as usize, 0);
        click_ends.clear();
        click_ends.resize(people as usize, 0);
        let event_rows = || event_folds.iter()
            .flat_map(|fold| fold.records(partition))
            .flatten()
            .filter_map(EvP::session_input);
        let click_rows = || click_folds.iter().flat_map(|fold| &fold[partition]);
        // Count and capture the payload in one traversal of the EvP records.
        // The old code walked those records twice (once to count, once to
        // scatter), reloading each atomic timestamp on both passes.
        gather.clear();
        for (person, payload) in event_rows() {
            let person = (person >> PB) as u32;
            gather.push((person, payload));
            event_ends[person as usize] += 1;
        }
        for click in click_rows() {
            let person = (click.person >> PB) as usize;
            if event_ends[person] != 0 {
                click_ends[person] += 1;
            }
        }
        // Retain the original event counts until the click scatter is done.
        // The prefix sums then become exclusive ends as records are placed.
        clicks.resize(prefix_starts(click_ends), Click { tag: 0, person: 0, ts: 0 });
        for &click in click_rows() {
            let person = (click.person >> PB) as usize;
            if event_ends[person] != 0 {
                let next = &mut click_ends[person];
                clicks[*next] = click;
                *next += 1;
            }
        }
        events.resize(prefix_starts(event_ends), 0);
        // The prefix sums in event_ends are now exclusive write cursors.
        for &(person, payload) in gather.iter() {
            let next = &mut event_ends[person as usize];
            events[*next] = payload;
            *next += 1;
        }
    }

    fn aggregate(
        &mut self,
        partition: usize,
        pname: &[&[u8]],
        categories: &[&[u8]],
        origin: i64,
        day0: i64,
        days: usize,
        organic: usize,
    ) {
        let Self {
            cd, counts, in_active, active, catsum, touched_categories,
            event_ends, click_ends, events, clicks, gather: _, event_scratch, click_scratch, buckets,
            user, user_rows,
        } = self;
        let (mut event_start, mut click_start) = (0, 0);
        for (person, (&event_end, &click_end)) in event_ends.iter().zip(click_ends.iter()).enumerate() {
            let events = &mut events[event_start..event_end];
            let clicks = &mut clicks[click_start..click_end];
            (event_start, click_start) = (event_end, click_end);
            if events.is_empty() {
                continue;
            }
            sort_timeline(clicks, click_scratch, |click| click.ts);
            let name = pname[person * P + partition];
            let write_day = |row: &UserDay, sums: &mut [i64], touched: &mut Vec<u32>| {
                let category = top_cat(sums, touched, categories);
                user.append(name, row, categories.get(category as usize).copied());
                *user_rows += 1;
            };
            let (mut lo, mut hi, mut best_click) = (0, 0, None::<Click>);
            let mut attribute = |start, session_day, revenue| {
                let time = start + LOOK as u32;
                let day_index = (session_day - day0) as usize;
                let lower = start;
                while lo < hi && clicks[lo].ts < lower {
                    counts[clicks[lo].campaign()] -= 1;
                    lo += 1;
                }
                while let Some(&click) = clicks.get(hi) {
                    if click.ts > time {
                        break;
                    }
                    hi += 1;
                    if click.ts < lower {
                        // This click has already expired before its first session.
                        lo += 1;
                        continue;
                    }
                    let campaign = click.campaign();
                    counts[campaign] += 1;
                    if !in_active[campaign] {
                        in_active[campaign] = true;
                        active.push(campaign as u32);
                    }
                    if best_click.is_none_or(|old| click.ts != old.ts || click.id() < old.id()) {
                        best_click = Some(click);
                    }
                }
                active.retain(|&campaign| {
                    let c = campaign as usize;
                    if counts[c] == 0 {
                        in_active[c] = false;
                        false
                    } else {
                        cd[c * days + day_index][3] += 1;
                        true
                    }
                });
                let campaign = best_click.filter(|click| click.ts >= lower).map_or(organic, Click::campaign);
                let total = &mut cd[campaign * days + day_index];
                total[0] += 1;
                total[1] += i64::from(revenue > 0);
                total[2] += revenue;
            };

            if events.len() < BUCKET_MIN_LEN {
                sort_events(events, event_scratch);
                aggregate_sessions(
                    events, origin, catsum, touched_categories,
                    |event| {
                        let time = (event >> 32) as u32;
                        (time, time)
                    },
                    |event, sums, touched| (1, add_purchase(event as u32, sums, touched)),
                    write_day, &mut attribute,
                );
            } else {
                buckets.build(events);
                aggregate_sessions(
                    &buckets.used, origin, catsum, touched_categories,
                    |index| {
                        let b = &buckets.by_time[index];
                        (b.first, b.last)
                    },
                    |index, sums, touched| buckets.aggregate(index, sums, touched),
                    write_day, &mut attribute,
                );
            }
            for campaign in active.drain(..) {
                counts[campaign as usize] = 0;
                in_active[campaign as usize] = false;
            }
        }
    }
}

/// Each span is either a single event or an exact summary of a connected
/// half-hour bucket. Session credit and day totals consume either directly.
#[inline]
fn aggregate_sessions<T: Copy>(
    rows: &[T],
    origin: i64,
    catsum: &mut [i64],
    touched: &mut Vec<u32>,
    bounds: impl Fn(T) -> (u32, u32),
    aggregate: impl Fn(T, &mut [i64], &mut Vec<u32>) -> (i64, i64),
    mut write_day: impl FnMut(&UserDay, &mut [i64], &mut Vec<u32>),
    mut credit: impl FnMut(u32, i64, i64),
) {
    let mut day = UserDay::default();
    let mut i = 0;
    while i < rows.len() {
        let (start, _) = bounds(rows[i]);
        let session_day = (origin + i64::from(start)).div_euclid(DAY);
        if day.sessions != 0 && day.day != session_day {
            write_day(&day, catsum, touched);
            day = UserDay::default();
        }
        day.day = session_day;
        day.sessions += 1;

        let mut previous = start;
        let mut revenue = 0i64;
        while let Some(&row) = rows.get(i) {
            let (first, last) = bounds(row);
            if first - previous > GAP as u32 {
                break;
            }
            previous = last;
            let (events, price) = aggregate(row, catsum, touched);
            day.events += events;
            revenue += price;
            i += 1;
        }
        day.revenue += revenue;
        credit(start, session_day, revenue);
    }
    write_day(&day, catsum, touched);
}

#[inline]
fn add_purchase(payload: u32, sums: &mut [i64], touched: &mut Vec<u32>) -> i64 {
    if payload == 0 {
        return 0;
    }
    let price = i64::from(payload & 0xff_ffff);
    let category = (payload >> 24) as usize;
    if sums[category] == 0 {
        touched.push(category as u32);
    }
    sums[category] += price;
    price
}

const BUCKET_MIN_LEN: usize = 128;
const MAX_EVENT_BUCKETS: usize = ((1u32 << 31) / GAP as u32) as usize + 1;

#[derive(Default)]
struct EventBucket {
    first: u32,
    last: u32,
    count: i64,
    /// One-based index of the most recent purchase in the bucket.
    purchase: usize,
}

#[derive(Default)]
struct EventBuckets {
    by_time: Vec<EventBucket>,
    used: Vec<usize>,
    purchases: Vec<(u32, usize)>,
}

impl EventBuckets {
    fn build(&mut self, events: &[u64]) {
        for index in self.used.drain(..) {
            self.by_time[index] = EventBucket::default();
        }
        self.by_time.resize_with(MAX_EVENT_BUCKETS, EventBucket::default);
        self.purchases.clear();
        for &event in events {
            let time = (event >> 32) as u32;
            // A half-hour bucket is connected for every input distribution.
            // Only its endpoints matter when joining adjacent occupied buckets.
            let index = (time / GAP as u32) as usize;
            let bucket = &mut self.by_time[index];
            if bucket.count == 0 {
                bucket.first = time;
                bucket.last = time;
                self.used.push(index);
            } else {
                bucket.first = bucket.first.min(time);
                bucket.last = bucket.last.max(time);
            }
            bucket.count += 1;
            let payload = event as u32;
            if payload != 0 {
                self.purchases.push((payload, bucket.purchase));
                bucket.purchase = self.purchases.len();
            }
        }
        self.used.sort_unstable();
    }

    fn aggregate(&self, index: usize, sums: &mut [i64], touched: &mut Vec<u32>) -> (i64, i64) {
        let bucket = &self.by_time[index];
        let mut node = bucket.purchase;
        let mut revenue = 0;
        while node != 0 {
            let (payload, previous) = self.purchases[node - 1];
            revenue += add_purchase(payload, sums, touched);
            node = previous;
        }
        (bucket.count, revenue)
    }
}

/// Convert group sizes to write cursors, returning the total number of rows.
fn prefix_starts(counts: &mut [usize]) -> usize {
    let mut total = 0;
    for count in counts {
        total += std::mem::replace(count, total);
    }
    total
}

/// Events are packed as `(timestamp << 32) | payload`, so ordering the whole
/// word orders by timestamp. Timestamp ties may be broken by payload: session
/// bounds read only the timestamp and category sums are commutative, so any
/// order within a timestamp produces the same output. Small runs use a direct
/// insertion sort without a comparator closure, medium runs use the library
/// primitive sort, and larger runs keep the byte radix.
#[inline]
fn sort_events(events: &mut [u64], scratch: &mut Vec<u64>) {
    if events.len() <= INSERTION_MAX_LEN {
        for i in 1..events.len() {
            let value = events[i];
            let mut j = i;
            while j > 0 && events[j - 1] > value {
                events[j] = events[j - 1];
                j -= 1;
            }
            events[j] = value;
        }
        return;
    }
    if events.len() <= RADIX_MIN_LEN {
        events.sort_unstable();
        return;
    }
    scratch.resize(events.len(), events[0]);
    radix_time(events, scratch, |event| (event >> 32) as u32, 24);
}

const INSERTION_MAX_LEN: usize = 32;

fn sort_timeline<T: Copy>(rows: &mut [T], scratch: &mut Vec<T>, time: impl Fn(T) -> u32 + Copy) {
    if rows.len() <= RADIX_MIN_LEN {
        rows.sort_unstable_by_key(|&row| time(row));
        return;
    }

    scratch.resize(rows.len(), rows[0]);
    radix_time(rows, scratch, time, 24);
}

const RADIX_MIN_LEN: usize = 96;

/// Stable counting scatter on the significant timestamp bytes. Tied events
/// and tied clicks can arrive in any order within their timestamp.
fn radix_time<T: Copy>(rows: &mut [T], scratch: &mut [T], time: impl Fn(T) -> u32 + Copy, shift: u32) {
    let digit = |row| ((time(row) >> shift) & 255) as usize;
    let mut counts = [0usize; 256];
    for &row in rows.iter() {
        counts[digit(row)] += 1;
    }
    if !counts.contains(&rows.len()) {
        let mut offsets = counts;
        prefix_starts(&mut offsets);
        for &row in rows.iter() {
            let next = &mut offsets[digit(row)];
            scratch[*next] = row;
            *next += 1;
        }
        rows.copy_from_slice(scratch);
    }
    if shift == 0 {
        return;
    }
    let mut start = 0;
    for count in counts {
        let end = start + count;
        if count > RADIX_MIN_LEN {
            radix_time(&mut rows[start..end], &mut scratch[start..end], time, shift - 8);
        } else if count > 1 {
            rows[start..end].sort_unstable_by_key(|&row| time(row));
        }
        start = end;
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

#[cfg(test)]
#[test]
fn lazy_chunks_cover_every_data_line_once() {
    let mut x = 0x9e37_79b9_7f4a_7c15u64;
    for case in 0..300 {
        let mut file = b"h1,h2\n".to_vec();
        if case % 7 == 0 {
            file.clear(); // header-only and empty variants below
        }
        let rows = (case * 13) % 97;
        for r in 0..rows {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            let len = (x % 9) as usize;
            file.extend(std::iter::repeat_n(b'a' + (r % 26) as u8, len));
            file.push(b'\n');
        }
        if case % 3 == 0 {
            file.extend_from_slice(b"tail,no,newline");
        }
        let mut want = Vec::new();
        if let Some(h) = memchr::memchr(b'\n', &file) {
            for_lines(&file[h + 1..], |l| want.push(l.to_vec()));
        }
        for target in [1, 2, 3, 5, 8, 64, 1 << 20] {
            let lc = LazyChunks::new(&[&file[..]], target);
            let mut got = Vec::new();
            for i in 0..lc.len() {
                for_lines(lc.get(i), |l| got.push(l.to_vec()));
            }
            assert_eq!(got, want, "case {case} target {target}");
        }
    }
}
