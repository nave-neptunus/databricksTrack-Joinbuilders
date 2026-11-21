//! Warm single-thread parser microbenchmark. EVENT_KERNEL is an events CSV
//! containing at least 64 MiB of body text. Timing excludes I/O and setup.

use crate::nova::{Dates, EventSink, Fields, scan_event_rows, short_key};
use std::hint::black_box;
use std::io::{BufRead, Read};
use std::path::Path;
use std::time::Instant;

const BLOCK_BYTES: usize = 64 * 1024 * 1024;
const FIELDS: usize = 7;

#[derive(Clone, Copy, Default)]
struct Span {
    start: u32,
    len: u32,
}

impl Span {
    #[inline]
    fn get(self, input: &[u8]) -> &[u8] {
        let start = self.start as usize;
        &input[start..start + self.len as usize]
    }
}

struct Index {
    base: usize,
    columns: [Vec<Span>; FIELDS],
    purchases: Vec<(Span, Span)>,
}

impl<'a> EventSink<'a> for Index {
    fn row(&mut self, _: &'a [u8], fields: impl Fields<'a>) {
        let span = |s: &[u8]| Span {
            start: (s.as_ptr() as usize - self.base) as u32,
            len: s.len() as u32,
        };
        let row = [
            span(fields.field::<0>()),
            span(fields.field::<1>()),
            span(fields.field::<2>()),
            span(fields.field::<3>()),
            span(fields.field::<4>()),
            span(fields.field::<5>()),
            span(fields.field::<6>()),
        ];
        for (column, item) in self.columns.iter_mut().zip(row) {
            column.push(item);
        }
        if fields.field::<4>() == b"purchase" {
            self.purchases.push((row[5], row[6]));
        }
    }
}

#[derive(Default)]
struct Boundaries {
    rows: usize,
    lengths: u64,
}

impl<'a> EventSink<'a> for Boundaries {
    #[inline]
    fn row(&mut self, _: &'a [u8], fields: impl Fields<'a>) {
        let lengths = [
            fields.field::<0>().len() as u8,
            fields.field::<1>().len() as u8,
            fields.field::<2>().len() as u8,
            fields.field::<3>().len() as u8,
            fields.field::<4>().len() as u8,
            fields.field::<5>().len() as u8,
            fields.field::<6>().len() as u8,
            0,
        ];
        self.rows += 1;
        self.lengths = self.lengths.wrapping_add(u64::from_le_bytes(lengths));
    }
}

#[repr(C)]
#[derive(Clone, Copy, Default, PartialEq, Eq)]
struct Compact {
    id: u64,
    user: u64,
    ts: i64,
    ingest: i64,
    product: u64,
    kind: u32,
    price: u32,
}

#[inline]
fn packed(s: &[u8], prefix: u8, base: usize) -> u64 {
    short_key(s, prefix).unwrap_or_else(|| {
        // The diagnostic buffer is at most 64 MiB. The unset high bit marks
        // an exact source span for spellings outside the compact alphabet.
        (s.as_ptr() as usize - base) as u64 | ((s.len() as u64) << 32)
    })
}

#[inline]
fn event_kind(s: &[u8]) -> u32 {
    match s {
        b"view" => 1,
        b"cart" => 2,
        b"purchase" => 3,
        _ => 0,
    }
}

#[inline]
fn ingest(dates: &mut Dates, field: &[u8]) -> i64 {
    if let Some(seconds) = dates.utc_seconds(field) {
        seconds
    } else {
        crate::parse_ts(field).map_or(i64::MAX, |time| time.div_euclid(1000))
    }
}

trait Times {
    fn event(&mut self, field: &[u8]) -> i64;
    fn ingest(&mut self, field: &[u8]) -> i64;
}

impl Times for Dates {
    #[inline]
    fn event(&mut self, field: &[u8]) -> i64 {
        self.parse(field).unwrap_or(i64::MAX)
    }

    #[inline]
    fn ingest(&mut self, field: &[u8]) -> i64 {
        ingest(self, field)
    }
}

#[derive(Clone, Copy, Default)]
struct Midnight {
    date: u64,
    seconds: i64,
}

#[derive(Default)]
struct PackedDates {
    days: [Midnight; 32],
    exact: Dates,
}

impl PackedDates {
    #[inline]
    fn iso_seconds(&mut self, s: &[u8]) -> Option<i64> {
        if s.len() != 20 || s[10] != b'T' || s[19] != b'Z' {
            return None;
        }
        const HYPHENS: u64 = 0xff00_00ff_0000_0000;
        let prefix = crate::rd8(s, 0);
        if prefix & HYPHENS != 0x2d00_002d_0000_0000 {
            return None;
        }
        let date = (prefix & !HYPHENS) | (u64::from(s[8]) << 32) | (u64::from(s[9]) << 56);
        let index = (usize::from(s[8]) * 10 + usize::from(s[9])) & 31;
        let clock = packed_clock(crate::rd8(s, 11))?;
        let entry = &mut self.days[index];
        if entry.date == date && date != 0 {
            return Some(entry.seconds + clock);
        }
        let seconds = crate::parse_ts(s)? / 1000;
        *entry = Midnight {
            date,
            seconds: seconds - clock,
        };
        Some(seconds)
    }
}

impl Times for PackedDates {
    #[inline]
    fn event(&mut self, field: &[u8]) -> i64 {
        self.iso_seconds(field)
            .map_or_else(|| self.exact.event(field), |seconds| seconds * 1000)
    }

    #[inline]
    fn ingest(&mut self, field: &[u8]) -> i64 {
        self.iso_seconds(field)
            .unwrap_or_else(|| self.exact.ingest(field))
    }
}

#[inline]
fn packed_clock(word: u64) -> Option<i64> {
    const CLOCK: u64 = u64::from_le_bytes(*b"00:00:00");
    const DIGIT_MASK: u64 = 0x0f0f_000f_0f00_0f0f;
    const ADD_SIX: u64 = 0x0606_0006_0600_0606;
    let digits = word ^ CLOCK;
    if (digits | digits.wrapping_add(ADD_SIX)) & !DIGIT_MASK != 0 {
        return None;
    }
    // Tens and ones occupy adjacent bytes. Each selected byte remains below
    // 100, so an addition can check all three permitted bounds in parallel.
    const PAIRS: u64 = 0x00ff_0000_ff00_00ff;
    const BIAS: u64 = 0x0044_0000_4400_0068;
    const HIGH: u64 = 0x0080_0000_8000_0080;
    let pairs = (digits.wrapping_mul(10).wrapping_add(digits >> 8)) & PAIRS;
    if (pairs + BIAS) & HIGH != 0 {
        return None;
    }
    let hours = pairs as u8;
    let minutes = (pairs >> 24) as u8;
    let seconds = (pairs >> 48) as u8;
    Some(i64::from(hours) * 3600 + i64::from(minutes) * 60 + i64::from(seconds))
}

struct Decode<'a, T> {
    base: usize,
    dates: T,
    records: &'a mut Vec<Compact>,
}

impl<'a, T: Times> EventSink<'a> for Decode<'_, T> {
    #[inline]
    fn row(&mut self, _: &'a [u8], fields: impl Fields<'a>) {
        let kind = event_kind(fields.field::<4>());
        let (product, price) = if kind == 3 {
            (
                packed(fields.field::<5>(), b'p', self.base),
                crate::parse_price(fields.field::<6>()).unwrap_or(0),
            )
        } else {
            (0, 0)
        };
        self.records.push(Compact {
            id: packed(fields.field::<0>(), b'e', self.base),
            user: packed(fields.field::<1>(), b'u', self.base),
            ts: self.dates.event(fields.field::<2>()),
            ingest: self.dates.ingest(fields.field::<3>()),
            kind,
            product,
            price,
        });
    }
}

fn measure<T>(label: &str, runs: usize, rows: usize, bytes: usize, mut call: impl FnMut() -> T) {
    black_box(call());
    let mut elapsed = Vec::with_capacity(runs);
    for _ in 0..runs {
        let start = Instant::now();
        black_box(call());
        elapsed.push(start.elapsed().as_secs_f64());
    }
    elapsed.sort_unstable_by(f64::total_cmp);
    let ns = elapsed[runs / 2] * 1e9;
    println!(
        "KERNEL {label:20} {:8.2} ns/row {:7.3} ns/byte",
        ns / rows as f64,
        ns / bytes as f64
    );
}

fn field_work(input: &[u8], spans: &[Span], mut decode: impl FnMut(&[u8]) -> u64) -> u64 {
    spans
        .iter()
        .fold(0u64, |sum, &span| sum.wrapping_add(decode(span.get(input))))
}

#[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
fn masks(input: &[u8]) -> [u64; 3] {
    use std::arch::x86_64::*;
    let mut totals = [0u64; 3];
    for block in input.chunks_exact(64) {
        // Every block contains both complete vectors.
        unsafe {
            let a = _mm256_loadu_si256(block.as_ptr().cast());
            let b = _mm256_loadu_si256(block.as_ptr().add(32).cast());
            for (&byte, sum) in [b'\n', b',', b'"'].iter().zip(&mut totals) {
                let needle = _mm256_set1_epi8(byte as i8);
                let lo = _mm256_movemask_epi8(_mm256_cmpeq_epi8(a, needle)) as u32;
                let hi = _mm256_movemask_epi8(_mm256_cmpeq_epi8(b, needle)) as u32;
                *sum = sum.wrapping_add(u64::from(lo) | (u64::from(hi) << 32));
            }
        }
    }
    totals
}

pub(super) fn run(path: &Path) {
    let mut file = std::io::BufReader::new(std::fs::File::open(path).unwrap());
    file.read_until(b'\n', &mut Vec::new()).unwrap();
    let mut data = Vec::with_capacity(BLOCK_BYTES);
    file.take(BLOCK_BYTES as u64)
        .read_to_end(&mut data)
        .unwrap();
    assert_eq!(
        data.len(),
        BLOCK_BYTES,
        "EVENT_KERNEL needs 64 MiB of body text"
    );
    data.truncate(memchr::memrchr(b'\n', &data).unwrap() + 1);
    let input = black_box(data.as_slice());
    let rows = memchr::memchr_iter(b'\n', input).count();
    let bytes = input.len();
    let base = input.as_ptr() as usize;
    let mut index = Index {
        base,
        columns: std::array::from_fn(|_| Vec::with_capacity(rows)),
        purchases: Vec::new(),
    };
    scan_event_rows(input, &mut index);
    let valid = index.columns[0].len();
    println!(
        "KERNEL input bytes={bytes} rows={rows} structural={valid} purchases={}",
        index.purchases.len()
    );
    let runs = 5;
    #[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
    measure("masks_64b", runs, rows, bytes, || masks(input));
    measure("scan_boundaries", runs, rows, bytes, || {
        let mut sink = Boundaries::default();
        scan_event_rows(input, &mut sink);
        (sink.rows, sink.lengths)
    });

    let [id, user, ts, order, kind, _, _] = &index.columns;
    measure("field_control", runs, rows, bytes, || {
        field_work(input, user, |x| x.len() as u64)
    });
    measure("field_event_id", runs, rows, bytes, || {
        field_work(input, id, |x| packed(x, b'e', base))
    });
    measure("field_user", runs, rows, bytes, || {
        field_work(input, user, |x| packed(x, b'u', base))
    });
    measure("field_ts", runs, rows, bytes, || {
        let mut dates = Dates::default();
        field_work(input, ts, |x| dates.parse(x).unwrap_or(i64::MAX) as u64)
    });
    measure("field_ingest", runs, rows, bytes, || {
        let mut dates = Dates::default();
        field_work(input, order, |x| ingest(&mut dates, x) as u64)
    });
    measure("field_type", runs, rows, bytes, || {
        field_work(input, kind, |x| u64::from(event_kind(x)))
    });
    measure("purchase_product", runs, rows, bytes, || {
        index.purchases.iter().fold(0u64, |sum, &(field, _)| {
            sum.wrapping_add(packed(field.get(input), b'p', base))
        })
    });
    measure("purchase_price", runs, rows, bytes, || {
        index.purchases.iter().fold(0u64, |sum, &(_, field)| {
            sum.wrapping_add(u64::from(crate::parse_price(field.get(input)).unwrap_or(0)))
        })
    });

    let mut output = Vec::with_capacity(valid);
    measure("scan_decode_store", runs, rows, bytes, || {
        output.clear();
        let mut sink = Decode {
            base,
            dates: Dates::default(),
            records: &mut output,
        };
        scan_event_rows(input, &mut sink);
        black_box(output.as_slice());
        output.len()
    });
    #[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
    {
        use crate::nova::scan_event_rows_blocks;

        measure("block64_boundaries", runs, rows, bytes, || {
            let mut sink = Boundaries::default();
            scan_event_rows_blocks(input, &mut sink);
            (sink.rows, sink.lengths)
        });

        let mut alternate = Vec::with_capacity(valid);
        measure("block64_decode_store", runs, rows, bytes, || {
            alternate.clear();
            let mut sink = Decode {
                base,
                dates: Dates::default(),
                records: &mut alternate,
            };
            scan_event_rows_blocks(input, &mut sink);
            black_box(alternate.as_slice());
            alternate.len()
        });
        assert!(
            output == alternate,
            "block scanner changed the decoded records"
        );
        println!("KERNEL block64 compact records MATCH");
    }
    let mut packed_output = Vec::with_capacity(valid);
    measure("packed_dates_full", runs, rows, bytes, || {
        packed_output.clear();
        let mut sink = Decode {
            base,
            dates: PackedDates::default(),
            records: &mut packed_output,
        };
        scan_event_rows(input, &mut sink);
        black_box(packed_output.as_slice());
        packed_output.len()
    });
    assert!(
        output == packed_output,
        "packed timestamp decoder changed records"
    );
    println!("KERNEL packed dates compact records MATCH");
    measure("packed_field_ts", runs, rows, bytes, || {
        let mut dates = PackedDates::default();
        field_work(input, ts, |x| dates.event(x) as u64)
    });
    measure("packed_field_ingest", runs, rows, bytes, || {
        let mut dates = PackedDates::default();
        field_work(input, order, |x| dates.ingest(x) as u64)
    });
    let mut stores = Vec::with_capacity(valid);
    measure("record_store_48b", runs, rows, bytes, || {
        stores.clear();
        for ts in 0..valid {
            stores.push(Compact {
                ts: ts as i64,
                ..Compact::default()
            });
        }
        black_box(stores.as_slice());
        stores.len()
    });
    let mut copy = Vec::with_capacity(valid);
    measure("record_copy_48b", runs, rows, bytes, || {
        copy.clear();
        copy.extend_from_slice(black_box(output.as_slice()));
        black_box(copy.as_slice());
        copy.len()
    });
}
