//! Nova's event pipeline. The scanner emits compact dedup records and session payloads.

use crate::{
    DAY, EvP, LazyChunks, P, People, ReadAhead, Users, event_rows, hs, lap, pack_ref, parse_price,
    unpack_ref,
};
use ahash::AHashMap;
use std::mem::size_of;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicUsize, Ordering::Relaxed};

const SHARDS: usize = 256;
const BLOCK_BYTES: usize = 4096;
const SLAB_BYTES: usize = 16 << 20;
const SHORT_KEY: u64 = 1 << 63;
const INVALID: u32 = u32::MAX;

/// Each payload byte below contributes seven exact bits. Its eighth bit is
/// available for shape metadata. Prefix, length and all tail bytes round-trip.
#[inline]
fn short_key(id: &[u8], prefix: u8) -> Option<u64> {
    let (&first, tail) = id.split_first()?;
    if first | 32 != prefix {
        return None;
    }
    let (bytes, len) = match tail.len() {
        8 => (u64::from_le_bytes(tail.try_into().unwrap()), 1 << 31),
        7 => (crate::rd8(id, 0) >> 8, (1 << 7) | (1 << 15) | (1 << 23)),
        n @ 0..=6 => {
            let mut bytes = [0; 8];
            bytes[..n].copy_from_slice(tail);
            let n = n as u64;
            let len = (n & 1) << 7 | (n & 2) << 14 | (n & 4) << 21;
            (u64::from_le_bytes(bytes), len)
        }
        _ => return None,
    };
    if bytes & 0x8080_8080_8080_8080 != 0 {
        return None;
    }
    Some(bytes | len | ((u64::from(first ^ prefix)) << 50) | SHORT_KEY)
}

// Odd multiplication modulo 2^63 and xor-shifts permute the complete
// short-ID payload. A mixed table key is still an exact identity.
#[inline]
fn permute_short(key: u64) -> u64 {
    let value = key & (SHORT_KEY - 1);
    let value = value ^ (value >> 27);
    let value = value.wrapping_mul(0x9e37_79b9_7f4a_7c15) & (SHORT_KEY - 1);
    (value ^ (value >> 31)) | SHORT_KEY
}

#[inline]
pub(super) fn user_key(uid: &[u8]) -> Option<u64> {
    short_key(uid, b'u').map(permute_short)
}

/// A producer owns disjoint blocks within demand-paged slabs. Stream order is
/// described by its block IDs. No initialized record moves when a stream grows.
struct Streams<T, const N: usize> {
    slabs: Vec<memmap2::MmapMut>,
    blocks: [Vec<u32>; N],
    tails: [*mut T; N],
    used: [usize; N],
    next: usize,
}

// Pointers address the owned mappings, which remain stable when moved.
unsafe impl<T: Send, const N: usize> Send for Streams<T, N> {}
// Shared readers can access only initialized records. They never write the tails.
unsafe impl<T: Sync, const N: usize> Sync for Streams<T, N> {}

impl<T: Copy, const N: usize> Streams<T, N> {
    const ROWS: usize = BLOCK_BYTES / size_of::<T>();
    const SLAB_BLOCKS: usize = SLAB_BYTES / BLOCK_BYTES;

    fn new() -> Self {
        Self {
            slabs: Vec::new(),
            blocks: std::array::from_fn(|_| Vec::new()),
            tails: [std::ptr::null_mut(); N],
            used: [Self::ROWS; N],
            next: 0,
        }
    }

    #[cold]
    fn extend(&mut self, stream: usize) {
        let next = self.next;
        let slab = next / Self::SLAB_BLOCKS;
        if slab == self.slabs.len() {
            let map = memmap2::MmapMut::map_anon(SLAB_BYTES).expect("Nova stream allocation");
            #[cfg(target_os = "linux")]
            let _ = map.advise(memmap2::Advice::HugePage);
            self.slabs.push(map);
        }
        let base = self.slabs[slab].as_mut_ptr();
        self.tails[stream] = base
            .wrapping_add(next % Self::SLAB_BLOCKS * BLOCK_BYTES)
            .cast();
        self.blocks[stream].push(u32::try_from(next).expect("Nova block index"));
        self.used[stream] = 0;
        self.next += 1;
    }

    #[inline]
    fn push(&mut self, stream: usize, row: T) {
        if self.used[stream] == Self::ROWS {
            self.extend(stream);
        }
        let slot = self.used[stream];
        // The cursor always points within this producer's current block.
        unsafe { self.tails[stream].add(slot).write(row) };
        self.used[stream] = slot + 1;
    }

    #[inline]
    fn get(&self, stream: usize, index: usize) -> &T {
        let id = self.blocks[stream][index / Self::ROWS] as usize;
        let slab = &self.slabs[id / Self::SLAB_BLOCKS];
        let offset = id % Self::SLAB_BLOCKS * BLOCK_BYTES;
        // Dedup indices refer only to rows returned by `records`.
        unsafe {
            &*slab
                .as_ptr()
                .add(offset)
                .cast::<T>()
                .add(index % Self::ROWS)
        }
    }

    fn records(&self, stream: usize) -> impl Iterator<Item = &T> {
        let block_ids = &self.blocks[stream];
        block_ids.iter().enumerate().flat_map(move |(i, &id)| {
            let id = id as usize;
            let slab = &self.slabs[id / Self::SLAB_BLOCKS];
            let offset = id % Self::SLAB_BLOCKS * BLOCK_BYTES;
            let len = if i + 1 == block_ids.len() {
                self.used[stream]
            } else {
                Self::ROWS
            };
            // These block IDs belong to the stream. All preceding blocks
            // are full and only the initialized tail prefix is exposed.
            unsafe { std::slice::from_raw_parts(slab.as_ptr().add(offset).cast::<T>(), len) }
        })
    }

    fn len(&self, stream: usize) -> usize {
        let blocks = self.blocks[stream].len();
        if blocks == 0 {
            0
        } else {
            (blocks - 1) * Self::ROWS + self.used[stream]
        }
    }
}

const ROW_BITS: u32 = 31 - P.trailing_zeros();
const ROW_LIMIT: usize = 1 << ROW_BITS;

#[repr(C)]
#[derive(Clone, Copy)]
struct CopyRecord {
    key: u64,
    ingest: i32,
    output: u32,
}
const _: () = assert!(size_of::<CopyRecord>() == 16);

impl CopyRecord {
    #[inline]
    fn tombstone(self, people: &People) {
        if self.output != INVALID {
            let partition = (self.output >> ROW_BITS) as usize;
            let row = self.output as usize & (ROW_LIMIT - 1);
            people.get(partition, row).ts.store(INVALID, Relaxed);
        }
    }
}

struct EventParts {
    copies: Streams<CopyRecord, SHARDS>,
    people: People,
}

#[derive(Clone, Copy)]
struct Pending {
    key: u64,
    uid: u64,
    name: u64,
    ingest: i32,
    ts: u32,
    price: u32,
}

impl Pending {
    const EMPTY: Self = Self {
        key: 0,
        uid: 0,
        name: 0,
        ingest: 0,
        ts: INVALID,
        price: 0,
    };
}

// The common event row spans three AVX2 vectors. Read each vector once for
// newlines, commas, and quote syntax. Every load stays inside the mapped chunk.
#[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
#[target_feature(enable = "avx2,pclmulqdq")]
unsafe fn fused_event_rows<'a>(mut bytes: &'a [u8], mut row: impl FnMut(&'a [u8], [&'a [u8]; 7])) {
    use std::arch::x86_64::*;

    let mask =
        |value, byte| _mm256_movemask_epi8(_mm256_cmpeq_epi8(value, _mm256_set1_epi8(byte))) as u32;
    while bytes.len() >= 96 {
        let (a, b) = unsafe {
            let ptr = bytes.as_ptr();
            (
                _mm256_loadu_si256(ptr.cast()),
                _mm256_loadu_si256(ptr.add(32).cast()),
            )
        };
        if mask(a, b'\n' as i8) | mask(b, b'\n' as i8) != 0 {
            let n = memchr::memchr(b'\n', &bytes[..64]).unwrap();
            if let Some(fields) = crate::event_fields(&bytes[..n]) {
                row(&bytes[..n], fields);
            }
            bytes = &bytes[n + 1..];
            continue;
        }
        let c = unsafe { _mm256_loadu_si256(bytes.as_ptr().add(64).cast()) };
        let newline = mask(c, b'\n' as i8);
        if newline == 0 {
            let n = memchr::memchr(b'\n', &bytes[96..]).map_or(bytes.len(), |n| n + 96);
            if let Some(fields) = crate::event_fields(&bytes[..n]) {
                row(&bytes[..n], fields);
            }
            bytes = &bytes[(n + 1).min(bytes.len())..];
            continue;
        }
        let n = newline.trailing_zeros();
        let clip = (1u32 << n) - 1;
        let line = &bytes[..64 + n as usize];
        let join = |byte| {
            u128::from(mask(a, byte))
                | (u128::from(mask(b, byte)) << 32)
                | (u128::from(mask(c, byte) & clip) << 64)
        };
        let commas = join(b',' as i8);
        if commas.count_ones() == 6 {
            let quotes = join(b'"' as i8);
            let terminal = 1u128 << line.len();
            // Prefix XOR pairs every opening quote with its closing quote.
            // A quote pair must cover a whole field and cannot cross a comma.
            let quote_syntax = if quotes == 0 {
                true
            } else {
                let parity = |bits: u64| {
                    let x = _mm_set_epi64x(0, bits as i64);
                    _mm_cvtsi128_si64(_mm_clmulepi64_si128::<0>(x, _mm_set_epi64x(0, -1))) as u64
                };
                let lo = parity(quotes as u64);
                let hi = parity((quotes >> 64) as u64) ^ 0u64.wrapping_sub(lo >> 63);
                let inside = (u128::from(hi) << 64) | u128::from(lo);
                let opens = (commas << 1) | 1;
                let closes = (commas | terminal) >> 1;
                inside & (commas | terminal) == 0
                    && quotes & inside & !opens == 0
                    && quotes & !inside & !closes == 0
            };
            if quote_syntax {
                let mut cuts = [0; 8];
                let mut remaining = commas;
                for cut in &mut cuts[1..7] {
                    *cut = remaining.trailing_zeros() as usize + 1;
                    remaining &= remaining - 1;
                }
                cuts[7] = line.len() + 1;
                let fields = std::array::from_fn(|i| {
                    let start = cuts[i];
                    let end = cuts[i + 1] - 1;
                    // A valid quoted field consumes both endpoints. `get` also covers
                    // an empty final field whose start equals the newline offset.
                    let quoted = (line.get(start) == Some(&b'"')) as usize;
                    unsafe { line.get_unchecked(start + quoted..end - quoted) }
                });
                row(line, fields);
            }
        }
        bytes = &bytes[line.len() + 1..];
    }
    event_rows(bytes, row);
}

#[inline]
fn scan_event_rows<'a, const FUSED: bool>(
    bytes: &'a [u8],
    row: impl FnMut(&'a [u8], [&'a [u8]; 7]),
) {
    #[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
    if FUSED && std::is_x86_feature_detected!("pclmulqdq") {
        // AVX2 is required by this compilation and PCLMULQDQ was checked above.
        return unsafe { fused_event_rows(bytes, row) };
    }
    event_rows(bytes, row);
}

#[inline]
fn swar_digits(word: u64) -> bool {
    const HIGH: u64 = 0xf0f0_f0f0_f0f0_f0f0;
    (word & HIGH) | ((word.wrapping_add(0x0606_0606_0606_0606) & HIGH) >> 4)
        == 0x3333_3333_3333_3333
}

#[inline]
fn swar_value(word: u64) -> u64 {
    let value = word - 0x3030_3030_3030_3030;
    let pairs = value.wrapping_mul(10).wrapping_add(value >> 8);
    let low = (pairs & 0x0000_00ff_0000_00ff).wrapping_mul(100 + (1_000_000 << 32));
    let high = ((pairs >> 16) & 0x0000_00ff_0000_00ff).wrapping_mul(1 + (10_000 << 32));
    low.wrapping_add(high) >> 32
}

#[derive(Default)]
struct Dates([([u8; 10], i64); 32]);

impl Dates {
    #[inline]
    fn utc_seconds(&mut self, s: &[u8]) -> Option<i64> {
        if s.len() != 20
            || s[4] != b'-'
            || s[10] != b'T'
            || s[13] != b':'
            || s[16] != b':'
            || s[19] != b'Z'
        {
            return None;
        }
        let date: [u8; 10] = s[..10].try_into().unwrap();
        let index = (date[8] as usize * 10 + date[9] as usize) & 31;
        let entry = &mut self.0[index];
        let h = crate::d2(s, 11)?;
        let m = crate::d2(s, 14)?;
        let sec = crate::d2(s, 17)?;
        if h > 23 || m > 59 || sec > 59 {
            return None;
        }
        let time = h * 3600 + m * 60 + sec;
        if entry.0 == date {
            return Some(entry.1 + time);
        }
        let seconds = crate::parse_ts(s)? / 1000;
        *entry = (date, seconds - time);
        Some(seconds)
    }

    #[inline]
    fn parse(&mut self, s: &[u8]) -> Option<i64> {
        if s.len() == 20 && s[19] == b'Z' {
            return self.utc_seconds(s).map(|t| t * 1000);
        }
        if s.len() == 13 {
            let leading = (crate::rd8(s, 0) << 24) | 0x30_3030;
            let trailing = crate::rd8(s, 5);
            if swar_digits(leading) && swar_digits(trailing) {
                return Some((swar_value(leading) * 100_000_000 + swar_value(trailing)) as i64);
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
        crate::parse_ts(s)
    }
}

pub(super) struct EventInput<'a> {
    pub(super) products: &'a AHashMap<&'a [u8], u32>,
    pub(super) unknown: u32,
    pub(super) start: i64,
    pub(super) end: i64,
}

#[inline]
fn event_hash(key: u64) -> u64 {
    if key & SHORT_KEY != 0 {
        key
    } else {
        hs(unpack_ref(key), 0).0
    }
}

fn events(
    work: &LazyChunks<'_>,
    read_ahead: &ReadAhead,
    users: &Users<'_>,
    input: &EventInput<'_>,
    declined: &AtomicBool,
) -> Vec<EventParts> {
    let next = AtomicUsize::new(0);
    let window_seconds = input.start.div_euclid(1000);
    let reservation = (work.total() / 72)
        .div_ceil(P * rayon::current_num_threads())
        .min(32_768);
    rayon::broadcast(|_| {
        let mut parts = EventParts {
            copies: Streams::new(),
            people: People::new(reservation),
        };
        let mut dates = Dates::default();
        let mut ids = (0, 0);
        let mut batch = [Pending::EMPTY; 16];
        let mut len = 0;
        let mut flush = |rows: &[Pending]| {
            for pending in rows {
                let mut record = CopyRecord {
                    key: pending.key,
                    ingest: pending.ingest,
                    output: INVALID,
                };
                if pending.ts != INVALID {
                    let person = users.person(pending.uid, 0, pending.name, &mut ids, declined);
                    let partition = person as usize & (P - 1);
                    let index = parts.people.len(partition);
                    if index >= ROW_LIMIT {
                        declined.store(true, Relaxed);
                        continue;
                    }
                    parts.people.push(
                        partition,
                        EvP {
                            person,
                            ts: AtomicU32::new(pending.ts),
                            price: pending.price,
                        },
                    );
                    record.output = (partition as u32) << ROW_BITS | index as u32;
                }
                parts
                    .copies
                    .push((event_hash(record.key) >> 55) as u8 as usize, record);
            }
        };
        loop {
            let index = next.fetch_add(1, Relaxed);
            if index >= work.len() {
                break;
            }
            read_ahead.wait(work.raw(index));
            let chunk = work.get(index);
            scan_event_rows::<true>(chunk, |_, [id, user, ts, ingest, kind, product, price]| {
                if id.is_empty() {
                    return;
                }
                let key = match short_key(id, b'e') {
                    Some(key) => permute_short(key),
                    None => {
                        let Some(name) = pack_ref(id).filter(|name| name & SHORT_KEY == 0) else {
                            declined.store(true, Relaxed);
                            return;
                        };
                        name
                    }
                };
                let Some(seconds) = dates.utc_seconds(ingest) else {
                    declined.store(true, Relaxed);
                    return;
                };
                let Ok(order) = i32::try_from(seconds - window_seconds) else {
                    declined.store(true, Relaxed);
                    return;
                };
                let mut pending = Pending::EMPTY;
                pending.key = key;
                pending.ingest = order;
                'valid: {
                    if user.is_empty() || product.is_empty() {
                        break 'valid;
                    }
                    let purchase = match kind {
                        b"purchase" => true,
                        b"view" | b"cart" => false,
                        _ => break 'valid,
                    };
                    let Some(time) = dates.parse(ts) else {
                        break 'valid;
                    };
                    if time < input.start || time >= input.end || seconds * 1000 - time > DAY {
                        break 'valid;
                    }
                    if purchase {
                        let Some(value) = parse_price(price) else {
                            break 'valid;
                        };
                        let category = *input.products.get(product).unwrap_or(&input.unknown);
                        pending.price = value | (category << 24);
                    }
                    let (Some(uid), Some(name)) = (user_key(user), pack_ref(user)) else {
                        declined.store(true, Relaxed);
                        break 'valid;
                    };
                    users.prefetch(uid);
                    pending.ts = (time - input.start) as u32;
                    pending.uid = uid;
                    pending.name = name;
                }
                batch[len] = pending;
                len += 1;
                if len == batch.len() {
                    flush(&batch);
                    len = 0;
                }
            });
        }
        flush(&batch[..len]);
        parts
    })
}

// The upper bits filter unrelated cells. Remaining bits hold the position
// of the winner in an immutable dedup stream.
const LOCATION_BITS: u32 = 26;
const LOCATION_MASK: u32 = (1 << LOCATION_BITS) - 1;

#[derive(Default)]
struct Dedup {
    winners: Vec<u32>,
}

impl Dedup {
    fn shard(&mut self, folds: &[EventParts], shard: usize) -> Option<()> {
        let mut count = 0;
        let mut longest = 1;
        for fold in folds {
            let rows = fold.copies.len(shard);
            count += rows;
            longest = longest.max(rows);
        }
        if count == 0 {
            return Some(());
        }
        let stride = longest.checked_next_power_of_two()?;
        if folds.len() > LOCATION_MASK as usize / stride {
            return None;
        }
        let shift = stride.trailing_zeros();
        let mask = (count * 2).next_power_of_two() - 1;
        self.winners.clear();
        self.winners.resize(mask + 1, 0);

        for (file, fold) in folds.iter().enumerate() {
            for (index, row) in fold.copies.records(shard).enumerate() {
                let hash = event_hash(row.key);
                let tag = ((hash >> 23) as u32) & !LOCATION_MASK;
                let location = ((file << shift) + index + 1) as u32;
                let mut slot = hash as usize & mask;
                loop {
                    let winner = self.winners[slot];
                    if winner == 0 {
                        self.winners[slot] = tag | location;
                        break;
                    }
                    if winner & !LOCATION_MASK == tag {
                        let previous = ((winner & LOCATION_MASK) - 1) as usize;
                        let prior_fold = &folds[previous >> shift];
                        let prior = prior_fold.copies.get(shard, previous & (stride - 1));
                        if same_event(prior.key, row.key) {
                            // All copies participate, including those without a payload.
                            if row.ingest < prior.ingest {
                                prior.tombstone(&prior_fold.people);
                                self.winners[slot] = tag | location;
                            } else {
                                row.tombstone(&fold.people);
                            }
                            break;
                        }
                    }
                    slot = (slot + 1) & mask;
                }
            }
        }
        Some(())
    }
}

#[inline]
fn same_event(a: u64, b: u64) -> bool {
    a == b || ((a | b) & SHORT_KEY == 0 && unpack_ref(a) == unpack_ref(b))
}

pub(super) fn run_events(
    work: &LazyChunks<'_>,
    read_ahead: &ReadAhead,
    users: &Users<'_>,
    input: &EventInput<'_>,
    linked_names: &[&'static [u8]],
    declined: &AtomicBool,
) -> Option<(u32, Vec<&'static [u8]>, Vec<People>)> {
    let folds = events(work, read_ahead, users, input, declined);
    let (local_count, names) = users.names(linked_names, declined);
    lap(&format!(
        "events parsed (nova, {} folds, {} persons)",
        folds.len(),
        names.len()
    ));
    if declined.load(Relaxed) {
        return None;
    }
    crate::pt_ordered(SHARDS, Dedup::default, |mut table, shard| {
        if table.shard(&folds, shard).is_none() {
            declined.store(true, Relaxed);
        }
        table
    });
    if declined.load(Relaxed) {
        return None;
    }
    let people = folds.into_iter().map(|fold| fold.people).collect();
    lap("events dedup + validate");
    Some((local_count, names, people))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn timestamp_cache_and_word_decoder_match_general_parser() {
        let mut cache = Dates::default();
        for value in [
            &b"2028-02-29T23:59:59Z"[..],
            b"2027-02-29T23:59:59Z",
            b"2028-02-29T24:00:00Z",
            b"2028-02-29T18:14:09-05:45",
            b"1791071890000",
            b"0000000000000",
            b"9999999999999",
            b"17910718900X0",
            b"17910/1890000",
            b"0001791071890000",
            b"179107189000000000000000",
        ] {
            assert_eq!(cache.parse(value), crate::parse_ts(value), "{value:?}");
        }
    }

    #[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
    #[test]
    fn fused_row_scanner_matches_scalar_quote_validation() {
        let fields = [
            "eabcdefg",
            "u01234567",
            "2026-10-01T00:00:00Z",
            "2026-10-01T00:00:00Z",
            "purchase",
            "p100",
            "",
        ];
        let mut input = Vec::new();
        for wraps in 0..128 {
            let start = input.len();
            for (i, field) in fields.iter().enumerate() {
                if i != 0 {
                    input.push(b',');
                }
                if wraps & (1 << i) != 0 {
                    input.push(b'"');
                }
                input.extend_from_slice(field.as_bytes());
                if wraps & (1 << i) != 0 {
                    input.push(b'"');
                }
            }
            input.push(b'\n');
            let mut corrupted = input[start..].to_vec();
            corrupted[23] = b'"';
            input.extend(corrupted);
            input.extend_from_slice(b"too,short,\"\"\n\n");
        }
        input.extend_from_slice(b"e9,u9,1,2026-10-01T00:00:00Z,view,p9,");
        let mut expected = Vec::new();
        crate::for_lines(&input, |line| {
            if let Some(values) = crate::fields::<7>(line) {
                expected.push((line, values));
            }
        });
        let mut actual = Vec::new();
        scan_event_rows::<true>(&input, |line, values| actual.push((line, values)));
        assert_eq!(actual, expected);
    }
}
