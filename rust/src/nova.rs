//! Nova's event pipeline. The scan builds one cache-friendly stream family by event ID.

use crate::scatter::{EV_STAGE, EvRec, PART_BITS, PARTS, Scatter, part_of};
use crate::{DAY, EvP, LazyChunks, P, PB, People, ReadAhead, event_rows, lap, parse_price};
use ahash::AHashMap;
use rayon::prelude::*;
use std::mem::size_of;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, AtomicUsize, Ordering::Relaxed};

const SHARDS: usize = 2048;
const SHARD_BITS: u32 = SHARDS.trailing_zeros();

/// The shard is the top `SHARD_BITS` mixed bits below the short-ID tag bit.
/// A long ID's claim and its carrier are pushed to the same stream.
#[inline]
fn shard_of(key: u64) -> usize {
    (key >> (63 - SHARD_BITS)) as usize & (SHARDS - 1)
}
const BLOCK_BYTES: usize = 4096;
const SLAB_BYTES: usize = 16 << 20;
const SHORT_KEY: u64 = 1 << 63;
const INVALID: u32 = u32::MAX;
/// Seed for the 63-bit hash key of a long event ID. The full bytes are carried
/// alongside it and compared by dedup, so a collision cannot merge events.
const EVENT_ID_SEED: u64 = 0x1319_8a2e_0370_7344;
/// Seed for the key of a long user ID (one that has no packed short key).
const USER_ID_SEED: u64 = 0x082e_fa98_ec4e_6c89;

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

/// Key and check of any user ID, with no shared state. Short IDs use the
/// reversible packed key. A long ID uses a 63-bit hash (top bit clear, so it
/// never meets a packed key) and an independent 32-bit check. Equal keys with
/// different checks, or different bytes in the link table, decline the fast
/// path, so a hash collision can never merge two people.
#[inline]
pub(super) fn user_ident(uid: &[u8]) -> (u64, u32) {
    match user_key(uid) {
        Some(key) => (key, 0),
        None => {
            let (key, check) = crate::hs(uid, USER_ID_SEED);
            // Zero marks an empty table slot, so a (vanishingly rare) zero hash becomes 1:
            // every partition and table then sees the same key.
            ((key & (u64::MAX >> 1)).max(1), check)
        }
    }
}

#[derive(Default)]
struct PersonName {
    bytes: [u8; 9],
    len: u8,
}

impl PersonName {
    fn decode(key: u64) -> Self {
        if key == 0 {
            return Self::default();
        }
        const INVERSE: u64 = 0x71de_83e1_9937_733d;
        let value = key & (SHORT_KEY - 1);
        let mixed = value ^ (value >> 31) ^ (value >> 62);
        let shifted = mixed.wrapping_mul(INVERSE) & (SHORT_KEY - 1);
        let raw = shifted ^ (shifted >> 27) ^ (shifted >> 54);
        let mut bytes = [0; 9];
        bytes[0] = b'u' ^ (((raw >> 55) & 1) as u8 * 32);
        bytes[1..].copy_from_slice(&(raw & 0x7f7f_7f7f_7f7f_7f7f).to_le_bytes());
        let tail = if raw & (1 << 31) != 0 {
            8
        } else {
            ((raw >> 7) & 1) | ((raw >> 14) & 2) | ((raw >> 21) & 4)
        };
        Self {
            bytes,
            len: tail as u8 + 1,
        }
    }
}


/// A producer owns disjoint blocks within demand-paged slabs. Stream order is
/// described by its block IDs. No initialized record moves when a stream grows.
pub(super) struct Streams<T, const N: usize> {
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

    pub(super) fn new() -> Self {
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
    pub(super) fn push(&mut self, stream: usize, row: T) {
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
        // Dedup indices refer only to initialized rows returned by `chunks`.
        unsafe {
            &*slab
                .as_ptr()
                .add(offset)
                .cast::<T>()
                .add(index % Self::ROWS)
        }
    }

    pub(super) fn chunks(&self, stream: usize) -> impl Iterator<Item = &[T]> {
        let block_ids = &self.blocks[stream];
        block_ids.iter().enumerate().map(move |(i, &id)| {
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

    pub(super) fn len(&self, stream: usize) -> usize {
        let blocks = self.blocks[stream].len();
        if blocks == 0 {
            0
        } else {
            (blocks - 1) * Self::ROWS + self.used[stream]
        }
    }
}

/// A parsed copy's identity and the slot its payload occupies in the final
/// person partition. Parse writes the payload once; dedup only tombstones.
#[repr(C)]
#[derive(Clone, Copy)]
struct Claim {
    key: u64,
    ingest: i32,
    loc: u32,
}
const _: () = assert!(size_of::<Claim>() == 16);

/// A long event ID's claim in one record. `eref` packs the pointer and length
/// of the full ID bytes that produced the hash `key`; dedup compares them
/// whenever two keys are equal, so a hash collision cannot merge events.
#[repr(C)]
#[derive(Clone, Copy)]
struct LongClaim {
    key: u64,
    eref: u64,
    ingest: i32,
    loc: u32,
}
const _: () = assert!(size_of::<LongClaim>() == 24);

/// One worker's claims. Long IDs live in their own streams, so short claims
/// stay 16 bytes and no record has to be paired with its neighbor.
struct Folds {
    short: Streams<Claim, SHARDS>,
    long: Streams<LongClaim, SHARDS>,
}

impl Folds {
    #[cfg(test)]
    fn new() -> Self {
        Self { short: Streams::new(), long: Streams::new() }
    }
}

// A copy's location tells dedup which payload to tombstone. Bit 31 picks its home:
//  - clear: a row already in its final person partition, written by the worker
//    that resolved it: worker (4 bits), person partition (PB bits), index.
//  - set: a row in the move list of its uid partition (its person lives in
//    another partition): uid partition (PART_BITS bits), index. Stage 5
//    re-scatters only the rows that survive dedup.
const LOC_INDEX_BITS: u32 = 27 - PB;
const LOC_PART_SHIFT: u32 = LOC_INDEX_BITS;
const LOC_WORKER_SHIFT: u32 = LOC_PART_SHIFT + PB;
const INDEX_MASK: u32 = (1 << LOC_INDEX_BITS) - 1;
const MAX_WORKERS: u32 = 1 << (31 - LOC_WORKER_SHIFT);
const MOVED: u32 = 1 << 31;
const MOVE_INDEX_BITS: u32 = 31 - PART_BITS;
const MOVE_INDEX_MASK: u32 = (1 << MOVE_INDEX_BITS) - 1;
const NO_LOC: u32 = u32::MAX;
const _: () = assert!(PARTS >= P);

trait Fields<'a> {
    fn field<const I: usize>(&self) -> &'a [u8];

    #[inline]
    fn empty<const I: usize>(&self) -> bool {
        self.field::<I>().is_empty()
    }
}

impl<'a> Fields<'a> for [&'a [u8]; 7] {
    #[inline]
    fn field<const I: usize>(&self) -> &'a [u8] {
        self[I]
    }
}

#[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
struct RowLayout<'a> {
    line: &'a [u8],
    cuts: [u8; 8],
    wrapped: u8,
}

#[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
impl<'a> Fields<'a> for RowLayout<'a> {
    #[inline]
    fn field<const I: usize>(&self) -> &'a [u8] {
        let start = self.cuts[I] as usize;
        let end = self.cuts[I + 1] as usize - 1;
        let trim = ((self.wrapped >> I) & 1) as usize;
        // The scanner produced distinct checked cuts and exact endpoint flags.
        unsafe { self.line.get_unchecked(start + trim..end - trim) }
    }

    #[inline]
    fn empty<const I: usize>(&self) -> bool {
        let raw = self.cuts[I + 1] - self.cuts[I] - 1;
        raw == 2 * ((self.wrapped >> I) & 1)
    }
}

trait EventSink<'a> {
    fn row(&mut self, source: &'a [u8], fields: impl Fields<'a>);
}

// The common event row spans three AVX2 vectors. Read each vector once for
// newlines, commas, and quote syntax. Every load stays inside the mapped chunk.
#[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
#[target_feature(enable = "avx2,bmi2")]
unsafe fn fused_event_rows<'a>(mut bytes: &'a [u8], sink: &mut impl EventSink<'a>) {
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
            if let Some(fields) = crate::row_fields::<7>(&bytes[..n]) {
                sink.row(&bytes[..n], fields);
            }
            bytes = &bytes[n + 1..];
            continue;
        }
        let c = unsafe { _mm256_loadu_si256(bytes.as_ptr().add(64).cast()) };
        let newline = mask(c, b'\n' as i8);
        if newline == 0 {
            let n = memchr::memchr(b'\n', &bytes[96..]).map_or(bytes.len(), |n| n + 96);
            if let Some(fields) = crate::row_fields::<7>(&bytes[..n]) {
                sink.row(&bytes[..n], fields);
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
            let quote_flags = if quotes == 0 {
                Some(0)
            } else if commas & 1 != 0 {
                // The first field ends before the start mask when it is empty.
                if let Some(fields) = crate::row_fields::<7>(line) {
                    sink.row(line, fields);
                }
                None
            } else {
                let opens = (commas << 1) | 1;
                let closes = (commas | (1u128 << line.len())) >> 1;
                let extract = |positions: u128| {
                    let lo = positions as u64;
                    let hi = (positions >> 64) as u64;
                    _pext_u64(quotes as u64, lo)
                        | (_pext_u64((quotes >> 64) as u64, hi) << lo.count_ones())
                };
                let wrapped = extract(opens);
                // Matching field flags enforce pairs. Counting both endpoints
                // rejects a lone quote in a one-byte field.
                (quotes & !(opens | closes) == 0
                    && wrapped == extract(closes)
                    && quotes.count_ones() == wrapped.count_ones() * 2)
                    .then_some(wrapped)
            };
            if let Some(wrapped) = quote_flags {
                let mut cuts = [0u8; 8];
                let mut remaining = commas;
                for cut in &mut cuts[1..7] {
                    *cut = remaining.trailing_zeros() as u8 + 1;
                    remaining &= remaining - 1;
                }
                cuts[7] = line.len() as u8 + 1;
                sink.row(
                    line,
                    RowLayout {
                        line,
                        cuts,
                        wrapped: wrapped as u8,
                    },
                );
            }
        }
        bytes = &bytes[line.len() + 1..];
    }
    event_rows(bytes, |line, fields| sink.row(line, fields));
}

#[inline]
fn scan_event_rows<'a>(bytes: &'a [u8], sink: &mut impl EventSink<'a>) {
    #[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
    if std::is_x86_feature_detected!("bmi2") {
        // AVX2 is required by this compilation and BMI2 was checked above.
        return unsafe { fused_event_rows(bytes, sink) };
    }
    event_rows(bytes, |line, fields| sink.row(line, fields));
}

/// Click rows usually fit in two vectors. Decode their separators and quote
/// endpoints together, retaining the generic scanner for short/long rows and tails.
#[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
#[target_feature(enable = "avx2,bmi2")]
unsafe fn fused_click_rows<'a>(mut bytes: &'a [u8], row: &mut impl FnMut(&'a [u8], [&'a [u8]; 4])) {
    use std::arch::x86_64::*;
    let mask = |value, byte| _mm256_movemask_epi8(_mm256_cmpeq_epi8(value, _mm256_set1_epi8(byte))) as u32;
    while bytes.len() >= 64 {
        let a = unsafe { _mm256_loadu_si256(bytes.as_ptr().cast()) };
        let b = unsafe { _mm256_loadu_si256(bytes.as_ptr().add(32).cast()) };
        let newlines = u64::from(mask(a, b'\n' as i8)) | (u64::from(mask(b, b'\n' as i8)) << 32);
        if newlines == 0 {
            let n = memchr::memchr(b'\n', &bytes[64..]).map_or(bytes.len(), |n| n + 64);
            if let Some(fields) = crate::row_fields::<4>(&bytes[..n]) { row(&bytes[..n], fields); }
            bytes = &bytes[(n + 1).min(bytes.len())..];
            continue;
        }
        let n = newlines.trailing_zeros() as usize;
        let line = &bytes[..n];
        let clip = (1u64 << n) - 1;
        let join = |byte| (u64::from(mask(a, byte)) | (u64::from(mask(b, byte)) << 32)) & clip;
        let commas = join(b',' as i8);
        if commas.count_ones() == 3 {
            let quotes = join(b'"' as i8);
            if commas & 1 != 0 {
                if let Some(fields) = crate::row_fields::<4>(line) { row(line, fields); }
            } else {
                let opens = (commas << 1) | 1;
                let closes = (commas | (1u64 << n)) >> 1;
                let wrapped = _pext_u64(quotes, opens);
                if quotes & !(opens | closes) == 0 && wrapped == _pext_u64(quotes, closes)
                    && quotes.count_ones() == wrapped.count_ones() * 2 {
                    let mut cuts = [0usize; 5];
                    let mut remaining = commas;
                    for cut in &mut cuts[1..4] {
                        *cut = remaining.trailing_zeros() as usize + 1;
                        remaining &= remaining - 1;
                    }
                    cuts[4] = n + 1;
                    row(line, std::array::from_fn(|i| {
                        let trim = ((wrapped >> i) & 1) as usize;
                        // Matched endpoints and quote count ensure each trim is in bounds.
                        unsafe { line.get_unchecked(cuts[i] + trim..cuts[i + 1] - 1 - trim) }
                    }));
                }
            }
        }
        bytes = &bytes[n + 1..];
    }
    crate::mask_rows::<4>(bytes, row);
}

#[inline]
pub(super) fn click_rows<'a>(bytes: &'a [u8], mut row: impl FnMut(&'a [u8], [&'a [u8]; 4])) {
    #[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
    if std::is_x86_feature_detected!("bmi2") {
        return unsafe { fused_click_rows(bytes, &mut row) };
    }
    crate::mask_rows::<4>(bytes, &mut row);
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

/// Decode six ASCII digits and two colons with a single eight-byte load.
#[inline]
fn clock_seconds(s: &[u8]) -> Option<i64> {
    const SEPARATORS: u64 = 0x0000_ff00_00ff_0000;
    const COLONS: u64 = 0x0000_3a00_003a_0000;
    const ZEROS: u64 = 0x0000_3000_0030_0000;
    let word = crate::rd8(s, 11);
    if word & SEPARATORS != COLONS || !swar_digits((word & !SEPARATORS) | ZEROS) {
        return None;
    }
    let digits = word & 0x0f0f_0f0f_0f0f_0f0f;
    let pairs = digits.wrapping_mul(10).wrapping_add(digits >> 8);
    let h = pairs as u8;
    let m = (pairs >> 24) as u8;
    let seconds = (pairs >> 48) as u8;
    (h <= 23 && m <= 59 && seconds <= 59)
        .then_some(i64::from(h) * 3600 + i64::from(m) * 60 + i64::from(seconds))
}

#[derive(Default)]
pub(super) struct Dates([([u8; 10], i64); 32]);

impl Dates {
    #[inline]
    fn with_offset(&mut self, s: &[u8], adjustment: i64) -> Option<i64> {
        if s[4] != b'-' || s[10] != b'T' {
            return None;
        }
        let time = clock_seconds(s)?;
        let date: [u8; 10] = s[..10].try_into().unwrap();
        let index = (date[8] as usize * 10 + date[9] as usize) & 31;
        let entry = &mut self.0[index];
        if entry.0 == date {
            return Some(entry.1 + time - adjustment);
        }
        let seconds = crate::parse_ts(s)? / 1000;
        *entry = (date, seconds + adjustment - time);
        Some(seconds)
    }

    #[inline]
    fn utc_seconds(&mut self, s: &[u8]) -> Option<i64> {
        if s.len() != 20 || s[19] != b'Z' {
            return None;
        }
        self.with_offset(s, 0)
    }

    #[inline]
    pub(super) fn parse(&mut self, s: &[u8]) -> Option<i64> {
        if s.len() == 20 && s[19] == b'Z' {
            return self.utc_seconds(s).map(|t| t * 1000);
        }
        if s.len() == 25 && (s[19] == b'+' || s[19] == b'-') && s[22] == b':' {
            let hours = crate::d2(s, 20)?;
            let minutes = crate::d2(s, 23)?;
            if hours > 23 || minutes > 59 {
                return None;
            }
            let adjustment = (hours * 60 + minutes) * 60;
            let adjustment = if s[19] == b'+' {
                adjustment
            } else {
                -adjustment
            };
            return self.with_offset(s, adjustment).map(|t| t * 1000);
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

/// P1 parser: turns each event row into one `EvRec` scattered by hash(uid).
/// It reads no shared table and uses no atomics per row. Field splitting comes
/// from `scan_event_rows`, whose SIMD paths hand any row they do not fully
/// understand to the scalar reference parser, so malformed rows are dropped by
/// exactly the same rules as before.
struct RecordParser<'a, 'catalog> {
    input: &'a EventInput<'catalog>,
    declined: &'a AtomicBool,
    window_seconds: i64,
    window_ms: u64,
    dates: Dates,
    out: &'a mut Scatter<EvRec, EV_STAGE>,
}

impl RecordParser<'_, '_> {
    fn parse_chunk(&mut self, bytes: &[u8]) {
        scan_event_rows(bytes, self);
    }
}

impl<'a> EventSink<'a> for RecordParser<'_, '_> {
    #[inline]
    fn row(&mut self, _: &'a [u8], fields: impl Fields<'a>) {
        let id = fields.field::<0>();
        if id.is_empty() {
            return;
        }
        // Short event IDs use the reversible packed key. Long, suffixed IDs
        // use a 63-bit hash and carry a reference to their full bytes; dedup
        // verifies those bytes before treating two keys as the same event.
        let (key, eref) = match short_key(id, b'e') {
            Some(key) => (permute_short(key), 0),
            None => {
                let Some(eref) = crate::pack_ref(id) else {
                    self.declined.store(true, Relaxed);
                    return;
                };
                (crate::hs(id, EVENT_ID_SEED).0 & (u64::MAX >> 1), eref)
            }
        };
        let Some(seconds) = self.dates.utc_seconds(fields.field::<3>()) else {
            self.declined.store(true, Relaxed);
            return;
        };
        let Ok(order) = i32::try_from(seconds - self.window_seconds) else {
            self.declined.store(true, Relaxed);
            return;
        };
        let mut rec = EvRec { ekey: key, eref, ukey: 0, name: 0, ingest: order, ts: INVALID, price: 0, uchk: 0 };
        // A claim without a payload has no user: spread it by event key.
        let mut part = part_of(key);
        'valid: {
            let user = fields.field::<1>();
            if user.is_empty() || fields.empty::<5>() {
                break 'valid;
            }
            let purchase = match fields.field::<4>() {
                b"purchase" => true,
                b"view" | b"cart" => false,
                _ => break 'valid,
            };
            let Some(time) = self.dates.parse(fields.field::<2>()) else {
                break 'valid;
            };
            let relative = time.wrapping_sub(self.input.start) as u64;
            if relative >= self.window_ms || seconds * 1000 - time > DAY {
                break 'valid;
            }
            let mut price = 0;
            if purchase {
                let Some(value) = parse_price(fields.field::<6>()) else {
                    break 'valid;
                };
                let category = *self
                    .input
                    .products
                    .get(fields.field::<5>())
                    .unwrap_or(&self.input.unknown);
                price = value | (category << 24);
            }
            let (ukey, uchk) = user_ident(user);
            // Keep a direct reference to the full ID for the name. This avoids
            // decoding short keys later (and an allocation per person).
            let Some(name) = crate::pack_ref(user) else {
                self.declined.store(true, Relaxed);
                break 'valid;
            };
            rec.name = name;
            rec.ukey = ukey;
            rec.uchk = uchk;
            rec.price = price;
            rec.ts = relative as u32;
            part = part_of(ukey);
        }
        self.out.push(part, rec);
    }
}

/// Rows per (producer, partition) arena slice for an estimated row count.
pub(super) fn rows_per_slice(estimated_rows: usize, producers: usize) -> usize {
    let rows = estimated_rows.div_ceil(PARTS * producers.max(1));
    rows + rows / 4
}

/// Stage 3 (P1): parse events into per-worker scatters by hash(uid).
fn scatter_events(
    work: &LazyChunks<'_>,
    read_ahead: &ReadAhead,
    input: &EventInput<'_>,
    declined: &AtomicBool,
    estimated_rows: usize,
) -> Vec<Scatter<EvRec, EV_STAGE>> {
    let next = AtomicUsize::new(0);
    let rows = rows_per_slice(estimated_rows, rayon::current_num_threads());
    rayon::broadcast(|_| {
        let mut out = Scatter::new(rows);
        let mut parser = RecordParser {
            input,
            declined,
            window_seconds: input.start.div_euclid(1000),
            window_ms: (input.end - input.start) as u64,
            dates: Dates::default(),
            out: &mut out,
        };
        loop {
            let index = next.fetch_add(1, Relaxed);
            if index >= work.len() {
                break;
            }
            read_ahead.wait(work.raw(index));
            parser.parse_chunk(work.get(index));
        }
        out.finish();
        out
    })
}

/// Resolves a click's user to a person: (uid partition, key, check).
pub(super) type Resolve<'a> = dyn Fn(usize, u64, u32) -> Option<u32> + Sync + 'a;

/// Stage 2: the all-uid link map split by hash(uid) into the uid partitions
/// that events and clicks use. Each entry is a (key, value) pair of the link
/// table, where the value holds the check and the person.
pub(super) struct LinkShards {
    start: Vec<usize>,
    entries: Vec<(u64, u64)>,
}

impl LinkShards {
    pub(super) fn build(table: &[(u64, u64)]) -> Self {
        const CHUNK: usize = 1 << 16;
        let counts: Vec<Vec<u32>> = table
            .par_chunks(CHUNK)
            .map(|chunk| {
                let mut n = vec![0u32; PARTS];
                for &(key, _) in chunk {
                    if key != 0 {
                        n[part_of(key)] += 1;
                    }
                }
                n
            })
            .collect();
        let mut start = vec![0usize; PARTS + 1];
        for n in &counts {
            for part in 0..PARTS {
                start[part + 1] += n[part] as usize;
            }
        }
        for part in 0..PARTS {
            start[part + 1] += start[part];
        }
        // Each chunk writes its entries after those of earlier chunks, so
        // every write goes to a distinct slot of a distinct (chunk, part) range.
        let mut cursors = Vec::with_capacity(counts.len());
        let mut running = start[..PARTS].to_vec();
        for n in &counts {
            cursors.push(running.clone());
            for part in 0..PARTS {
                running[part] += n[part] as usize;
            }
        }
        let mut entries = vec![(0u64, 0u64); start[PARTS]];
        let base = entries.as_mut_ptr() as usize;
        table.par_chunks(CHUNK).zip(cursors).for_each(|(chunk, mut cursor)| {
            for &(key, value) in chunk {
                if key != 0 {
                    let part = part_of(key);
                    // The ranges computed above are disjoint and in bounds.
                    unsafe { (base as *mut (u64, u64)).add(cursor[part]).write((key, value)) };
                    cursor[part] += 1;
                }
            }
        });
        Self { start, entries }
    }

    fn shard(&self, part: usize) -> &[(u64, u64)] {
        &self.entries[self.start[part]..self.start[part + 1]]
    }
}

/// How many dense ids a partition takes from its person partition at a time.
const ID_BLOCK: u32 = 16;

/// Hands out ids of unlinked people. Id = first + k * P + q puts the person in
/// person partition q with local index first / P + k. Two uid partitions share
/// each person partition, so they draw their k from one counter, in blocks.
struct IdBlock<'a> {
    next: &'a AtomicU32,
    first: u64,
    q: u64,
    cur: u32,
    end: u32,
}

impl IdBlock<'_> {
    #[inline]
    fn fresh(&mut self, declined: &AtomicBool) -> u32 {
        if self.cur == self.end {
            self.cur = self.next.fetch_add(ID_BLOCK, Relaxed);
            self.end = self.cur.wrapping_add(ID_BLOCK);
        }
        let id = self.first + u64::from(self.cur) * P as u64 + self.q;
        self.cur = self.cur.wrapping_add(1);
        if id >= crate::PERSON_SET {
            declined.store(true, Relaxed);
            return 0;
        }
        id as u32
    }
}

/// One uid partition's map from user key to person: the partition's link
/// shard, plus every unlinked user seen on a valid event. Only this
/// partition's worker writes it, so no atomics are needed. A user with no
/// link is its own person.
struct PartTab {
    slots: Vec<(u64, u64)>,
    mask: usize,
    used: usize,
    /// Unlinked people created here: (person id, packed reference to the uid bytes).
    fresh: Vec<(u32, u64)>,
}

impl PartTab {
    fn new(shard: &[(u64, u64)], extra: usize) -> Self {
        let cap = ((shard.len() + extra) * 2).next_power_of_two().max(16);
        let mut tab = Self { slots: vec![(0, 0); cap], mask: cap - 1, used: 0, fresh: Vec::new() };
        for &(key, value) in shard {
            tab.place(key, value);
        }
        tab
    }

    #[inline]
    fn place(&mut self, key: u64, value: u64) {
        let mut i = key as usize & self.mask;
        while self.slots[i].0 != 0 {
            i = (i + 1) & self.mask;
        }
        self.slots[i] = (key, value);
        self.used += 1;
    }

    fn grow(&mut self) {
        let old = std::mem::replace(&mut self.slots, vec![(0, 0); (self.mask + 1) * 2]);
        self.mask = self.slots.len() - 1;
        self.used = 0;
        for (key, value) in old {
            if key != 0 {
                self.place(key, value);
            }
        }
    }

    /// The person of a user that is linked or has a valid event.
    #[inline]
    fn find(&self, key: u64, check: u32, declined: &AtomicBool) -> Option<u32> {
        let key = key.max(1);
        let mut i = key as usize & self.mask;
        loop {
            let (k, value) = self.slots[i];
            if k == 0 {
                return None;
            }
            if k == key {
                if (value >> 32) as u32 != check {
                    declined.store(true, Relaxed);
                    return None;
                }
                return Some(value as u32 & (crate::PERSON_SET as u32 - 1));
            }
            i = (i + 1) & self.mask;
        }
    }

    /// The person of a user on a valid event copy, creating an unlinked one.
    #[inline]
    fn person(&mut self, key: u64, check: u32, name: u64, ids: &mut IdBlock<'_>, declined: &AtomicBool) -> u32 {
        if self.used * 2 > self.slots.len() {
            self.grow();
        }
        let key = key.max(1);
        let mut i = key as usize & self.mask;
        loop {
            let (k, value) = self.slots[i];
            if k == 0 {
                let id = ids.fresh(declined);
                self.slots[i] = (key, (u64::from(check) << 32) | crate::PERSON_SET | u64::from(id));
                self.used += 1;
                self.fresh.push((id, name));
                return id;
            }
            if k == key {
                if (value >> 32) as u32 != check {
                    declined.store(true, Relaxed);
                }
                return value as u32 & (crate::PERSON_SET as u32 - 1);
            }
            i = (i + 1) & self.mask;
        }
    }
}

/// Output of stage 4: dedup claims by event key, the rows already in their
/// person partition, the move lists and each uid partition's user map.
struct Resolved {
    claims: Vec<Folds>,
    people: Vec<People>,
    moves: Vec<OnceLock<Vec<EvP>>>,
    tabs: Vec<OnceLock<PartTab>>,
    /// Next unused local index of each person partition's unlinked people.
    next_local: Vec<AtomicU32>,
}

/// Stage 4a and 4c, per uid partition and in cache: resolve each user to its
/// person from the partition's link shard (an unlinked user is its own
/// person), keep rows whose person partition is this partition's home in
/// place, and put the rest in the partition's move list. Every well-formed
/// event ID also leaves a claim for the dedup by event key.
fn resolve_events(
    scatters: &[Scatter<EvRec, EV_STAGE>],
    shards: &LinkShards,
    first_id: usize,
    declined: &AtomicBool,
    estimated_rows: usize,
) -> Resolved {
    let next = AtomicUsize::new(0);
    let workers = AtomicUsize::new(0);
    let rows_per_partition = estimated_rows.div_ceil(P * rayon::current_num_threads());
    let reservation = (rows_per_partition + rows_per_partition / 4).clamp(16, INDEX_MASK as usize + 1);
    let moves: Vec<OnceLock<Vec<EvP>>> = (0..PARTS).map(|_| OnceLock::new()).collect();
    let tabs: Vec<OnceLock<PartTab>> = (0..PARTS).map(|_| OnceLock::new()).collect();
    let next_local: Vec<AtomicU32> = (0..P).map(|_| AtomicU32::new(0)).collect();
    let mut parsed: Vec<_> = rayon::broadcast(|_| {
        let worker = workers.fetch_add(1, Relaxed) as u32;
        let too_many = worker >= MAX_WORKERS;
        if too_many {
            declined.store(true, Relaxed);
        }
        let mut people = People::new(if too_many { 0 } else { reservation });
        let mut short = Streams::<Claim, SHARDS>::new();
        let mut long = Streams::<LongClaim, SHARDS>::new();
        loop {
            let part = next.fetch_add(1, Relaxed);
            if part >= PARTS {
                break;
            }
            let home = part & (P - 1);
            let total: usize = scatters.iter().map(|s| s.records(part).iter().map(|r| r.len()).sum::<usize>()).sum();
            let mut tab = PartTab::new(shards.shard(part), total / 16);
            let mut ids = IdBlock { next: &next_local[home], first: first_id as u64, q: home as u64, cur: 0, end: 0 };
            let mut moved: Vec<EvP> = Vec::new();
            for scatter in scatters {
                for rows in scatter.records(part) {
                    for rec in rows {
                        let loc = if rec.ts != INVALID {
                            let person = tab.person(rec.ukey, rec.uchk, rec.name, &mut ids, declined);
                            let q = person as usize & (P - 1);
                            let row = EvP { person, ts: AtomicU32::new(rec.ts), price: rec.price };
                            if q == home {
                                let index = people.len(q);
                                if index >= INDEX_MASK as usize || too_many {
                                    declined.store(true, Relaxed);
                                    continue;
                                }
                                people.push(q, row);
                                (worker << LOC_WORKER_SHIFT) | ((q as u32) << LOC_PART_SHIFT) | index as u32
                            } else {
                                let index = moved.len();
                                if index >= MOVE_INDEX_MASK as usize {
                                    declined.store(true, Relaxed);
                                    continue;
                                }
                                moved.push(row);
                                MOVED | ((part as u32) << MOVE_INDEX_BITS) | index as u32
                            }
                        } else {
                            // Invalid copies still compete for the earliest
                            // ingest, but have no payload to keep or tombstone.
                            NO_LOC
                        };
                        let shard = shard_of(rec.ekey);
                        if rec.eref != 0 {
                            long.push(shard, LongClaim { key: rec.ekey, eref: rec.eref, ingest: rec.ingest, loc });
                        } else {
                            short.push(shard, Claim { key: rec.ekey, ingest: rec.ingest, loc });
                        }
                    }
                }
            }
            let _ = moves[part].set(moved);
            let _ = tabs[part].set(tab);
        }
        (worker, Folds { short, long }, people)
    });
    // `broadcast` returns results in completion order, but locations encode
    // the assigned worker id, so restore that order before indexing.
    parsed.sort_unstable_by_key(|(worker, _, _)| *worker);
    let mut claims = Vec::with_capacity(parsed.len());
    let mut people = Vec::with_capacity(parsed.len());
    for (_, c, p) in parsed {
        claims.push(c);
        people.push(p);
    }
    Resolved { claims, people, moves, tabs, next_local }
}

/// Names of all dense person ids: the linked people (by root, in order), then
/// the unlinked ones. Ids no one used are empty and have no events.
fn person_names(
    tabs: &[OnceLock<PartTab>],
    next_local: &[AtomicU32],
    linked: &[&'static [u8]],
) -> Option<(u32, Vec<&'static [u8]>)> {
    let first = linked.len().div_ceil(P) * P;
    let used = next_local.iter().map(|k| k.load(Relaxed) as usize).max().unwrap_or(0);
    let n = first + used * P;
    if n >= crate::PERSON_SET as usize {
        return None;
    }
    let refs_owner: Vec<u64> = vec![0; n - first];
    // Every id is written by exactly one partition (ids are unique); zero pages stay lazy.
    let refs = unsafe { std::slice::from_raw_parts(refs_owner.as_ptr().cast::<AtomicU64>(), n - first) };
    tabs.par_iter().for_each(|tab| {
        for &(id, name) in &tab.get().unwrap().fresh {
            refs[id as usize - first].store(name, Relaxed);
        }
    });
    let mut names = Vec::with_capacity(n);
    names.extend_from_slice(linked);
    names.resize(first, &[][..]);
    let fresh: Vec<&'static [u8]> = (0..n - first)
        .into_par_iter()
        .map(|i| match refs[i].load(Relaxed) {
            0 => &[][..],
            r => crate::unpack_ref(r),
        })
        .collect();
    names.extend(fresh);
    Some(((n / P) as u32, names))
}

/// Stage 5: re-scatter the move lists to their person partitions in one pass.
/// Only rows that survived dedup move. Worker `w` takes every `W`th uid
/// partition and appends to its own `People`, so nothing is shared.
fn rescatter(people: &mut [People], moves: &[OnceLock<Vec<EvP>>]) {
    let workers = people.len().max(1);
    people.par_iter_mut().enumerate().for_each(|(worker, people)| {
        for part in (worker..PARTS).step_by(workers) {
            for row in moves[part].get().map_or(&[][..], |list| list.as_slice()) {
                let ts = row.ts.load(Relaxed);
                if ts != INVALID {
                    let q = row.person as usize & (P - 1);
                    people.push(q, EvP { person: row.person, ts: AtomicU32::new(ts), price: row.price });
                }
            }
        }
    });
}

// One 16-byte cell holds the full event key, the winning ingest time and the
// location of the payload parse already wrote into its final person partition.
// The high byte of `meta` is a generation, so tables are reused across shards.
const META_GEN_SHIFT: u32 = 24;
const META_INGEST_MASK: u32 = (1 << META_GEN_SHIFT) - 1;
const INGEST_BIAS: i32 = 1 << 21;

#[repr(C)]
#[derive(Clone, Copy, Default)]
struct Slot {
    key: u64,
    meta: u32,
    loc: u32,
}

/// Where dedup finds a claim's payload to tombstone.
struct Placed<'a> {
    people: &'a [People],
    moves: &'a [OnceLock<Vec<EvP>>],
}

struct Router {
    slots: Vec<Slot>,
    // For a long event ID, the packed reference to the bytes that produced
    // `slots[i].key`. Zero for short IDs, whose keys are exact by themselves.
    refs: Vec<u64>,
    generation: u32,
    route_ns: u64,
    table_percent: usize,
}

impl Router {
    fn new() -> Self {
        Self {
            slots: Vec::new(),
            refs: Vec::new(),
            generation: 0,
            route_ns: 0,
            table_percent: std::env::var("BREAKER_TABLE_PERCENT").ok().and_then(|s| s.parse().ok()).unwrap_or(200).max(110),
        }
    }

    #[inline]
    fn tombstone(placed: &Placed<'_>, loc: u32) {
        if loc == NO_LOC {
            return;
        }
        // The payload is already in its slot; only its timestamp changes.
        if loc & MOVED != 0 {
            let part = ((loc & !MOVED) >> MOVE_INDEX_BITS) as usize;
            let index = (loc & MOVE_INDEX_MASK) as usize;
            placed.moves[part].get().unwrap()[index].ts.store(INVALID, Relaxed);
            return;
        }
        let worker = (loc >> LOC_WORKER_SHIFT) as usize;
        let part = ((loc >> LOC_PART_SHIFT) as usize) & (P - 1);
        let index = (loc & INDEX_MASK) as usize;
        placed.people[worker].get(part, index).ts.store(INVALID, Relaxed);
    }

    fn route(&mut self, people: &Placed<'_>, claims: &[Folds], shard: usize) {
        let begin = std::time::Instant::now();
        // Every record is one logical claim, so physical counts size the table.
        let count: usize = claims.iter().map(|fold| fold.short.len(shard) + fold.long.len(shard)).sum();
        if count == 0 {
            return;
        }
        let mask = (count * self.table_percent / 100).next_power_of_two() - 1;
        if self.slots.len() < mask + 1 {
            self.slots.resize(mask + 1, Slot::default());
            self.refs.resize(mask + 1, 0);
        }
        self.generation += 1;
        if self.generation == 1 << (32 - META_GEN_SHIFT) {
            self.slots.fill(Slot::default());
            self.refs.fill(0);
            self.generation = 1;
        }
        let generation = self.generation << META_GEN_SHIFT;

        // Parse has already placed every copy in its final person partition.
        // Dedup only chooses the earliest-ingest copy and tombstones losers,
        // so there is no scatter pass and no second traversal of event data.
        for fold in claims {
            for block in fold.short.chunks(shard) {
                for (offset, claim) in block.iter().enumerate() {
                    if let Some(next) = block.get(offset + 8) {
                        crate::prefetch_read(self.slots.as_ptr().wrapping_add(next.key as usize & mask).cast());
                    }
                    let meta = generation | ((claim.ingest + INGEST_BIAS) as u32 & META_INGEST_MASK);
                    let mut slot = claim.key as usize & mask;
                    loop {
                        let winner = self.slots[slot];
                        if winner.meta >> META_GEN_SHIFT != generation >> META_GEN_SHIFT {
                            self.slots[slot] = Slot { key: claim.key, meta, loc: claim.loc };
                            break;
                        }
                        // A long ID's key has the short tag bit clear, so it
                        // can never equal a short key; refs are read only
                        // for long slots, which always set them.
                        if winner.key == claim.key {
                            let previous = (winner.meta & META_INGEST_MASK) as i32 - INGEST_BIAS;
                            if claim.ingest < previous {
                                Self::tombstone(people, winner.loc);
                                self.slots[slot] = Slot { key: claim.key, meta, loc: claim.loc };
                            } else {
                                Self::tombstone(people, claim.loc);
                            }
                            break;
                        }
                        slot = (slot + 1) & mask;
                    }
                }
            }
        }
        for fold in claims {
            for block in fold.long.chunks(shard) {
                for (offset, claim) in block.iter().enumerate() {
                    if let Some(next) = block.get(offset + 8) {
                        crate::prefetch_read(self.slots.as_ptr().wrapping_add(next.key as usize & mask).cast());
                    }
                    let meta = generation | ((claim.ingest + INGEST_BIAS) as u32 & META_INGEST_MASK);
                    let mut slot = claim.key as usize & mask;
                    loop {
                        let winner = self.slots[slot];
                        if winner.meta >> META_GEN_SHIFT != generation >> META_GEN_SHIFT {
                            self.slots[slot] = Slot { key: claim.key, meta, loc: claim.loc };
                            self.refs[slot] = claim.eref;
                            break;
                        }
                        // Equal hashes identify one event only when the full
                        // byte strings agree. Different IDs with the same hash
                        // keep probing, just as table collisions always have.
                        if winner.key == claim.key
                            && (self.refs[slot] == claim.eref
                                || crate::unpack_ref(self.refs[slot]) == crate::unpack_ref(claim.eref))
                        {
                            let previous = (winner.meta & META_INGEST_MASK) as i32 - INGEST_BIAS;
                            if claim.ingest < previous {
                                Self::tombstone(people, winner.loc);
                                self.slots[slot] = Slot { key: claim.key, meta, loc: claim.loc };
                                self.refs[slot] = claim.eref;
                            } else {
                                Self::tombstone(people, claim.loc);
                            }
                            break;
                        }
                        slot = (slot + 1) & mask;
                    }
                }
            }
        }
        self.route_ns += begin.elapsed().as_nanos() as u64;
    }
}

pub(super) fn run_events<C: Send>(
    work: &LazyChunks<'_>,
    read_ahead: &ReadAhead,
    shards: &LinkShards,
    input: &EventInput<'_>,
    linked_names: &[&'static [u8]],
    declined: &AtomicBool,
    estimated_rows: usize,
    parse_clicks: impl FnOnce(&Resolve<'_>) -> C + Send,
) -> Option<(u32, Vec<&'static [u8]>, Vec<People>, C)> {
    let scatters = scatter_events(work, read_ahead, input, declined, estimated_rows);
    lap("  events scattered");
    let mut resolved = resolve_events(&scatters, shards, linked_names.len().div_ceil(P) * P, declined, estimated_rows);
    lap("  events resolved");
    // The process exits right after the run; skip unmapping the arenas.
    std::mem::forget(scatters);
    if declined.load(Relaxed) {
        return None;
    }
    let (local_count, names) = person_names(&resolved.tabs, &resolved.next_local, linked_names)?;
    lap(&format!(
        "events parsed (nova, {} folds, {} persons)",
        resolved.claims.len(),
        names.len()
    ));
    if declined.load(Relaxed) {
        return None;
    }
    // Resolution finished building every partition's user map. Dedup only
    // touches event payloads, so click parsing can resolve users meanwhile.
    let resolve = |part: usize, key: u64, check: u32| {
        resolved.tabs[part].get().and_then(|tab| tab.find(key, check, declined))
    };
    let ((), clicks) = rayon::join(
        || {
            let placed = Placed { people: &resolved.people, moves: &resolved.moves };
            let routers = crate::pt((0..SHARDS).into_par_iter(), Router::new, |mut router, shard| {
                router.route(&placed, &resolved.claims, shard);
                router
            });
            if std::env::var_os("TIMING").is_some() {
                for router in routers {
                    eprintln!("ROUTER tombstone={:.1}", router.route_ns as f64 / 1e6);
                }
            }
            lap("  dedup tombstoned");
            lap("events dedup + validate");
        },
        || parse_clicks(&resolve),
    );
    if declined.load(Relaxed) {
        return None;
    }
    drop(resolved.claims);
    rescatter(&mut resolved.people, &resolved.moves);
    lap("  moves re-scattered");
    Some((local_count, names, resolved.people, clicks))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn leak(b: &[u8]) -> &'static [u8] {
        Box::leak(b.to_vec().into_boxed_slice())
    }

    fn event(ekey: u64, eref: &[u8], user: &[u8], ingest: i32, ts: u32, price: u32) -> EvRec {
        let (ukey, uchk) = user_ident(user);
        let eref = if eref.is_empty() { 0 } else { crate::pack_ref(leak(eref)).unwrap() };
        EvRec { ekey, eref, ukey, name: crate::pack_ref(leak(user)).unwrap(), ingest, ts, price, uchk }
    }

    type Live = Vec<(Vec<u8>, u32, u32)>;

    /// Run records through stage 4 (resolve), dedup and stage 5 (re-scatter)
    /// and return the live (person name, ts, price) rows, sorted. `links` are
    /// (user ID, linked person) pairs and `linked_names` the names of the
    /// linked people, as stage 2 would produce them.
    fn run_pipeline(
        recs: &[EvRec],
        force_part: Option<usize>,
        links: &[(&[u8], u32)],
        linked_names: &[&'static [u8]],
    ) -> (Live, bool) {
        let mut table = vec![(0u64, 0u64); 64];
        for &(uid, person) in links {
            let (key, check) = user_ident(uid);
            let mut i = key as usize & 63;
            while table[i].0 != 0 {
                i = (i + 1) & 63;
            }
            table[i] = (key, (u64::from(check) << 32) | crate::PERSON_SET | u64::from(person));
        }
        let shards = LinkShards::build(&table);
        let declined = AtomicBool::new(false);
        let mut scatter = Scatter::<EvRec, EV_STAGE>::new(4);
        for r in recs {
            scatter.push(force_part.unwrap_or_else(|| part_of(r.ukey)), *r);
        }
        scatter.finish();
        let first = linked_names.len().div_ceil(P) * P;
        let mut resolved = resolve_events(&[scatter], &shards, first, &declined, recs.len());
        let placed = Placed { people: &resolved.people, moves: &resolved.moves };
        for shard in 0..SHARDS {
            Router::new().route(&placed, &resolved.claims, shard);
        }
        rescatter(&mut resolved.people, &resolved.moves);
        let Some((_, names)) = person_names(&resolved.tabs, &resolved.next_local, linked_names) else {
            return (Vec::new(), true);
        };
        let mut live = Vec::new();
        for p in &resolved.people {
            for part in 0..P {
                for r in p.records(part).into_iter().flatten() {
                    assert_eq!(r.person as usize & (P - 1), part, "row in the wrong person partition");
                    if let Some(ts) = r.live() {
                        live.push((names[r.person as usize].to_vec(), ts, r.price));
                    }
                }
            }
        }
        live.sort();
        (live, declined.load(Relaxed))
    }

    fn run_stub(recs: &[EvRec], force_part: Option<usize>) -> (Live, bool) {
        run_pipeline(recs, force_part, &[], &[])
    }

    #[test]
    fn stub_keeps_forced_long_event_id_collisions_apart_and_dedups_equal_bytes() {
        // Two different long event IDs forced onto one key, plus a true duplicate of the first.
        let key = 0x0123_4567_89ab_cdefu64 & (u64::MAX >> 1);
        let u = b"u_0123456789abcdef0123456789abcdef";
        let recs = [
            event(key, b"e_alpha_long_identifier", u, 20, 100, 1),
            event(key, b"e_bravo_long_identifier", u, 20, 200, 2),
            event(key, b"e_alpha_long_identifier", u, 10, 100, 1),
        ];
        // The same rows scattered into one partition or by uid must agree.
        for force in [Some(17), None] {
            let (live, declined) = run_stub(&recs, force);
            assert!(!declined);
            assert_eq!(live, vec![(u.to_vec(), 100, 1), (u.to_vec(), 200, 2)]);
        }
    }

    #[test]
    fn stub_keeps_forced_short_id_collisions_apart() {
        // Different short users and event IDs forced into one scatter partition, and a
        // short user differing from another only in the case of its prefix.
        let recs = [
            event(permute_short(short_key(b"e1", b'e').unwrap()), b"", b"u1234567", 5, 10, 1),
            event(permute_short(short_key(b"E1", b'e').unwrap()), b"", b"U1234567", 5, 20, 2),
            event(permute_short(short_key(b"e2", b'e').unwrap()), b"", b"u1234567", 5, 30, 3),
            // Same event ID as the first with a later ingest: the first copy wins.
            event(permute_short(short_key(b"e1", b'e').unwrap()), b"", b"u1234567", 9, 40, 4),
        ];
        let (live, declined) = run_stub(&recs, Some(3));
        assert!(!declined);
        assert_eq!(live, vec![
            (b"U1234567".to_vec(), 20, 2),
            (b"u1234567".to_vec(), 10, 1),
            (b"u1234567".to_vec(), 30, 3),
        ]);
    }

    #[test]
    fn stub_declines_when_two_long_user_ids_share_a_key_with_different_checks() {
        let mut a = event(1, b"", b"u_0123456789abcdef0123456789abcdef", 5, 10, 1);
        let mut b = event(2, b"", b"u_0123456789abcdef0123456789abcdeg", 5, 10, 1);
        // Force one user key and different independent checks.
        a.ukey = 0x55;
        b.ukey = 0x55;
        a.uchk = 1;
        b.uchk = 2;
        let (_, declined) = run_stub(&[a, b], Some(0));
        assert!(declined, "a hash collision must decline the fast path, never merge people");
    }

    #[test]
    fn linked_users_share_a_person_and_moved_copies_dedup_across_partitions() {
        // Aliases a and b of one person live in different uid partitions, and the
        // person's partition is neither. Copies of one event ID under both aliases
        // (the later copy names another user) must leave exactly the earliest.
        let a: &[u8] = b"u0000001";
        let b: &[u8] = b"u0000002";
        let c: &[u8] = b"u0000003";
        let person = 5u32;
        let links = [(a, person), (b, person)];
        let linked_names: Vec<&'static [u8]> = vec![leak(a); 6];
        let e = |s: &[u8]| permute_short(short_key(s, b'e').unwrap());
        let (ka, kb) = (user_ident(a).0, user_ident(b).0);
        assert_ne!(part_of(ka) & (P - 1), person as usize & (P - 1));
        assert_ne!(part_of(kb) & (P - 1), person as usize & (P - 1));
        let recs = [
            event(e(b"e1"), b"", a, 10, 100, 1),
            // Same event ID, later ingest, other alias: loses to the copy above.
            event(e(b"e1"), b"", b, 20, 200, 2),
            // Earliest copy is invalid (no payload): the later valid copy is dropped.
            event(e(b"e2"), b"", b, 5, INVALID, 0),
            event(e(b"e2"), b"", a, 9, 300, 3),
            event(e(b"e3"), b"", b, 7, 400, 4),
            // An unlinked user stays its own person.
            event(e(b"e4"), b"", c, 7, 500, 5),
        ];
        let (live, declined) = run_pipeline(&recs, None, &links, &linked_names);
        assert!(!declined);
        assert_eq!(live, vec![
            (a.to_vec(), 100, 1),
            (a.to_vec(), 400, 4),
            (c.to_vec(), 500, 5),
        ]);
    }

    #[test]
    fn clicks_resolve_linked_users_without_events_in_their_partition() {
        let a: &[u8] = b"u0000001";
        let c: &[u8] = b"u0000003";
        let mut table = vec![(0u64, 0u64); 16];
        let (key, check) = user_ident(a);
        table[key as usize & 15] = (key, (u64::from(check) << 32) | crate::PERSON_SET | 3);
        let shards = LinkShards::build(&table);
        let part = part_of(key);
        let tab = PartTab::new(shards.shard(part), 0);
        let declined = AtomicBool::new(false);
        assert_eq!(tab.find(key, check, &declined), Some(3));
        let (ck, cc) = user_ident(c);
        assert_eq!(tab.find(ck, cc, &declined), None);
        assert!(!declined.load(Relaxed));
        // A different check for a known key is a hash collision: decline.
        assert_eq!(tab.find(key, check ^ 1, &declined), None);
        assert!(declined.load(Relaxed));
    }

    #[test]
    fn long_user_ids_hash_disjoint_from_short_keys() {
        let long = b"u_0123456789abcdef0123456789abcdef";
        let (key, check) = user_ident(long);
        assert_eq!(key & SHORT_KEY, 0);
        assert_eq!((key, check), user_ident(long));
        assert_ne!(user_ident(b"u_0123456789abcdef0123456789abcdeg"), (key, check));
        let (short, zero) = user_ident(b"u_12");
        assert_eq!((short & SHORT_KEY, zero), (SHORT_KEY, 0));
    }

    fn place(
        people: &mut [People],
        claims: &mut [Folds],
        worker: usize,
        key: u64,
        eref: u64,
        ingest: i32,
        person: u32,
        ts: u32,
        price: u32,
    ) {
        let loc = if ts == INVALID {
            NO_LOC
        } else {
            let part = person as usize & (P - 1);
            let index = people[worker].len(part);
            people[worker].push(part, EvP { person, ts: AtomicU32::new(ts), price });
            ((worker as u32) << LOC_WORKER_SHIFT) | ((part as u32) << LOC_PART_SHIFT) | index as u32
        };
        if eref == 0 {
            // Short keys carry SHORT_KEY in production; route only that shard here.
            claims[worker].short.push(0, Claim { key: key | SHORT_KEY, ingest, loc });
        } else {
            claims[worker].long.push(0, LongClaim { key, eref, ingest, loc });
        }
    }

    fn live_rows(people: &[People], part: usize) -> Vec<(u32, u32)> {
        people.iter().flat_map(|p| p.records(part).into_iter().flatten())
            .filter_map(|r| r.live().map(|ts| (ts, r.price))).collect()
    }

    fn route_shard0(people: &[People], claims: &[Folds]) {
        let moves: Vec<OnceLock<Vec<EvP>>> = Vec::new();
        Router::new().route(&Placed { people, moves: &moves }, claims, 0);
    }

    #[test]
    fn router_keeps_distinct_long_ids_with_equal_hash_keys() {
        let a: &'static [u8] = Box::leak(b"e_alpha_long_identifier".to_vec().into_boxed_slice());
        let b: &'static [u8] = Box::leak(b"e_bravo_long_identifier".to_vec().into_boxed_slice());
        // Deliberately force one key for two different full IDs. Without byte
        // verification the second event would be tombstoned as a duplicate.
        let key = 0x0123_4567_89ab_cdefu64 & (u64::MAX >> 1);
        let mut people = [People::new(32)];
        let mut claims = vec![Folds::new()];
        place(&mut people, &mut claims, 0, key, crate::pack_ref(a).unwrap(), 10, 3, 100, 1);
        place(&mut people, &mut claims, 0, key, crate::pack_ref(b).unwrap(), 20, 3, 200, 2);
        route_shard0(&people, &claims);
        assert_eq!(live_rows(&people, 3), vec![(100, 1), (200, 2)]);
    }

    #[test]
    fn router_dedups_equal_long_ids_by_bytes_not_pointers() {
        let a: &'static [u8] = Box::leak(b"e_same_long_identifier".to_vec().into_boxed_slice());
        let b: &'static [u8] = Box::leak(b"e_same_long_identifier".to_vec().into_boxed_slice());
        assert_ne!(a.as_ptr(), b.as_ptr());
        let key = crate::hs(a, EVENT_ID_SEED).0 & (u64::MAX >> 1);
        let mut people = [People::new(32)];
        let mut claims = vec![Folds::new()];
        place(&mut people, &mut claims, 0, key, crate::pack_ref(a).unwrap(), 30, 3, 300, 3);
        place(&mut people, &mut claims, 0, key, crate::pack_ref(b).unwrap(), 10, 3, 100, 1);
        place(&mut people, &mut claims, 0, key, crate::pack_ref(b).unwrap(), 20, 3, 200, 2);
        route_shard0(&people, &claims);
        assert_eq!(live_rows(&people, 3), vec![(100, 1)]);
    }

    #[test]
    fn router_keeps_earliest_invalid_and_changed_person_copies() {
        let mut people = [People::new(32), People::new(1)];
        let mut claims = vec![Folds::new(), Folds::new()];
        // Exercise overflow and a claim block boundary.
        for i in 0..300u64 {
            place(&mut people, &mut claims, 0, i + 1, 0, 20, 3, 10, 1);
        }
        place(&mut people, &mut claims, 1, 1, 0, 10, 7, INVALID, 0);
        place(&mut people, &mut claims, 1, 2, 0, 5, 7, 5, 2);
        place(&mut people, &mut claims, 1, 2, 0, 30, 3, 20, 3);
        place(&mut people, &mut claims, 1, 3, 0, 20, 3, 10, 1);
        assert_eq!(people[0].len(3), 300);
        assert_eq!(people[1].len(7), 1);
        route_shard0(&people, &claims);
        let live = |part| people.iter().flat_map(|p| p.records(part).into_iter().flatten())
            .filter_map(|r| r.live().map(|ts| (r.person, ts, r.price))).collect::<Vec<_>>();
        assert_eq!(live(3), vec![(3, 10, 1); 298]);
        assert_eq!(live(7), vec![(7, 5, 2)]);
    }

    #[test]
    fn click_scanner_matches_scalar_at_boundaries_and_quote_forms() {
        let mut input = Vec::new();
        for width in 0..140 {
            for wraps in 0..16 {
                let fields = ["c".repeat(width), "u01234567".into(), "c_123".into(), "2028-02-29T12:01:02Z".into()];
                let start = input.len();
                for (i, field) in fields.iter().enumerate() {
                    if i != 0 { input.push(b','); }
                    if wraps & (1 << i) != 0 { input.push(b'"'); }
                    input.extend_from_slice(field.as_bytes());
                    if wraps & (1 << i) != 0 { input.push(b'"'); }
                }
                input.push(b'\n');
                let mut corrupt = input[start..].to_vec();
                let at = width.min(corrupt.len() - 1);
                corrupt[at] = b'"';
                input.extend(corrupt);
            }
        }
        for text in ["\n", ",,,\n", "\",u,c,t\n", "c,\",c,t\n", "c,u,\",t\n", "c,u,c,\"\n", "c,u,c,\"\"\n", "c,,c,t\n", "c,u,c,\"t\"x\n", "c,u,c,t,extra\n", "c,u,c,t\r\n", "c,u,c,t"] {
            input.extend_from_slice(text.as_bytes());
        }
        let mut expected = Vec::new();
        crate::for_lines(&input, |line| {
            if let Some(fields) = crate::fields::<4>(line) { expected.push((line, fields)); }
        });
        let mut actual = Vec::new();
        click_rows(&input, |line, fields| actual.push((line, fields)));
        assert_eq!(actual, expected);
    }

    #[test]
    fn canonical_names_round_trip_byte_for_byte() {
        for name in [
            &b"u"[..],
            b"U",
            b"u0",
            b"U\0",
            b"u ab",
            b"u00AB ",
            b"U1234567",
            b"uABCDEFGH",
            b"U\0\0\0\0\0\0\0\0",
            b"u\x7f1234567",
        ] {
            let decoded = PersonName::decode(user_key(name).unwrap());
            assert_eq!(&decoded.bytes[..decoded.len as usize], name);
        }
    }

    #[test]
    fn timestamp_cache_and_word_decoder_match_general_parser() {
        let mut cache = Dates::default();
        for value in [
            &b"2028-02-29T23:59:59Z"[..],
            b"2027-02-29T23:59:59Z",
            b"2028-02-29T24:00:00Z",
            b"2028-02-29T18:14:09-05:45",
            b"2028-02-29T23:55:59+00:30",
            b"2028-02-29T23:55:59-00:30",
            b"2028-02-29T23:59:60Z",
            b"2028-02-29T23:59:59+23:59",
            b"2028-02-29T23:59:59+24:00",
            b"2028-02-29T23:59:59-23:60",
            b"2028-02-29T12;59:59Z",
            b"2028-02-29T12:59;59Z",
            b"2028-02-29T12:59:xxZ",
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
        struct Collect<'a>(Vec<(&'a [u8], [&'a [u8]; 7])>);
        impl<'a> EventSink<'a> for Collect<'a> {
            fn row(&mut self, line: &'a [u8], values: impl Fields<'a>) {
                self.0.push((
                    line,
                    [
                        values.field::<0>(),
                        values.field::<1>(),
                        values.field::<2>(),
                        values.field::<3>(),
                        values.field::<4>(),
                        values.field::<5>(),
                        values.field::<6>(),
                    ],
                ));
            }
        }
        let mut actual = Collect(Vec::new());
        scan_event_rows(&input, &mut actual);
        assert_eq!(actual.0, expected);
    }
}
