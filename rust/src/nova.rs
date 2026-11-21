//! Nova's event pipeline. The scan builds one cache-friendly stream family by event ID.

use crate::{DAY, EvP, LazyChunks, P, People, ReadAhead, Users, event_rows, lap, parse_price};
use ahash::AHashMap;
use rayon::prelude::*;
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

fn person_names(users: &Users<'_>, linked: &[&'static [u8]]) -> Option<(u32, Vec<&'static [u8]>)> {
    let n = (users.next.load(Relaxed) as usize).min(users.names.len());
    if n < linked.len() || n >= crate::PERSON_SET as usize {
        return None;
    }
    let names: &'static [PersonName] = Box::leak(
        (linked.len()..n)
            .into_par_iter()
            .map(|id| PersonName::decode(users.names[id].load(Relaxed)))
            .collect::<Vec<_>>()
            .into_boxed_slice(),
    );
    let mut all = Vec::with_capacity(n);
    all.extend_from_slice(linked);
    all.extend(
        names
            .iter()
            .map(|person| &person.bytes[..person.len as usize]),
    );
    Some((n.div_ceil(P) as u32, all))
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

#[repr(C)]
#[derive(Clone, Copy)]
struct Event {
    key: u64,
    ingest: i32,
    person: u32,
    ts: u32,
    price: u32,
}
const _: () = assert!(size_of::<Event>() == 24);

#[derive(Clone, Copy)]
struct Pending {
    row: Event,
    uid: u64,
}

impl Pending {
    const EMPTY: Self = Self {
        row: Event {
            key: 0,
            ingest: 0,
            person: 0,
            ts: INVALID,
            price: 0,
        },
        uid: 0,
    };
}

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

struct Parser<'a, 'catalog, 'table> {
    input: &'a EventInput<'catalog>,
    users: &'a Users<'table>,
    declined: &'a AtomicBool,
    window_seconds: i64,
    window_ms: u64,
    dates: Dates,
    ids: (u32, u32),
    batch: [Pending; 16],
    count: usize,
    streams: Streams<Event, SHARDS>,
}

impl Parser<'_, '_, '_> {
    fn parse_chunk(&mut self, bytes: &[u8]) {
        scan_event_rows(bytes, self);
    }

    #[inline]
    fn flush(&mut self) {
        for pending in &self.batch[..self.count] {
            let mut row = pending.row;
            if row.ts != INVALID {
                row.person =
                    self.users
                        .person(pending.uid, 0, pending.uid, &mut self.ids, self.declined);
            }
            self.streams.push((row.key >> 55) as u8 as usize, row);
        }
        self.count = 0;
    }
}

impl<'a> EventSink<'a> for Parser<'_, '_, '_> {
    #[inline]
    fn row(&mut self, _: &'a [u8], fields: impl Fields<'a>) {
        let id = fields.field::<0>();
        if id.is_empty() {
            return;
        }
        // The parser can release its input chunk after this call. No
        // captured identities reference the source buffer.
        let Some(key) = short_key(id, b'e').map(permute_short) else {
            self.declined.store(true, Relaxed);
            return;
        };
        let Some(seconds) = self.dates.utc_seconds(fields.field::<3>()) else {
            self.declined.store(true, Relaxed);
            return;
        };
        let Ok(order) = i32::try_from(seconds - self.window_seconds) else {
            self.declined.store(true, Relaxed);
            return;
        };
        let mut pending = Pending::EMPTY;
        pending.row.key = key;
        pending.row.ingest = order;
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
            if purchase {
                let Some(value) = parse_price(fields.field::<6>()) else {
                    break 'valid;
                };
                let category = *self
                    .input
                    .products
                    .get(fields.field::<5>())
                    .unwrap_or(&self.input.unknown);
                pending.row.price = value | (category << 24);
            }
            let Some(uid) = user_key(user) else {
                self.declined.store(true, Relaxed);
                break 'valid;
            };
            self.users.prefetch(uid);
            pending.row.ts = relative as u32;
            pending.uid = uid;
        }
        self.batch[self.count] = pending;
        self.count += 1;
        if self.count == self.batch.len() {
            self.flush();
        }
    }
}

fn events(
    work: &LazyChunks<'_>,
    read_ahead: &ReadAhead,
    users: &Users<'_>,
    input: &EventInput<'_>,
    declined: &AtomicBool,
) -> Vec<Streams<Event, SHARDS>> {
    let next = AtomicUsize::new(0);
    rayon::broadcast(|_| {
        let mut parser = Parser {
            input,
            users,
            declined,
            window_seconds: input.start.div_euclid(1000),
            window_ms: (input.end - input.start) as u64,
            dates: Dates::default(),
            ids: (0, 0),
            batch: [Pending::EMPTY; 16],
            count: 0,
            streams: Streams::new(),
        };
        loop {
            let index = next.fetch_add(1, Relaxed);
            if index >= work.len() {
                break;
            }
            read_ahead.wait(work.raw(index));
            parser.parse_chunk(work.get(index));
        }
        parser.flush();
        parser.streams
    })
}

type EventStream = Streams<Event, SHARDS>;

// Tag bits avoid an indirect load for almost every occupied cell with a
// different key. The location refers to a record in an immutable stream.
const LOCATION_BITS: u32 = 26;
const LOCATION_MASK: u32 = (1 << LOCATION_BITS) - 1;

struct Router {
    people: People,
    winners: Vec<u32>,
    destinations: Vec<u32>,
    bases: Vec<usize>,
}

impl Router {
    fn new(reservation: usize) -> Self {
        Self {
            people: People::new(reservation),
            winners: Vec::new(),
            destinations: Vec::new(),
            bases: Vec::new(),
        }
    }

    fn route(&mut self, folds: &[EventStream], shard: usize) -> Option<()> {
        let mut count = 0;
        let mut longest = 1;
        self.bases.clear();
        for fold in folds {
            self.bases.push(count);
            let rows = fold.len(shard);
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
        self.destinations.clear();
        self.destinations.resize(count, 0);

        for (file, fold) in folds.iter().enumerate() {
            for (index, row) in fold.records(shard).enumerate() {
                let hash = row.key;
                let tag = ((hash >> 23) as u32) & !LOCATION_MASK;
                let location = ((file << shift) + index + 1) as u32;
                let mut slot = hash as usize & mask;
                let mut keep = true;
                loop {
                    let winner = self.winners[slot];
                    if winner == 0 {
                        self.winners[slot] = tag | location;
                        break;
                    }
                    if winner & !LOCATION_MASK == tag {
                        let previous = ((winner & LOCATION_MASK) - 1) as usize;
                        let prior_file = previous >> shift;
                        let prior_index = previous & (stride - 1);
                        let prior = folds[prior_file].get(shard, prior_index);
                        if prior.key == row.key {
                            // Invalid copies still compete for earliest ingest.
                            if row.ingest < prior.ingest {
                                if prior.ts != INVALID {
                                    let dest = self.destinations[self.bases[prior_file] + prior_index];
                                    self.people.get(prior.person as usize & (P - 1), dest as usize)
                                        .ts.store(INVALID, Relaxed);
                                }
                                self.winners[slot] = tag | location;
                            } else {
                                keep = false;
                            }
                            break;
                        }
                    }
                    slot = (slot + 1) & mask;
                }
                if keep && row.ts != INVALID {
                    let partition = row.person as usize & (P - 1);
                    self.destinations[self.bases[file] + index] =
                        u32::try_from(self.people.len(partition)).ok()?;
                    self.people.push(partition, EvP {
                        person: row.person,
                        ts: AtomicU32::new(row.ts),
                        price: row.price,
                    });
                }
            }
        }
        Some(())
    }
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
    if declined.load(Relaxed) {
        return None;
    }
    let (local_count, names) = person_names(users, linked_names)?;
    lap(&format!(
        "events parsed (nova, {} folds, {} persons)",
        folds.len(),
        names.len()
    ));
    if declined.load(Relaxed) {
        return None;
    }
    let rows: usize = folds
        .iter()
        .map(|f| (0..SHARDS).map(|shard| f.len(shard)).sum::<usize>())
        .sum();
    let reservation = rows.div_ceil(P * rayon::current_num_threads()).min(32_768);
    let people = crate::pt_ordered(
        SHARDS,
        || Router::new(reservation),
        |mut router, shard| {
            if router.route(&folds, shard).is_none() {
                declined.store(true, Relaxed);
            }
            router
        },
    )
    .into_iter()
    .map(|router| router.people)
    .collect();
    if declined.load(Relaxed) {
        return None;
    }
    drop(folds);
    lap("events dedup + validate");
    Some((local_count, names, people))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fused_router_keeps_earliest_invalid_and_changed_person_copies() {
        let mut folds = vec![Streams::new(), Streams::new()];
        // Force overflow in the person output and cross a source block boundary.
        for i in 0..300u64 {
            folds[0].push(0, Event { key: i + 1, ingest: 20, person: 3, ts: 10, price: 1 });
        }
        folds[1].push(0, Event { key: 1, ingest: 10, person: 7, ts: INVALID, price: 0 });
        folds[1].push(0, Event { key: 2, ingest: 10, person: 7, ts: 5, price: 2 });
        folds[1].push(0, Event { key: 2, ingest: 30, person: 3, ts: 20, price: 3 });
        folds[1].push(0, Event { key: 3, ingest: 20, person: 3, ts: 10, price: 1 });
        let mut router = Router::new(1);
        router.route(&folds, 0).unwrap();
        let live = |part| router.people.records(part).into_iter().flatten()
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
