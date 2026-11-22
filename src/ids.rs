use std::cmp::Ordering;
use std::collections::HashMap;
use std::hash::{BuildHasherDefault, Hasher};
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering::Relaxed};
use std::sync::Mutex;

use crate::sys::fallback;

const BITS: u32 = 6;
pub const SHARDS: usize = 1 << BITS;
const LONG: u64 = 1 << 63;
const SHORT_MASK: u64 = (1 << 48) - 1;
const NONE: u32 = u32::MAX;

const fn codes() -> [u8; 256] {
    let mut table = [0; 256];
    let mut i = 0;
    while i < 10 {
        table[b'0' as usize + i] = i as u8 + 1;
        i += 1;
    }
    let mut i = 0;
    while i < 26 {
        table[b'a' as usize + i] = i as u8 + 11;
        i += 1;
    }
    table
}
const CODES: [u8; 256] = codes();
const DIGITS: &[u8; 36] = b"0123456789abcdefghijklmnopqrstuvwxyz";

// Prefix is implicit in each dictionary. Left-aligned, nonzero digit
// codes make integer comparison equivalent to byte order, including prefixes.
pub fn pack(bytes: &[u8], prefix: u8) -> Option<u64> {
    let (&first, tail) = bytes.split_first()?;
    if first != prefix || !(1..=8).contains(&tail.len()) {
        return None;
    }
    let mut key = 0u64;
    for &b in tail {
        let code = CODES[b as usize];
        if code == 0 {
            return None;
        }
        key = key << 6 | code as u64;
    }
    Some(key << (6 * (8 - tail.len())))
}

fn unpack(mut key: u64, prefix: u8, buf: &mut [u8; 9]) -> &[u8] {
    buf[0] = prefix;
    let mut n = 1;
    while key != 0 {
        let code = (key >> 42) as usize;
        buf[n] = DIGITS[code - 1];
        n += 1;
        key = (key << 6) & SHORT_MASK;
    }
    &buf[..n]
}

#[derive(Clone, Copy)]
pub enum Id<'a> {
    Short(u64),
    Long(&'a [u8]),
}

impl<'a> Id<'a> {
    pub fn parse(bytes: &'a [u8], prefix: u8) -> Self {
        pack(bytes, prefix).map_or(Self::Long(bytes), Self::Short)
    }

    #[inline]
    pub fn hash(self) -> u64 {
        match self {
            Self::Short(key) => mix(key),
            Self::Long(bytes) => hash_bytes(bytes),
        }
    }

    pub fn cmp(self, other: Self, prefix: u8) -> Ordering {
        match (self, other) {
            (Self::Short(a), Self::Short(b)) => a.cmp(&b),
            (Self::Long(a), Self::Long(b)) => a.cmp(b),
            (Self::Short(a), Self::Long(b)) => unpack(a, prefix, &mut [0; 9]).cmp(b),
            (Self::Long(a), Self::Short(b)) => a.cmp(unpack(b, prefix, &mut [0; 9])),
        }
    }
}

#[inline]
fn mix(h: u64) -> u64 {
    let h = (h ^ (h >> 29)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    h ^ (h >> 32)
}

// A reversible mix of the 48 packed bits in a 63-bit word, leaving the
// high bit available for raw-span claims. Short event equality is exact.
#[inline]
pub fn event_key(key: u64) -> u64 {
    let mixed = (key ^ (key >> 29)).wrapping_mul(0xbf58_476d_1ce4_e5b9) & (u64::MAX >> 1);
    mixed ^ (mixed >> 32)
}

fn hash_bytes(bytes: &[u8]) -> u64 {
    let mut hash = bytes.len() as u64 ^ 0x517c_c1b7_2722_0a95;
    let (words, tail) = bytes.as_chunks::<8>();
    for word in words {
        hash =
            (hash.rotate_left(19) ^ u64::from_le_bytes(*word)).wrapping_mul(0x9e37_79b9_7f4a_7c15);
    }
    let mut last = [0; 8];
    last[..tail.len()].copy_from_slice(tail);
    mix(hash ^ u64::from_le_bytes(last))
}

#[inline]
pub fn shard(hash: u64) -> usize {
    ((hash >> 32) as usize) & (SHARDS - 1)
}

#[derive(Default)]
pub struct IdentityHasher(u64);

impl Hasher for IdentityHasher {
    fn finish(&self) -> u64 {
        self.0
    }
    fn write(&mut self, _: &[u8]) {
        unreachable!("dictionary keys are u64 hashes")
    }
    fn write_u64(&mut self, hash: u64) {
        self.0 = hash;
    }
}

#[derive(Default)]
struct Keys {
    entries: Vec<u64>,
    bytes: Vec<u8>,
}

impl Keys {
    #[inline]
    fn id(&self, index: u32) -> Id<'_> {
        let key = self.entries[index as usize];
        if key & LONG == 0 {
            return Id::Short(key);
        }
        let at = ((key & !LONG) >> 16) as usize;
        let len = (key as u16) as usize;
        Id::Long(&self.bytes[at..at + len])
    }

    #[inline]
    fn matches(&self, index: u32, id: Id<'_>) -> bool {
        match (id, self.id(index)) {
            (Id::Short(a), Id::Short(b)) => a == b,
            (Id::Long(a), Id::Long(b)) => a == b,
            _ => false,
        }
    }

    fn push(&mut self, id: Id<'_>) {
        let key = match id {
            Id::Short(key) => key,
            Id::Long(bytes) => {
                let at = self.bytes.len() as u64;
                let len = u16::try_from(bytes.len()).unwrap_or_else(|_| fallback("ID too long"));
                if at >= (1 << 47) {
                    fallback("ID table exceeds byte capacity");
                }
                self.bytes.extend_from_slice(bytes);
                LONG | at << 16 | len as u64
            }
        };
        self.entries.push(key);
    }
}

#[derive(Default)]
pub struct Shard {
    table: HashMap<u64, u32, BuildHasherDefault<IdentityHasher>>,
    keys: Keys,
    next: Vec<u32>,
}

impl Shard {
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            table: HashMap::with_capacity_and_hasher(capacity, BuildHasherDefault::default()),
            keys: Keys {
                entries: Vec::with_capacity(capacity),
                bytes: Vec::new(),
            },
            next: Vec::with_capacity(capacity),
        }
    }

    #[inline]
    fn lookup(&self, id: Id<'_>, hash: u64) -> Option<u32> {
        let mut index = *self.table.get(&hash)?;
        while index != NONE {
            if self.keys.matches(index, id) {
                return Some(index);
            }
            index = self.next[index as usize];
        }
        None
    }

    // The hash is only a bucket selector. Check the stored bytes on every
    // matching long-ID hash and keep any true collisions on the same chain.
    #[inline]
    pub fn intern(&mut self, id: Id<'_>, hash: u64) -> (u32, bool) {
        let head = self.table.entry(hash).or_insert(NONE);
        let mut index = *head;
        while index != NONE {
            if self.keys.matches(index, id) {
                return (index, false);
            }
            index = self.next[index as usize];
        }
        let index = self.keys.entries.len() as u32;
        if index >= (1 << (32 - BITS)) {
            fallback("ID shard exceeds index capacity");
        }
        self.keys.push(id);
        self.next.push(*head);
        *head = index;
        (index, true)
    }
}

#[repr(align(128))]
#[derive(Default)]
struct Stripe(Mutex<Shard>);

pub struct Dictionary([Stripe; SHARDS]);

impl Dictionary {
    pub fn new() -> Self {
        Self(std::array::from_fn(|_| Stripe::default()))
    }

    #[inline]
    pub fn intern(&self, id: Id<'_>, hash: u64) -> (u32, bool) {
        let stripe = shard(hash);
        let (index, is_new) = self.0[stripe].0.lock().unwrap().intern(id, hash);
        ((index << BITS) | stripe as u32, is_new)
    }

    pub fn freeze(self) -> FrozenDictionary {
        FrozenDictionary(self.0.map(|stripe| stripe.0.into_inner().unwrap()))
    }

    pub fn into_names(self) -> Names {
        self.freeze().into_names()
    }
}

// Lock-free interning for long IDs. A slot packs a 16-bit tag and a 1-based
// local index; entries[local] packs a pointer and length of the stored bytes.
// New keys are copied into per-thread bump chunks, so the table never moves.
const LONG_PENDING: u64 = u32::MAX as u64;
const LONG_CHUNK: usize = 1 << 20;

struct LongStripe {
    slots: &'static [AtomicU64],
    entries: &'static [AtomicU64],
    next: AtomicU32,
}

pub struct LongDictionary {
    stripes: Vec<LongStripe>,
    arena: crate::sys::Arena,
}

pub struct LongSink {
    cur: *mut u8,
    end: *mut u8,
}

impl LongSink {
    pub fn new() -> Self {
        Self { cur: std::ptr::null_mut(), end: std::ptr::null_mut() }
    }
}

impl LongDictionary {
    pub fn new(max_users: usize, byte_cap: usize) -> Self {
        let slots = (2 * max_users.div_ceil(SHARDS)).next_power_of_two().max(1024);
        let cap = slots / 2;
        let base = crate::sys::anon(SHARDS * (slots + cap) * 8) as *const AtomicU64;
        let stripes = (0..SHARDS)
            .map(|i| unsafe {
                let at = base.add(i * (slots + cap));
                LongStripe {
                    slots: std::slice::from_raw_parts(at, slots),
                    entries: std::slice::from_raw_parts(at.add(slots), cap),
                    next: AtomicU32::new(0),
                }
            })
            .collect();
        Self { stripes, arena: crate::sys::Arena::new(byte_cap + (64 << 20)) }
    }

    #[inline]
    pub fn prefetch(&self, hash: u64) {
        let table = &self.stripes[shard(hash)];
        let slot = hash as usize & (table.slots.len() - 1);
        unsafe {
            use std::arch::x86_64::{_mm_prefetch, _MM_HINT_T0};
            _mm_prefetch::<_MM_HINT_T0>(table.slots.as_ptr().add(slot).cast());
        }
    }

    fn store(&self, sink: &mut LongSink, bytes: &[u8]) -> u64 {
        if bytes.len() > u16::MAX as usize {
            fallback("ID too long");
        }
        if (sink.end as usize - sink.cur as usize) < bytes.len() {
            let n = bytes.len().max(LONG_CHUNK);
            sink.cur = self.arena.alloc(n);
            sink.end = unsafe { sink.cur.add(n) };
        }
        let at = sink.cur;
        unsafe {
            std::ptr::copy_nonoverlapping(bytes.as_ptr(), at, bytes.len());
            sink.cur = at.add(bytes.len());
        }
        (at as u64) << 16 | bytes.len() as u64
    }

    pub fn intern(&self, bytes: &[u8], hash: u64, sink: &mut LongSink) -> u32 {
        let stripe = shard(hash);
        let table = &self.stripes[stripe];
        let mask = table.slots.len() - 1;
        let tag = hash >> 48;
        let start = hash as usize & mask;
        let mut slot = start;
        loop {
            let cell = &table.slots[slot];
            let mut word = cell.load(std::sync::atomic::Ordering::Acquire);
            if word == 0 {
                match cell.compare_exchange(
                    0,
                    tag << 48 | LONG_PENDING,
                    std::sync::atomic::Ordering::Acquire,
                    std::sync::atomic::Ordering::Acquire,
                ) {
                    Ok(_) => {
                        let local = table.next.fetch_add(1, Relaxed) as usize;
                        if local >= table.entries.len() || local >= (1 << (32 - BITS)) - 1 {
                            fallback("long user shard exceeds capacity");
                        }
                        table.entries[local].store(self.store(sink, bytes), Relaxed);
                        cell.store(tag << 48 | (local as u64 + 1), std::sync::atomic::Ordering::Release);
                        return ((local as u32) << BITS) | stripe as u32;
                    }
                    Err(current) => word = current,
                }
            }
            if word >> 48 == tag {
                while word & 0xFFFF_FFFF == LONG_PENDING {
                    std::hint::spin_loop();
                    word = cell.load(std::sync::atomic::Ordering::Acquire);
                }
                let local = (word & 0xFFFF_FFFF) as usize - 1;
                let entry = table.entries[local].load(Relaxed);
                let stored = unsafe {
                    std::slice::from_raw_parts((entry >> 16) as *const u8, (entry & 0xFFFF) as usize)
                };
                if stored == bytes {
                    return ((local as u32) << BITS) | stripe as u32;
                }
            }
            slot = (slot + 1) & mask;
            if slot == start {
                fallback("long user shard is full");
            }
        }
    }

    pub fn into_names(self) -> LongNames {
        let counts = self.stripes.iter().map(|s| s.next.load(Relaxed) as usize).collect();
        LongNames { stripes: self.stripes, counts, _arena: self.arena }
    }
}

pub struct LongNames {
    stripes: Vec<LongStripe>,
    counts: Vec<usize>,
    _arena: crate::sys::Arena,
}

impl LongNames {
    pub fn bytes<'a>(&'a self, index: u32, _prefix: u8, _buf: &'a mut [u8; 9]) -> &'a [u8] {
        let stripe = index as usize & (SHARDS - 1);
        let local = (index >> BITS) as usize;
        assert!(local < self.counts[stripe]);
        let entry = self.stripes[stripe].entries[local].load(Relaxed);
        unsafe { std::slice::from_raw_parts((entry >> 16) as *const u8, (entry & 0xFFFF) as usize) }
    }
}

pub struct FrozenDictionary([Shard; SHARDS]);

impl FrozenDictionary {
    #[inline]
    pub fn lookup(&self, id: Id<'_>, hash: u64) -> Option<u32> {
        let stripe = shard(hash);
        let index = self.0[stripe].lookup(id, hash)?;
        Some((index << BITS) | stripe as u32)
    }

    pub fn id(&self, index: u32) -> Option<Id<'_>> {
        let keys = &self.0[index as usize & (SHARDS - 1)].keys;
        let local = index >> BITS;
        ((local as usize) < keys.entries.len()).then(|| keys.id(local))
    }

    pub fn short_people(&self, stripe: usize, roots: &[u32]) -> PackedPeople {
        let entries = &self.0[stripe].keys.entries;
        let user_bits = usize::BITS - roots.len().saturating_sub(1).leading_zeros();
        let mut short = PackedPeople::new(entries.len(), user_bits);
        for (local, &key) in entries.iter().enumerate() {
            if key & LONG == 0 {
                let index = Names::index(stripe, local as u32);
                short.insert(key, roots[index as usize]);
            }
        }
        short
    }

    pub fn into_names(self) -> Names {
        Names::from_shards(self.0)
    }
}

pub struct Names([Keys; SHARDS]);

impl Names {
    pub fn from_shards(shards: [Shard; SHARDS]) -> Self {
        Self(shards.map(|shard| shard.keys))
    }

    pub fn index(shard: usize, local: u32) -> u32 {
        (local << BITS) | shard as u32
    }

    fn id(&self, index: u32) -> Id<'_> {
        self.0[index as usize & (SHARDS - 1)].id(index >> BITS)
    }

    pub fn cmp(&self, a: u32, b: u32, prefix: u8) -> Ordering {
        self.id(a).cmp(self.id(b), prefix)
    }

    pub fn bytes<'a>(&'a self, index: u32, prefix: u8, buf: &'a mut [u8; 9]) -> &'a [u8] {
        match self.id(index) {
            Id::Short(key) => unpack(key, prefix, buf),
            Id::Long(bytes) => bytes,
        }
    }
}

#[repr(align(64))]
#[derive(Clone, Copy)]
struct PeopleBucket([u64; 8]);

pub struct PackedPeople {
    buckets: Vec<PeopleBucket>,
    overflow: HashMap<u64, u32, BuildHasherDefault<IdentityHasher>>,
    bucket_bits: u32,
    value_shift: u32,
    key_mask: u64,
}

impl PackedPeople {
    fn new(entries: usize, user_bits: u32) -> Self {
        let min_bucket_bits = (49 + user_bits).saturating_sub(64);
        let buckets = entries
            .div_ceil(3)
            .next_power_of_two()
            .max(1 << min_bucket_bits);
        let bucket_bits = buckets.trailing_zeros();
        let value_shift = 49 - bucket_bits;
        Self {
            buckets: vec![PeopleBucket([0; 8]); if entries == 0 { 0 } else { buckets }],
            overflow: HashMap::default(),
            bucket_bits,
            value_shift,
            key_mask: (1 << value_shift) - 1,
        }
    }

    // Multiplication by an odd constant and XOR with a right shift are
    // reversible in 48 bits. The bucket stores the low bits implicitly,
    // so the remaining bits are the complete key within that bucket.
    #[inline]
    fn position(&self, key: u64) -> (usize, u64) {
        let h = key.wrapping_mul(0x517c_c1b7_2722_0a95) & SHORT_MASK;
        let hash = h ^ (h >> 23);
        (
            hash as usize & (self.buckets.len() - 1),
            (hash >> self.bucket_bits) + 1,
        )
    }

    fn insert(&mut self, key: u64, user: u32) {
        let (bucket, quotient) = self.position(key);
        if let Some(slot) = self.buckets[bucket].0.iter_mut().find(|slot| **slot == 0) {
            *slot = (user as u64) << self.value_shift | quotient;
        } else {
            self.overflow.insert(key, user);
        }
    }

    #[inline]
    pub fn prefetch(&self, key: u64) {
        if self.buckets.is_empty() {
            return;
        }
        let (bucket, _) = self.position(key);
        unsafe {
            use std::arch::x86_64::{_mm_prefetch, _MM_HINT_T0};
            _mm_prefetch::<_MM_HINT_T0>(self.buckets.as_ptr().add(bucket).cast());
        }
    }

    #[inline]
    pub fn get(&self, key: u64) -> Option<u32> {
        if self.buckets.is_empty() {
            return None;
        }
        let (bucket, quotient) = self.position(key);
        let words = &self.buckets[bucket];
        let matches = unsafe {
            use std::arch::x86_64::*;
            let group = _mm512_load_si512(words.0.as_ptr().cast());
            let keys = _mm512_and_si512(group, _mm512_set1_epi64(self.key_mask as i64));
            _mm512_cmpeq_epi64_mask(keys, _mm512_set1_epi64(quotient as i64))
        };
        if matches != 0 {
            Some((words.0[matches.trailing_zeros() as usize] >> self.value_shift) as u32)
        } else {
            self.overflow.get(&key).copied()
        }
    }
}

#[repr(align(128))]
struct AtomicStripe {
    slots: &'static [AtomicU64],
    next: AtomicU32,
}

pub struct PackedDictionary([AtomicStripe; SHARDS]);

impl PackedDictionary {
    pub fn new(capacity: usize) -> Self {
        let slots = (2 * capacity.div_ceil(SHARDS)).next_power_of_two().max(64);
        // One zeroed huge-page region (never freed) backs every stripe, so
        // random probes cost one TLB entry per 2 MB instead of per 4 KB.
        let base = crate::sys::anon(SHARDS * slots * 8) as *const AtomicU64;
        Self(std::array::from_fn(|i| AtomicStripe {
            slots: unsafe { std::slice::from_raw_parts(base.add(i * slots), slots) },
            next: AtomicU32::new(0),
        }))
    }

    // Each word holds the entire packed ID and a 1-based local index.
    // Claim an empty slot with the packed ID before publishing its index.
    pub fn intern(&self, key: u64, hash: u64) -> u32 {
        let stripe = shard(hash);
        let table = &self.0[stripe];
        let mask = table.slots.len() - 1;
        let start = hash as usize & mask;
        let mut slot = start;
        loop {
            let cell = &table.slots[slot];
            let mut word = cell.load(Relaxed);
            if word == 0 {
                match cell.compare_exchange(0, key, Relaxed, Relaxed) {
                    Ok(_) => {
                        let local = table.next.fetch_add(1, Relaxed) + 1;
                        if local > u16::MAX as u32 {
                            fallback("packed user shard exceeds index capacity");
                        }
                        cell.store(key | (local as u64) << 48, Relaxed);
                        return Names::index(stripe, local - 1);
                    }
                    Err(current) => word = current,
                }
            }
            if word & SHORT_MASK == key {
                while word >> 48 == 0 {
                    std::hint::spin_loop();
                    word = cell.load(Relaxed);
                }
                return Names::index(stripe, (word >> 48) as u32 - 1);
            }
            slot = (slot + 1) & mask;
            if slot == start {
                fallback("packed user shard is full");
            }
        }
    }

    #[inline]
    pub fn prefetch(&self, hash: u64) {
        let table = &self.0[shard(hash)];
        let slot = hash as usize & (table.slots.len() - 1);
        unsafe {
            use std::arch::x86_64::{_mm_prefetch, _MM_HINT_T0};
            _mm_prefetch::<_MM_HINT_T0>(table.slots.as_ptr().add(slot).cast());
        }
    }

    pub fn into_names(self) -> PackedNames {
        PackedNames(self.0.map(|stripe| {
            let mut keys = vec![0; stripe.next.into_inner() as usize];
            for slot in stripe.slots {
                let word = slot.load(Relaxed);
                if word != 0 {
                    keys[(word >> 48) as usize - 1] = word & SHORT_MASK;
                }
            }
            keys
        }))
    }
}

pub struct PackedNames([Vec<u64>; SHARDS]);

impl PackedNames {
    pub fn bytes<'a>(&self, index: u32, prefix: u8, buf: &'a mut [u8; 9]) -> &'a [u8] {
        let key = self.0[index as usize & (SHARDS - 1)][(index >> BITS) as usize];
        unpack(key, prefix, buf)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn forced_hash_collision_keeps_full_ids_distinct() {
        let dictionary = Dictionary::new();
        let a = Id::Long(b"u0123456789abcdef0123456789abcdef");
        let b = Id::Long(b"u0123456789abcdef0123456789abcdeF");
        let (i, new_i) = dictionary.intern(a, 123);
        let (j, new_j) = dictionary.intern(b, 123);
        assert!(new_i && new_j);
        assert_ne!(i, j);
        assert_eq!(dictionary.intern(a, 123), (i, false));
        assert_eq!(dictionary.intern(b, 123), (j, false));
        let names = dictionary.into_names();
        assert_eq!(names.cmp(i, j, b'u'), Ordering::Greater);
        assert_eq!(
            names.bytes(i, b'u', &mut [0; 9]),
            b"u0123456789abcdef0123456789abcdef"
        );
    }

    #[test]
    fn short_pack_keeps_byte_order_and_leading_zeroes() {
        let ids = [
            b"u0".as_slice(),
            b"u00",
            b"u01",
            b"u1",
            b"u10",
            b"ua",
            b"uabc123zz",
            b"uzz",
        ];
        for pair in ids.windows(2) {
            assert!(pack(pair[0], b'u') < pack(pair[1], b'u'));
        }
        for id in ids {
            assert_eq!(unpack(pack(id, b'u').unwrap(), b'u', &mut [0; 9]), id);
        }
        assert_eq!(pack(b"uABC", b'u'), None);
        assert_eq!(pack(b"u000000001", b'u'), None);
    }

    #[test]
    fn concurrent_packed_interning_publishes_one_index_per_key() {
        let dictionary = PackedDictionary::new(64);
        let a = pack(b"u000123ab", b'u').unwrap();
        let b = pack(b"u000123ac", b'u').unwrap();
        let (a_index, b_index) = std::thread::scope(|scope| {
            let threads: Vec<_> = (0..8)
                .map(|_| {
                    scope.spawn(|| {
                        let a_index = dictionary.intern(a, 42);
                        let b_index = dictionary.intern(b, 42);
                        for _ in 0..100 {
                            assert_eq!(dictionary.intern(a, 42), a_index);
                            assert_eq!(dictionary.intern(b, 42), b_index);
                        }
                        (a_index, b_index)
                    })
                })
                .collect();
            let answers: Vec<_> = threads
                .into_iter()
                .map(|thread| thread.join().unwrap())
                .collect();
            assert!(answers.iter().all(|answer| *answer == answers[0]));
            answers[0]
        });
        assert_ne!(a_index, b_index);
        let names = dictionary.into_names();
        assert_eq!(names.bytes(a_index, b'u', &mut [0; 9]), b"u000123ab");
        assert_eq!(names.bytes(b_index, b'u', &mut [0; 9]), b"u000123ac");
    }
}
