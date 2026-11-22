use std::cmp::Ordering;
use std::collections::HashMap;
use std::hash::{BuildHasherDefault, Hasher};
use std::sync::Mutex;

use crate::sys::fallback;

const BITS: u32 = 6;
pub const SHARDS: usize = 1 << BITS;
const LONG: u64 = 1 << 63;
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
        key = (key << 6) & ((1 << 48) - 1);
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

pub struct FrozenDictionary([Shard; SHARDS]);

impl FrozenDictionary {
    #[inline]
    pub fn lookup(&self, id: Id<'_>, hash: u64) -> Option<u32> {
        let stripe = shard(hash);
        let index = self.0[stripe].lookup(id, hash)?;
        Some((index << BITS) | stripe as u32)
    }

    pub fn into_names(self) -> Names {
        Names(self.0.map(|shard| shard.keys))
    }
}

pub struct Names([Keys; SHARDS]);

impl Names {
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
}
