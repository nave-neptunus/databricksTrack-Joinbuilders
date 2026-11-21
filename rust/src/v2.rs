//! Direct person-partition ingestion. Event-ID shards hold references to
//! records in those partitions, so deduplication never moves a winner.

use crate::nova::{Dates, EventInput, EventSink, Fields, Streams, scan_event_rows};
use crate::{
    DAY, EvP, LazyChunks, People, ReadAhead, TOMBSTONE, Users, hs, lap, mum, pack_ref, parse_price,
};
use rayon::prelude::*;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicUsize, Ordering::Relaxed};

pub(super) const PERSON_BITS: u32 = 7;
const PARTITIONS: usize = 1 << PERSON_BITS;
const MAX_SHARDS: usize = 1024;
const SHARD_ROWS: usize = 98_304;
const NO_EVENT: u32 = u32::MAX;
const LOCATION_BITS: u32 = 26;
const LOCATION_MASK: u32 = (1 << LOCATION_BITS) - 1;

/// Keep the injective compact UID representation for its supported alphabet.
/// The other half of the key space is reserved for the general UID table.
#[inline]
pub(super) fn user_key(user: &[u8], seed: u64) -> (u64, u32) {
    if let Some(key) = crate::nova::user_key(user) {
        (key, 0)
    } else {
        let (key, check) = hs(user, seed);
        (key & (u64::MAX >> 1), check)
    }
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct InlineKey([u64; 2]);

impl InlineKey {
    #[inline]
    fn pack(id: &[u8], bytes: &[u8]) -> Option<Self> {
        if id.len() > 16 {
            return None;
        }
        let first = Self::word(bytes, id.len().min(8))?;
        let second = if id.len() > 8 {
            Self::word(&bytes[8..], id.len() - 8)?
        } else {
            0
        };
        Some(Self([first, second]))
    }

    #[inline]
    fn word(bytes: &[u8], len: usize) -> Option<u64> {
        if len == 0 {
            return Some(0);
        }
        let keep = u64::MAX << (64 - len * 8);
        let word = if let Some(prefix) = bytes.first_chunk::<8>() {
            u64::from_be_bytes(*prefix) & keep
        } else {
            let mut padded = [0; 8];
            padded[..len].copy_from_slice(&bytes[..len]);
            u64::from_be_bytes(padded)
        };
        // A zero byte is the padding marker. Such IDs go to the exact side
        // table, so a prefix and an ID ending in zero cannot alias.
        let marked = word | !keep;
        const ONES: u64 = 0x0101_0101_0101_0101;
        ((marked.wrapping_sub(ONES) & !marked & (ONES << 7)) == 0).then_some(word)
    }

    #[inline]
    fn escaped(hash: u32, index: u32) -> Self {
        Self([0, (u64::from(hash) << 32) | u64::from(index)])
    }
}

trait ExactKey: Copy {
    fn hash(self) -> u32;
    fn same(self, ids: &[&[u8]], other: Self, other_ids: &[&[u8]]) -> bool;
}

impl ExactKey for InlineKey {
    #[inline]
    fn hash(self) -> u32 {
        let [a, b] = self.0;
        if a == 0 {
            (b >> 32) as u32
        } else {
            (mum(a ^ crate::K0, b ^ crate::K1) >> 32) as u32
        }
    }

    #[inline]
    fn same(self, ids: &[&[u8]], other: Self, other_ids: &[&[u8]]) -> bool {
        match (self.0[0] == 0, other.0[0] == 0) {
            (false, false) => self == other,
            (true, true) => ids[self.0[1] as u32 as usize] == other_ids[other.0[1] as u32 as usize],
            _ => false,
        }
    }
}

/// In this stream every packed ID starts with the same byte. Its suffix
/// alone carries the full identity and preserves the byte order.
#[derive(Clone, Copy, PartialEq, Eq)]
struct CommonKey(u64);

impl ExactKey for CommonKey {
    #[inline]
    fn hash(self) -> u32 {
        (mum(self.0 ^ crate::K0, crate::K1) >> 32) as u32
    }

    #[inline]
    fn same(self, _: &[&[u8]], other: Self, _: &[&[u8]]) -> bool {
        self == other
    }
}

#[derive(Clone, Copy)]
struct KeyRef<K> {
    key: K,
    order: i32,
    location: u32,
}

const _: () = assert!(std::mem::size_of::<KeyRef<CommonKey>>() == 16);
const _: () = assert!(std::mem::size_of::<KeyRef<InlineKey>>() == 24);

#[derive(Clone, Copy)]
struct Purchase {
    location: u32,
    product: &'static [u8],
    price: &'static [u8],
}

struct Fold {
    people: People,
    common: Streams<KeyRef<CommonKey>, MAX_SHARDS>,
    wide: Streams<KeyRef<InlineKey>, MAX_SHARDS>,
    long_ids: Vec<&'static [u8]>,
    purchases: Vec<Purchase>,
}

impl Fold {
    fn new(reservation: usize) -> Self {
        Self {
            people: People::with_partitions(reservation, PARTITIONS),
            common: Streams::new(),
            wide: Streams::new(),
            long_ids: Vec::new(),
            purchases: Vec::new(),
        }
    }

    #[inline]
    fn kill(&self, location: u32) {
        if location != NO_EVENT {
            let part = location as usize & (PARTITIONS - 1);
            let index = (location >> PERSON_BITS) as usize;
            self.people.get(part, index).ts.store(TOMBSTONE, Relaxed);
        }
    }

    fn finish(mut self, input: &EventInput<'_>) -> People {
        for Purchase {
            location,
            product,
            price,
        } in self.purchases
        {
            let part = location as usize & (PARTITIONS - 1);
            let index = (location >> PERSON_BITS) as usize;
            let row = self.people.get_mut(part, index);
            if *row.ts.get_mut() == TOMBSTONE {
                continue;
            }
            if let Some(value) = parse_price(price) {
                let category = *input.products.get(product).unwrap_or(&input.unknown);
                row.price = value | (category << 24);
            } else {
                *row.ts.get_mut() = TOMBSTONE;
            }
        }
        self.people
    }
}

// Inline keys start with a nonzero byte in their high word. The two
// smaller words identify an escaped key and a common-prefix key.
const COMMON_TAG: u64 = 1;
const NO_PURCHASE: u32 = u32::MAX;

#[derive(Clone, Copy)]
struct Pending {
    id: InlineKey,
    order: i32,
    ts: u32,
    user_key: u64,
    user_check: u32,
    name: u64,
    purchase: u32,
}

const BATCH: usize = 16;

struct Parser<'a, 'table> {
    fold: Fold,
    users: &'a Users<'table>,
    declined: &'a AtomicBool,
    shard_mask: usize,
    ids: (u32, u32),
    seed: u64,
    dates: Dates,
    origin: i64,
    origin_seconds: i64,
    window_ms: u64,
    pending: Vec<Pending>,
}

impl Parser<'_, '_> {
    #[inline]
    fn push_key(fold: &mut Fold, shard_mask: usize, id: InlineKey, order: i32, location: u32) {
        if id.0[0] == COMMON_TAG {
            let key = CommonKey(id.0[1]);
            let shard = (key.hash() >> 22) as usize & shard_mask;
            fold.common.push(
                shard,
                KeyRef {
                    key,
                    order,
                    location,
                },
            );
        } else {
            let shard = (id.hash() >> 22) as usize & shard_mask;
            fold.wide.push(
                shard,
                KeyRef {
                    key: id,
                    order,
                    location,
                },
            );
        }
    }

    #[inline]
    fn decode_key(&mut self, id: &'static [u8], line: &[u8]) -> InlineKey {
        let start = usize::from(line[0] == b'"');
        let bytes = &line[start..];
        if id[0] == b'e' && id.len() <= 9 {
            if let Some(word) = InlineKey::word(&bytes[1..], id.len() - 1) {
                return InlineKey([COMMON_TAG, word]);
            }
        }
        match InlineKey::pack(id, bytes) {
            Some(key) => key,
            None => {
                let hash = (hs(id, self.seed).0 >> 32) as u32;
                let Ok(index) = u32::try_from(self.fold.long_ids.len()) else {
                    self.declined.store(true, Relaxed);
                    return InlineKey::escaped(hash, 0);
                };
                self.fold.long_ids.push(id);
                InlineKey::escaped(hash, index)
            }
        }
    }

    #[inline]
    fn flush(&mut self) {
        for row in self.pending.drain(..) {
            let person = self.users.person(
                row.user_key,
                row.user_check,
                row.name,
                &mut self.ids,
                self.declined,
            );
            let part = person as usize & (PARTITIONS - 1);
            let index = self.fold.people.push(
                part,
                EvP {
                    person,
                    ts: AtomicU32::new(row.ts),
                    price: 0,
                },
            );
            if index >= (u32::MAX >> PERSON_BITS) as usize {
                self.declined.store(true, Relaxed);
                continue;
            }
            let location = (index as u32) << PERSON_BITS | part as u32;
            if row.purchase != NO_PURCHASE {
                self.fold.purchases[row.purchase as usize].location = location;
            }
            Self::push_key(&mut self.fold, self.shard_mask, row.id, row.order, location);
        }
    }

    #[inline]
    fn validate(
        &mut self,
        fields: impl Fields<'static>,
        ingest: i64,
        id: InlineKey,
        order: i32,
    ) -> Option<Pending> {
        let user = fields.field::<1>();
        if user.is_empty() || fields.empty::<5>() || ingest == i64::MAX {
            return None;
        }
        let purchase = match fields.field::<4>() {
            b"view" | b"cart" => false,
            b"purchase" => true,
            _ => return None,
        };
        let ts = self.dates.parse(fields.field::<2>())?;
        let relative = ts.wrapping_sub(self.origin) as u64;
        if relative >= self.window_ms || ingest.saturating_sub(ts) > DAY {
            return None;
        }
        let Some(name) = pack_ref(user) else {
            self.declined.store(true, Relaxed);
            return None;
        };
        let (user_key, user_check) = user_key(user, self.seed);
        self.users.prefetch(user_key);
        let purchase = if purchase {
            let index = self.fold.purchases.len() as u32;
            self.fold.purchases.push(Purchase {
                location: NO_EVENT,
                product: fields.field::<5>(),
                price: fields.field::<6>(),
            });
            index
        } else {
            NO_PURCHASE
        };
        Some(Pending {
            id,
            order,
            ts: relative as u32,
            user_key,
            user_check,
            name,
            purchase,
        })
    }
}

impl EventSink<'static> for Parser<'_, '_> {
    #[inline]
    fn row(&mut self, line: &'static [u8], fields: impl Fields<'static>) {
        let id = fields.field::<0>();
        if id.is_empty() {
            return;
        }
        let ingest_field = fields.field::<3>();
        let (ingest, seconds) = if let Some(seconds) = self.dates.utc_seconds(ingest_field) {
            (seconds * 1000, Some(seconds))
        } else {
            match crate::parse_ts(ingest_field) {
                None | Some(i64::MAX) => (i64::MAX, None),
                Some(value) if value % 1000 == 0 => (value, Some(value / 1000)),
                Some(_) => {
                    self.declined.store(true, Relaxed);
                    return;
                }
            }
        };
        let order = if let Some(seconds) = seconds {
            let Ok(order) = i32::try_from(seconds - self.origin_seconds) else {
                self.declined.store(true, Relaxed);
                return;
            };
            if order == i32::MAX {
                self.declined.store(true, Relaxed);
                return;
            }
            order
        } else {
            i32::MAX
        };
        let id = self.decode_key(id, line);
        if let Some(row) = self.validate(fields, ingest, id, order) {
            self.pending.push(row);
            if self.pending.len() == BATCH {
                self.flush();
            }
        } else {
            Self::push_key(&mut self.fold, self.shard_mask, id, order, NO_EVENT);
        }
    }
}

#[derive(Default)]
struct Dedup {
    seen: Vec<u64>,
    repeated: Vec<u64>,
    winners: Vec<u32>,
}

impl Dedup {
    fn shard<K: ExactKey>(
        &mut self,
        folds: &[Fold],
        shard: usize,
        keys: impl Fn(&Fold) -> &Streams<KeyRef<K>, MAX_SHARDS>,
    ) -> Option<()> {
        let mut count = 0;
        let mut longest = 0;
        for fold in folds {
            let len = keys(fold).len(shard);
            count += len;
            longest = longest.max(len);
        }
        if count < 2 {
            return Some(());
        }
        let stride = longest.next_power_of_two();
        let shift = stride.trailing_zeros();
        if folds.len() > LOCATION_MASK as usize / stride {
            return None;
        }

        // Bit collisions only send extra records to the exact table.
        let words = count.div_ceil(8).next_power_of_two();
        self.seen.resize(words, 0);
        self.seen.fill(0);
        self.repeated.resize(words, 0);
        self.repeated.fill(0);
        for fold in folds {
            for row in keys(fold).records(shard) {
                let hash = row.key.hash();
                let word = (hash as usize >> 6) & (words - 1);
                let bit = 1u64 << (hash & 63);
                self.repeated[word] |= self.seen[word] & bit;
                self.seen[word] |= bit;
            }
        }

        let mask = (count * 2).next_power_of_two() - 1;
        self.winners.resize(mask + 1, 0);
        self.winners.fill(0);
        for (producer, fold) in folds.iter().enumerate() {
            for (index, row) in keys(fold).records(shard).enumerate() {
                let hash = row.key.hash();
                let word = (hash as usize >> 6) & (words - 1);
                if self.repeated[word] & (1u64 << (hash & 63)) == 0 {
                    continue;
                }
                let tag = hash.rotate_left(13) & !LOCATION_MASK;
                let location = ((producer << shift) + index + 1) as u32;
                let mut slot = hash as usize & mask;
                loop {
                    let winner = self.winners[slot];
                    if winner == 0 {
                        self.winners[slot] = tag | location;
                        break;
                    }
                    if winner & !LOCATION_MASK == tag {
                        let prior = ((winner & LOCATION_MASK) - 1) as usize;
                        let owner = &folds[prior >> shift];
                        let old = keys(owner).get(shard, prior & (stride - 1));
                        if row.key.same(&fold.long_ids, old.key, &owner.long_ids) {
                            if row.order < old.order {
                                owner.kill(old.location);
                                self.winners[slot] = tag | location;
                            } else {
                                fold.kill(row.location);
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

pub(super) fn run_events(
    work: &LazyChunks<'static>,
    read_ahead: &ReadAhead,
    users: &Users<'_>,
    input: &EventInput<'_>,
    linked: &[&'static [u8]],
    declined: &AtomicBool,
    seed: u64,
) -> Option<(u32, Vec<&'static [u8]>, Vec<People>)> {
    let avg_bytes = work
        .bufs
        .iter()
        .find(|b| b.len() >= 1 << 16)
        .map_or(80, |b| {
            let sample = &b[..1 << 16];
            (sample.len() / memchr::memchr_iter(b'\n', sample).count().max(1)).max(32)
        });
    let estimate = work.total() / avg_bytes;
    let shards = estimate
        .div_ceil(SHARD_ROWS)
        .next_power_of_two()
        .clamp(1, MAX_SHARDS);
    let reservation = estimate
        .div_ceil(PARTITIONS * rayon::current_num_threads())
        .min(32_768);

    let next = AtomicUsize::new(0);
    let folds = rayon::broadcast(|_| {
        let mut parser = Parser {
            fold: Fold::new(reservation),
            users,
            declined,
            shard_mask: shards - 1,
            ids: (0, 0),
            seed,
            dates: Dates::default(),
            origin: input.start,
            origin_seconds: input.start.div_euclid(1000),
            window_ms: (input.end - input.start) as u64,
            pending: Vec::with_capacity(BATCH),
        };
        while !declined.load(Relaxed) {
            let index = next.fetch_add(1, Relaxed);
            if index >= work.len() {
                break;
            }
            read_ahead.wait(work.raw(index));
            scan_event_rows(work.get(index), &mut parser);
        }
        parser.flush();
        parser.fold
    });
    if declined.load(Relaxed) {
        return None;
    }
    let (_, names) = users.names(linked, declined);
    let local_count = names.len().div_ceil(PARTITIONS) as u32;
    if declined.load(Relaxed) {
        return None;
    }
    lap(&format!(
        "events parsed (v2, {} folds, {} persons)",
        folds.len(),
        names.len()
    ));

    crate::pt_ordered(shards, Dedup::default, |mut dedup, shard| {
        if dedup.shard(&folds, shard, |f| &f.common).is_none()
            || dedup.shard(&folds, shard, |f| &f.wide).is_none()
        {
            declined.store(true, Relaxed);
        }
        dedup
    });
    if declined.load(Relaxed) {
        return None;
    }
    lap("events dedup");

    let people = folds
        .into_par_iter()
        .map(|fold| fold.finish(input))
        .collect();
    lap("events dedup + validate");
    Some((local_count, names, people))
}
