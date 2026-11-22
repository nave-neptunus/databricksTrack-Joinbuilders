use std::mem::MaybeUninit;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Mutex;

use crate::ids::{
    self, Dictionary, FrozenDictionary, Id, Names, PackedDictionary, PackedNames, PackedPeople,
    Shard, SHARDS,
};
use crate::iface::*;
use crate::{
    core, k_ref, parse, setup, sys, Kind, Rec, BB, BSIZE, CBOUND, DAY_MS, G, ING_MAX, K, PBOUND, UB,
};

pub struct Dictionaries {
    pub users: UserNames,
    pub clicks: Names,
}

pub struct UserNames {
    linked: Names,
    short: PackedNames,
    long: Names,
    boundary: u32,
}

impl UserNames {
    pub fn bytes<'a>(&'a self, index: u32, prefix: u8, buf: &'a mut [u8; 9]) -> &'a [u8] {
        if index < self.boundary {
            self.linked.bytes(index, prefix, buf)
        } else {
            let index = index - self.boundary;
            if index & 1 == 0 {
                self.short.bytes(index >> 1, prefix, buf)
            } else {
                self.long.bytes(index >> 1, prefix, buf)
            }
        }
    }
}

struct Users {
    linked: FrozenDictionary,
    short: PackedDictionary,
    long: Dictionary,
    linked_short: [PackedPeople; SHARDS],
    roots: Vec<u32>,
    boundary: u32,
}

impl Users {
    fn new(g: &G) -> Self {
        let (linked, roots) = links(g);
        let boundary = roots.len() as u32;
        let maps: [Mutex<Option<PackedPeople>>; SHARDS] = std::array::from_fn(|_| Mutex::new(None));
        let next = AtomicUsize::new(0);
        sys::run_par(g.nt, |_| loop {
            let stripe = next.fetch_add(1, Ordering::Relaxed);
            if stripe >= SHARDS {
                break;
            }
            *maps[stripe].lock().unwrap() = Some(linked.short_people(stripe, &roots));
        });
        Self {
            linked,
            short: PackedDictionary::new((g.ucap as usize - boundary as usize) / 2),
            long: Dictionary::new(),
            linked_short: maps.map(|m| m.into_inner().unwrap().unwrap()),
            roots,
            boundary,
        }
    }

    #[inline]
    fn person(&self, id: Id<'_>, hash: u64) -> u32 {
        let linked = match id {
            Id::Short(key) => self.linked_short[ids::shard(hash)].get(key),
            Id::Long(_) => self
                .linked
                .lookup(id, hash)
                .map(|index| self.roots[index as usize]),
        };
        linked.unwrap_or_else(|| {
            let index = match id {
                Id::Short(key) => self.short.intern(key, hash) as u64 * 2,
                Id::Long(_) => self.long.intern(id, hash).0 as u64 * 2 + 1,
            };
            u32::try_from(index + self.boundary as u64)
                .unwrap_or_else(|_| sys::fallback("IDs exceed user index capacity"))
        })
    }

    #[inline]
    fn prefetch(&self, id: u64) {
        if id & ID_ODD == 0 {
            let stripe = ids::shard(Id::Short(id).hash());
            self.linked_short[stripe].prefetch(id);
        }
    }

    fn into_names(self) -> UserNames {
        UserNames {
            linked: self.linked.into_names(),
            short: self.short.into_names(),
            long: self.long.into_names(),
            boundary: self.boundary,
        }
    }
}

fn file(g: &G, index: usize) -> &[u8] {
    let f = &g.files[index];
    unsafe { std::slice::from_raw_parts(g.region.add(f.off), f.len) }
}

fn line_start(data: &[u8], from: usize) -> usize {
    if from == 0 || from == data.len() || data[from - 1] == b'\n' {
        return from;
    }
    data[from..]
        .iter()
        .position(|&b| b == b'\n')
        .map_or(data.len(), |i| from + i + 1)
}

fn find(parent: &mut [u32], mut id: u32) -> u32 {
    let mut root = id;
    while parent[root as usize] != root {
        root = parent[root as usize];
    }
    while id != root {
        let next = parent[id as usize];
        parent[id as usize] = root;
        id = next;
    }
    root
}

fn links(g: &G) -> (FrozenDictionary, Vec<u32>) {
    let users = Dictionary::new();
    let chunks: Vec<_> = g
        .queue
        .iter()
        .filter(|c| g.files[c.file as usize].kind == Kind::Lk)
        .collect();
    let edges: Vec<_> = (0..g.nt)
        .map(|_| Mutex::new(Vec::<(u32, u32)>::new()))
        .collect();
    let next = AtomicUsize::new(0);
    sys::run_par(g.nt, |tid| {
        let mut edges = edges[tid].lock().unwrap();
        while let Some(chunk) = chunks.get(next.fetch_add(1, Ordering::Relaxed)) {
            let data = file(g, chunk.file as usize);
            let start = line_start(data, chunk.a);
            let end = line_start(data, chunk.b);
            k_ref::for_each_line(&data[start..end], |a, b| {
                let Some((a, b)) = setup::split2(&data[start + a..start + b]) else {
                    return;
                };
                if a.is_empty() || b.is_empty() {
                    return;
                }
                let a = Id::parse(a, b'u');
                let b = Id::parse(b, b'u');
                edges.push((users.intern(a, a.hash()).0, users.intern(b, b.hash()).0));
            });
        }
        g.t(tid).n_lk_rows = edges.len() as u64;
    });
    let users = users.freeze();
    let mut parent = Vec::new();
    for thread_edges in edges {
        for (a, b) in thread_edges.into_inner().unwrap() {
            let needed = a.max(b) as usize + 1;
            if needed > g.ucap as usize {
                sys::fallback("linked IDs exceed user capacity");
            }
            if needed > parent.len() {
                parent.extend(parent.len() as u32..needed as u32);
            }
            let ra = find(&mut parent, a);
            let rb = find(&mut parent, b);
            parent[ra.max(rb) as usize] = ra.min(rb);
        }
    }
    let mut minimum = vec![u32::MAX; parent.len()];
    for i in 0..parent.len() as u32 {
        let Some(id) = users.id(i) else { continue };
        let root = find(&mut parent, i) as usize;
        let best = minimum[root];
        if best == u32::MAX || id.cmp(users.id(best).unwrap(), b'u').is_lt() {
            minimum[root] = i;
        }
    }
    for i in 0..parent.len() as u32 {
        let root = find(&mut parent, i);
        parent[i as usize] = root;
    }
    for p in &mut parent {
        *p = minimum[*p as usize];
    }
    (users, parent)
}

#[inline]
fn id_at(g: &G, id: u64, base: usize) -> Id<'_> {
    if id & ID_ODD == 0 {
        return Id::Short(id);
    }
    let len = odd_len(id);
    if len == 255 {
        sys::fallback("ID exceeds the fast scanner's length capacity");
    }
    let at = base + odd_offset(id);
    Id::Long(unsafe { std::slice::from_raw_parts(g.region.add(at), len) })
}

const USER_CACHE: usize = 16384;
const EVENT_SHARDS: usize = 1024;
const NO_PAYLOAD: u32 = u32::MAX;

#[inline]
fn event_shard(hash: u64) -> usize {
    (hash >> 32) as usize & (EVENT_SHARDS - 1)
}

struct Scan<'a> {
    g: &'a G,
    users: &'a Users,
    user_keys: [u64; USER_CACHE],
    user_values: [u32; USER_CACHE],
    claims: parse::Part,
    people: PersonEvents,
    worker: u32,
    clicks: [Vec<Click>; SHARDS],
    ev: Box<EvBatch>,
    ck: Box<CkBatch>,
    base: usize,
    n_ev: u64,
    n_ck: u64,
}

#[derive(Clone, Copy)]
struct CopyLocation {
    ingest: u32,
    loc: u32,
}

impl CopyLocation {
    #[inline]
    fn pack(self) -> u64 {
        (self.ingest as u64) << 32 | self.loc as u64
    }

    #[inline]
    fn unpack(word: u64) -> Self {
        Self {
            ingest: (word >> 32) as u32,
            loc: word as u32,
        }
    }
}

struct PersonEvent {
    w0: AtomicU64,
    user: u64,
}

const PERSON_BLOCK_SHIFT: u32 = 9;
const PERSON_BLOCK_ROWS: usize = 1 << PERSON_BLOCK_SHIFT;
const PERSON_WORKER_SHIFT: u32 = 28;
const PERSON_WORKER_MASK: u32 = (1 << PERSON_WORKER_SHIFT) - 1;

struct PersonBlock {
    rows: Box<[MaybeUninit<PersonEvent>]>,
    len: usize,
}

impl PersonBlock {
    fn new() -> Self {
        Self {
            rows: Box::new_uninit_slice(PERSON_BLOCK_ROWS),
            len: 0,
        }
    }

    fn as_slice(&self) -> &[PersonEvent] {
        // Exactly the first len rows are initialized by PersonEvents::push.
        unsafe { std::slice::from_raw_parts(self.rows.as_ptr().cast(), self.len) }
    }
}

struct PersonEvents {
    blocks: Vec<PersonBlock>,
    buckets: Vec<Vec<u32>>,
}

impl PersonEvents {
    fn new(buckets: usize) -> Self {
        Self {
            blocks: Vec::new(),
            buckets: (0..buckets).map(|_| Vec::new()).collect(),
        }
    }

    #[inline]
    fn push(&mut self, worker: u32, stripe: usize, event: PersonEvent) -> u32 {
        let bucket = &mut self.buckets[stripe];
        let block = match bucket.last().copied() {
            Some(block) if self.blocks[block as usize].len < PERSON_BLOCK_ROWS => block,
            _ => {
                let block = self.blocks.len() as u32;
                if block >= (1 << (PERSON_WORKER_SHIFT - PERSON_BLOCK_SHIFT)) {
                    sys::fallback("worker exceeds event locator capacity");
                }
                self.blocks.push(PersonBlock::new());
                bucket.push(block);
                block
            }
        };
        let dst = &mut self.blocks[block as usize];
        let index = dst.len;
        dst.rows[index].write(event);
        dst.len += 1;
        let loc = worker << PERSON_WORKER_SHIFT | block << PERSON_BLOCK_SHIFT | index as u32;
        if loc == NO_PAYLOAD {
            sys::fallback("event locator exceeds capacity");
        }
        loc
    }

    fn bucket(&self, bucket: usize) -> impl Iterator<Item = &[PersonEvent]> {
        self.buckets[bucket]
            .iter()
            .map(|&i| self.blocks[i as usize].as_slice())
    }
}

#[inline]
fn event_at(parsed: &[Parsed], loc: u32) -> Option<&PersonEvent> {
    if loc == NO_PAYLOAD {
        return None;
    }
    let worker = (loc >> PERSON_WORKER_SHIFT) as usize;
    let block = ((loc & PERSON_WORKER_MASK) >> PERSON_BLOCK_SHIFT) as usize;
    let index = loc as usize & (PERSON_BLOCK_ROWS - 1);
    Some(&parsed[worker].people.blocks[block].as_slice()[index])
}

#[derive(Clone, Copy)]
struct Click {
    id: u64,
    key: u64,
    user: u32,
}

struct Parsed {
    claims: FinishedClaims,
    people: PersonEvents,
    clicks: [Mutex<Vec<Click>>; SHARDS],
}

struct FinishedClaims(parse::Part);
// Partition chunks are immutable after finish() publishes the buffered stores.
unsafe impl Sync for FinishedClaims {}

impl Scan<'_> {
    #[inline]
    fn person(&mut self, id: u64) -> u32 {
        let key = id_at(self.g, id, self.base);
        let hash = key.hash();
        if let Id::Short(short) = key {
            let slot = hash as usize & (USER_CACHE - 1);
            if self.user_keys[slot] == short {
                return self.user_values[slot];
            }
            let p = self.intern_person(key, hash);
            self.user_keys[slot] = short;
            self.user_values[slot] = p;
            p
        } else {
            self.intern_person(key, hash)
        }
    }

    #[inline]
    fn intern_person(&self, key: Id<'_>, hash: u64) -> u32 {
        let person = self.users.person(key, hash);
        if person as u64 >= self.g.ucap {
            sys::fallback("IDs exceed user capacity");
        }
        person
    }

    #[inline]
    fn event_row(&mut self, r: EvRow) {
        if r.eid == ID_EMPTY {
            return;
        }
        let g = self.g;
        let ingest = r.ingest.wrapping_sub(g.ing_base) as u64;
        if r.ingest == TS_BAD || ingest >= ING_MAX * 1000 || ingest % 1000 != 0 {
            sys::fallback("ingest_ts outside the fast path's whole-second range");
        }
        let rel = r.ts.wrapping_sub(g.ws) as u64;
        let valid = r.uid != ID_EMPTY
            && rel < g.wlen
            && r.kind <= KIND_PURCHASE
            && r.product != PROD_EMPTY
            && (r.kind != KIND_PURCHASE || r.price != 0)
            && r.ingest.wrapping_sub(r.ts) <= DAY_MS;
        let mut loc = NO_PAYLOAD;
        if valid {
            let user = self.absolute_id(r.uid);
            let stripe = ids::shard(id_at(g, r.uid, self.base).hash());
            let cat = if r.price == 0 {
                0
            } else {
                g.prod[(r.product as usize).min(PBOUND)] as u64
            };
            let w0 = rel << 33 | (r.price as u64) << 9 | cat << 4;
            loc = self.people.push(
                self.worker,
                stripe,
                PersonEvent {
                    w0: AtomicU64::new(w0),
                    user,
                },
            );
        }
        let (id, hash) = if r.eid & ID_ODD == 0 {
            let key = ids::event_key(r.eid);
            (key, key)
        } else {
            (self.absolute_id(r.eid), id_at(g, r.eid, self.base).hash())
        };
        let copy = CopyLocation {
            ingest: (ingest / 1000) as u32,
            loc,
        };
        self.claims.push(event_shard(hash), id, copy.pack());
    }

    #[inline]
    fn click_row(&mut self, r: ClRow) {
        let g = self.g;
        let rel = r.ts.wrapping_sub(g.cbase) as u64;
        if r.cid == ID_EMPTY
            || r.uid == ID_EMPTY
            || rel >= g.clen
            || r.campaign as usize >= CBOUND
            || g.camp[r.campaign as usize] == 255
        {
            return;
        }
        let hash = id_at(g, r.cid, self.base).hash();
        let id = self.absolute_id(r.cid);
        let user = self.person(r.uid);
        let key = rel << 32 | (r.campaign as u64) << 4;
        self.clicks[ids::shard(hash)].push(Click { id, key, user });
    }

    fn absolute_id(&self, id: u64) -> u64 {
        if id & ID_ODD == 0 {
            id
        } else {
            id + ((self.base as u64) << 8)
        }
    }
}

impl EventSink for Scan<'_> {
    fn ev(&mut self) -> *mut EvBatch {
        &mut *self.ev
    }

    fn ev_flush(&mut self) {
        let n = std::mem::take(&mut self.ev.n);
        self.n_ev += n as u64;
        for i in 0..n {
            self.event_row(self.ev.row(i));
        }
    }
}

impl ClickSink for Scan<'_> {
    fn ck(&mut self) -> *mut CkBatch {
        &mut *self.ck
    }

    fn ck_flush(&mut self) {
        let n = std::mem::take(&mut self.ck.n);
        self.n_ck += n as u64;
        const LOOKAHEAD: usize = 12;
        for &id in &self.ck.uid[..n.min(LOOKAHEAD)] {
            self.users.prefetch(id);
        }
        for i in 0..n {
            if i + LOOKAHEAD < n {
                self.users.prefetch(self.ck.uid[i + LOOKAHEAD]);
            }
            self.click_row(self.ck.row(i));
        }
    }
}

fn scan_reserve(g: &G, kind: Kind, stripes: usize) -> usize {
    let bytes: usize = g
        .files
        .iter()
        .filter(|file| file.kind == kind)
        .map(|file| file.len)
        .sum();
    if bytes < 8 << 20 {
        return 0;
    }
    let (index, _) = g
        .files
        .iter()
        .enumerate()
        .find(|(_, file)| file.kind == kind)
        .unwrap();
    let data = file(g, index);
    let sample = &data[..data.len().min(64 << 10)];
    let lines = sample.iter().filter(|&&b| b == b'\n').count().max(1);
    let average = sample.len().div_ceil(lines).max(16);
    (bytes / average).div_ceil(stripes * g.nt) * 5 / 4
}

fn scan(g: &G, users: &Users, arena: &sys::Arena) -> Vec<Parsed> {
    let click_reserve = scan_reserve(g, Kind::Ck, SHARDS);
    let workers: Vec<_> = (0..g.nt).map(|_| Mutex::new(None)).collect();
    let next = AtomicUsize::new(0);
    sys::run_par(g.nt, |tid| {
        let t = g.t(tid);
        if parse::PARTS_READY.load(Ordering::Relaxed) {
            t.ev.set_arena(&g.ev_arena);
            t.us.set_arena(&g.us_arena);
        } else {
            t.ev = parse::Part::new(&g.ev_arena, g.nb);
            t.us = parse::Part::new(&g.us_arena, g.nub);
        }
        if !parse::CORE_READY.load(Ordering::Relaxed) {
            t.core = core::Core::new(g.nc, g.nd, g.row_cap, g.ws_mod, g.day0);
        }
        let mut s = Scan {
            g,
            users,
            user_keys: [0; USER_CACHE],
            user_values: [0; USER_CACHE],
            claims: parse::Part::new(arena, EVENT_SHARDS),
            people: PersonEvents::new(SHARDS),
            worker: tid as u32,
            clicks: std::array::from_fn(|_| Vec::with_capacity(click_reserve)),
            ev: EvBatch::new(),
            ck: CkBatch::new(),
            base: 0,
            n_ev: 0,
            n_ck: 0,
        };
        while let Some(chunk) = g.queue.get(next.fetch_add(1, Ordering::Relaxed)) {
            let f = &g.files[chunk.file as usize];
            if f.kind == Kind::Lk {
                continue;
            }
            let data = file(g, chunk.file as usize);
            let a = line_start(data, chunk.a);
            let b = line_start(data, chunk.b);
            if a == b {
                continue;
            }
            s.base = f.off + a;
            match f.kind {
                Kind::Ev => K::events(&data[a..b], &mut s),
                Kind::Ck => K::clicks(&data[a..b], &mut s),
                Kind::Lk => unreachable!(),
            }
        }
        t.n_ev_rows = s.n_ev;
        t.n_ck_rows = s.n_ck;
        s.claims.finish();
        *workers[tid].lock().unwrap() = Some(Parsed {
            claims: FinishedClaims(s.claims),
            people: s.people,
            clicks: s.clicks.map(Mutex::new),
        });
    });
    workers
        .into_iter()
        .map(|s| s.into_inner().unwrap().unwrap())
        .collect()
}

#[inline]
fn choose(g: &G, winner: &mut CopyLocation, copy: CopyLocation, parsed: &[Parsed]) {
    let loser = if copy.ingest < winner.ingest {
        let loser = winner.loc;
        *winner = copy;
        loser
    } else {
        if copy.ingest == winner.ingest && winner.loc != copy.loc {
            let same = match (event_at(parsed, winner.loc), event_at(parsed, copy.loc)) {
                (Some(a), Some(b)) => {
                    a.w0.load(Ordering::Relaxed) == b.w0.load(Ordering::Relaxed)
                        && id_at(g, a.user, 0).cmp(id_at(g, b.user, 0), b'u').is_eq()
                }
                (None, None) => true,
                _ => false,
            };
            if !same {
                sys::fallback("equal-ingest copies disagree");
            }
        }
        copy.loc
    };
    if let Some(row) = event_at(parsed, loser) {
        row.w0.store(u64::MAX, Ordering::Relaxed);
    }
}

#[derive(Clone, Copy)]
struct DedupSlot {
    key: u64,
    copy: CopyLocation,
}

impl DedupSlot {
    const EMPTY: Self = Self {
        key: 0,
        copy: CopyLocation {
            ingest: 0,
            loc: NO_PAYLOAD,
        },
    };
}

struct EventSlots {
    slots: Vec<DedupSlot>,
}

impl EventSlots {
    fn reset(&mut self, capacity: usize) {
        let len = (2 * capacity).next_power_of_two();
        self.slots.resize(len, DedupSlot::EMPTY);
        self.slots.fill(DedupSlot::EMPTY);
    }

    #[inline]
    fn prefetch(&self, key: u64) {
        if key & ID_ODD != 0 {
            return;
        }
        let slot = key as usize & (self.slots.len() - 1);
        unsafe {
            use std::arch::x86_64::{_mm_prefetch, _MM_HINT_T0};
            _mm_prefetch::<_MM_HINT_T0>(self.slots.as_ptr().add(slot).cast());
        }
    }

    #[inline]
    fn copy(&mut self, g: &G, parsed: &[Parsed], key: u64, copy: CopyLocation) {
        let mask = self.slots.len() - 1;
        let mut i = key as usize & mask;
        loop {
            let slot = &mut self.slots[i];
            if slot.key == 0 {
                *slot = DedupSlot { key, copy };
                return;
            }
            if slot.key == key {
                choose(g, &mut slot.copy, copy, parsed);
                return;
            }
            i = (i + 1) & mask;
        }
    }
}

fn dedup(g: &G, parsed: &[Parsed]) -> Names {
    let click_names: [Mutex<Option<Shard>>; SHARDS] = std::array::from_fn(|_| Mutex::new(None));
    let next = AtomicUsize::new(0);
    sys::run_par(g.nt, |tid| {
        let clicks = &mut g.t(tid).us;
        let mut short = EventSlots { slots: Vec::new() };
        let mut long_slots = Vec::<CopyLocation>::new();
        loop {
            let shard = next.fetch_add(1, Ordering::Relaxed);
            if shard >= EVENT_SHARDS {
                let stripe = shard - EVENT_SHARDS;
                if stripe >= SHARDS {
                    break;
                }
                let keys = dedup_clicks(g, parsed, stripe, clicks);
                *click_names[stripe].lock().unwrap() = Some(keys);
                continue;
            }
            let total: usize = parsed.iter().map(|p| p.claims.0.count(shard)).sum();
            short.reset(total);
            let mut long = Shard::with_capacity(total / 8);
            long_slots.clear();
            long_slots.reserve(total / 8);
            for p in parsed {
                p.claims.0.for_each(shard, |part| {
                    for (i, row) in part.iter().enumerate() {
                        if let Some(next) = part.get(i + 10) {
                            short.prefetch(next.w0);
                        }
                        let copy = CopyLocation::unpack(row.w1);
                        if row.w0 & ID_ODD == 0 {
                            short.copy(g, parsed, row.w0, copy);
                        } else {
                            let id = id_at(g, row.w0, 0);
                            let (index, is_new) = long.intern(id, id.hash());
                            if is_new {
                                long_slots.push(copy);
                            } else {
                                choose(g, &mut long_slots[index as usize], copy, parsed);
                            }
                        }
                    }
                });
            }
        }
        clicks.finish();
    });
    Names::from_shards(click_names.map(|w| w.into_inner().unwrap().unwrap()))
}

fn dedup_clicks(g: &G, parsed: &[Parsed], stripe: usize, clicks: &mut parse::Part) -> Shard {
    let count = parsed
        .iter()
        .map(|p| p.clicks[stripe].lock().unwrap().len())
        .sum();
    let mut keys = Shard::with_capacity(count);
    for p in parsed {
        let part = std::mem::take(&mut *p.clicks[stripe].lock().unwrap());
        for row in part {
            let id = id_at(g, row.id, 0);
            let (local, is_new) = keys.intern(id, id.hash());
            if is_new {
                let click = Names::index(stripe, local);
                let user = row.user as u64;
                let w0 = (user & ((1 << UB) - 1)) << 43 | click as u64;
                clicks.push((user >> UB) as usize, w0, row.key);
            }
        }
    }
    keys
}

// Resolve one ID stripe at a time so its linked-user table stays cache-local.
fn resolve_winners(g: &G, parsed: &[Parsed], users: &Users) -> Vec<Vec<Mutex<Vec<Rec>>>> {
    let next = AtomicUsize::new(0);
    let workers: Vec<_> = (0..g.nt).map(|_| Mutex::new(None)).collect();
    sys::run_par(g.nt, |tid| {
        let mut people: Vec<Vec<Rec>> = (0..g.nub).map(|_| Vec::new()).collect();
        loop {
            let stripe = next.fetch_add(1, Ordering::Relaxed);
            if stripe >= SHARDS {
                break;
            }
            for p in parsed {
                for row in p.people.bucket(stripe).flatten() {
                    let w0 = row.w0.load(Ordering::Relaxed);
                    if w0 == u64::MAX {
                        continue;
                    }
                    let key = id_at(g, row.user, 0);
                    let user = users.person(key, key.hash()) as u64;
                    if user >= g.ucap {
                        sys::fallback("IDs exceed user capacity");
                    }
                    people[(user >> UB) as usize].push(Rec { w0, w1: user });
                }
            }
        }
        *workers[tid].lock().unwrap() = Some(people.into_iter().map(Mutex::new).collect());
    });
    workers
        .into_iter()
        .map(|worker| worker.into_inner().unwrap().unwrap())
        .collect()
}

// Express consumes contiguous runs in event-slot order. Assign those slots
// after dedup so opaque event IDs and shuffled input cannot split a person.
fn order_events(g: &G, people: &[Vec<Mutex<Vec<Rec>>>]) {
    let mut offsets = Vec::with_capacity(g.nub + 1);
    offsets.push(0usize);
    for bucket in 0..g.nub {
        let count: usize = people.iter().map(|p| p[bucket].lock().unwrap().len()).sum();
        offsets.push(offsets[bucket] + count);
    }
    if offsets[g.nub] as u64 > g.ecap {
        sys::fallback("events exceed slot capacity");
    }
    let next = AtomicUsize::new(0);
    sys::run_par(g.nt, |tid| {
        let part = &mut g.t(tid).ev;
        const USERS_PER_BUCKET: usize = 1 << UB;
        let mut rows = Vec::new();
        let mut grouped = Vec::new();
        let mut counts = vec![0; USERS_PER_BUCKET + 1];
        let mut positions = vec![0; USERS_PER_BUCKET];
        loop {
            let bucket = next.fetch_add(1, Ordering::Relaxed);
            if bucket >= g.nub {
                break;
            }
            for p in people {
                let mut input = std::mem::take(&mut *p[bucket].lock().unwrap());
                rows.append(&mut input);
            }
            if rows.is_empty() {
                continue;
            }
            counts.fill(0);
            for row in &rows {
                counts[(row.w1 as usize & (USERS_PER_BUCKET - 1)) + 1] += 1;
            }
            for i in 0..USERS_PER_BUCKET {
                counts[i + 1] += counts[i];
            }
            positions.copy_from_slice(&counts[..USERS_PER_BUCKET]);
            grouped.resize(rows.len(), Rec { w0: 0, w1: 0 });
            for row in rows.drain(..) {
                let pos = &mut positions[row.w1 as usize & (USERS_PER_BUCKET - 1)];
                grouped[*pos] = row;
                *pos += 1;
            }
            for window in counts.windows(2) {
                grouped[window[0]..window[1]].sort_unstable_by_key(|row| row.w0 >> 33);
            }
            for (index, row) in (offsets[bucket]..).zip(grouped.drain(..)) {
                part.push(
                    index >> BB,
                    row.w0,
                    ((index & (BSIZE - 1)) as u64) | row.w1 << 37,
                );
            }
        }
        part.finish();
    });
}

pub fn stage1(g: &mut G) {
    let users = Users::new(g);
    sys::lap("link_ids");
    let claim_arena =
        sys::Arena::new(g.ecap as usize * 16 + g.nt * (EVENT_SHARDS * parse::CHUNK + (2 << 20)));
    let parsed = scan(g, &users, &claim_arena);
    sys::lap("scan_ids");
    let clicks = dedup(g, &parsed);
    sys::run_par(g.nt, |tid| claim_arena.release_part(tid, g.nt));
    sys::lap("dedup_ids");
    let winners = resolve_winners(g, &parsed, &users);
    drop(parsed);
    sys::lap("resolve_users");
    order_events(g, &winners);
    sys::lap("order_events");
    g.ids = Some(Dictionaries {
        users: users.into_names(),
        clicks,
    });
}
