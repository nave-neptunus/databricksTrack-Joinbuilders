use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;

use crate::ids::{self, Dictionary, Id, Names, Shard, SHARDS};
use crate::iface::*;
use crate::{
    core, k_ref, parse, setup, sys, Kind, Rec, BB, BSIZE, CBOUND, DAY_MS, G, ING_MAX, K, PBOUND, UB,
};

pub struct Dictionaries {
    pub users: Names,
    pub clicks: Names,
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

fn links(g: &G, users: &Dictionary) -> Vec<u32> {
    let mut parent = Vec::new();
    let mut names = Vec::<&[u8]>::new();
    let mut n = 0;
    for (i, f) in g.files.iter().enumerate() {
        if f.kind != Kind::Lk {
            continue;
        }
        let data = file(g, i);
        let start = line_start(data, 1);
        k_ref::for_each_line(&data[start..], |a, b| {
            let Some((a, b)) = setup::split2(&data[start + a..start + b]) else {
                return;
            };
            if a.is_empty() || b.is_empty() {
                return;
            }
            let ka = Id::parse(a, b'u');
            let kb = Id::parse(b, b'u');
            let (ia, _) = users.intern(ka, ka.hash());
            let (ib, _) = users.intern(kb, kb.hash());
            let needed = ia.max(ib) as usize + 1;
            if needed > g.ucap as usize {
                sys::fallback("linked IDs exceed user capacity");
            }
            if needed > parent.len() {
                parent.extend(parent.len() as u32..needed as u32);
                names.resize(needed, &[]);
            }
            if names[ia as usize].is_empty() {
                names[ia as usize] = a;
            }
            if names[ib as usize].is_empty() {
                names[ib as usize] = b;
            }
            let ra = find(&mut parent, ia);
            let rb = find(&mut parent, ib);
            if names[ra as usize] < names[rb as usize] {
                parent[rb as usize] = ra;
            } else {
                parent[ra as usize] = rb;
            }
            n += 1;
        });
    }
    for i in 0..parent.len() {
        find(&mut parent, i as u32);
    }
    g.t(0).n_lk_rows = n;
    parent
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

const USER_CACHE: usize = 4096;

struct Scan<'a> {
    g: &'a G,
    users: &'a Dictionary,
    clicks: &'a Dictionary,
    roots: &'a [u32],
    user_keys: [u64; USER_CACHE],
    user_values: [u32; USER_CACHE],
    events: [Vec<Event>; SHARDS],
    us: &'a mut parse::Part,
    ev: Box<EvBatch>,
    ck: Box<CkBatch>,
    base: usize,
    n_ev: u64,
    n_ck: u64,
}

#[derive(Clone, Copy)]
struct Event {
    // Long IDs have their exact mmap offset and byte count in the scanner token.
    id: u64,
    w0: u64,
    user_ingest: u64,
}

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
        let (index, _) = self.users.intern(key, hash);
        let person = self.roots.get(index as usize).copied().unwrap_or(index);
        if person as u64 >= self.g.ucap {
            sys::fallback("IDs exceed user capacity");
        }
        person
    }

    #[inline]
    fn event_row(&mut self, r: EvRow) {
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
        let (mut w0, mut user) = (u64::MAX, 0);
        if valid {
            user = self.person(r.uid);
            let cat = if r.price == 0 {
                0
            } else {
                g.prod[(r.product as usize).min(PBOUND)] as u64
            };
            w0 = rel << 33 | (r.price as u64) << 9 | cat << 4;
        }
        let hash = id_at(g, r.eid, self.base).hash();
        let id = if r.eid & ID_ODD == 0 {
            r.eid
        } else {
            r.eid + ((self.base as u64) << 8)
        };
        self.events[ids::shard(hash)].push(Event {
            id,
            w0,
            user_ingest: (user as u64) << 21 | ingest / 1000,
        });
    }

    #[inline]
    fn click_row(&mut self, r: ClRow) {
        let g = self.g;
        let rel = r.ts.wrapping_sub(g.cbase) as u64;
        if r.uid == ID_EMPTY
            || rel >= g.clen
            || r.campaign as usize >= CBOUND
            || g.camp[r.campaign as usize] == 255
        {
            return;
        }
        let key = id_at(g, r.cid, self.base);
        let (click, is_new) = self.clicks.intern(key, key.hash());
        if is_new {
            let user = self.person(r.uid) as u64;
            let w0 = (user & ((1 << UB) - 1)) << 43 | click as u64;
            let w1 = rel << 32 | (r.campaign as u64) << 4;
            self.us.push((user >> UB) as usize, w0, w1);
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
        for i in 0..n {
            self.click_row(self.ck.row(i));
        }
    }
}

fn scan(
    g: &G,
    users: &Dictionary,
    clicks: &Dictionary,
    roots: &[u32],
) -> Vec<[Mutex<Vec<Event>>; SHARDS]> {
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
            clicks,
            roots,
            user_keys: [0; USER_CACHE],
            user_values: [0; USER_CACHE],
            events: std::array::from_fn(|_| Vec::new()),
            us: &mut t.us,
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
        s.us.finish();
        t.n_ev_rows = s.n_ev;
        t.n_ck_rows = s.n_ck;
        *workers[tid].lock().unwrap() = Some(s.events.map(Mutex::new));
    });
    workers
        .into_iter()
        .map(|s| s.into_inner().unwrap().unwrap())
        .collect()
}

fn dedup(g: &G, parsed: &[[Mutex<Vec<Event>>; SHARDS]]) -> Vec<Vec<Mutex<Vec<Rec>>>> {
    let next = AtomicUsize::new(0);
    let workers: Vec<_> = (0..g.nt).map(|_| Mutex::new(None)).collect();
    sys::run_par(g.nt, |tid| {
        let mut people: Vec<Vec<Rec>> = (0..g.nub).map(|_| Vec::new()).collect();
        loop {
            let shard = next.fetch_add(1, Ordering::Relaxed);
            if shard >= SHARDS {
                break;
            }
            let total = parsed.iter().map(|p| p[shard].lock().unwrap().len()).sum();
            let mut keys = Shard::with_capacity(total);
            let mut slots = Vec::<Event>::with_capacity(total);
            for p in parsed {
                let part = std::mem::take(&mut *p[shard].lock().unwrap());
                for row in part {
                    let id = id_at(g, row.id, 0);
                    let (index, is_new) = keys.intern(id, id.hash());
                    if is_new {
                        slots.push(row);
                    } else {
                        let slot = &mut slots[index as usize];
                        let old_ing = slot.user_ingest & ING_MAX;
                        let new_ing = row.user_ingest & ING_MAX;
                        if new_ing < old_ing {
                            *slot = row;
                        } else if new_ing == old_ing
                            && (slot.w0 != row.w0 || slot.user_ingest != row.user_ingest)
                        {
                            sys::fallback("equal-ingest copies disagree");
                        }
                    }
                }
            }
            for e in slots {
                if e.w0 != u64::MAX {
                    let u = e.user_ingest >> 21;
                    people[(u >> UB) as usize].push(Rec { w0: e.w0, w1: u });
                }
            }
        }
        *workers[tid].lock().unwrap() = Some(people.into_iter().map(Mutex::new).collect());
    });
    workers
        .into_iter()
        .map(|w| w.into_inner().unwrap().unwrap())
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
        let mut rows = Vec::new();
        loop {
            let bucket = next.fetch_add(1, Ordering::Relaxed);
            if bucket >= g.nub {
                break;
            }
            for p in people {
                let mut source = std::mem::take(&mut *p[bucket].lock().unwrap());
                rows.append(&mut source);
            }
            rows.sort_unstable_by_key(|r| (r.w1, r.w0 >> 33));
            for (index, row) in (offsets[bucket]..).zip(rows.drain(..)) {
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
    let users = Dictionary::new();
    let clicks = Dictionary::new();
    let roots = links(g, &users);
    sys::lap("link_ids");
    let parsed = scan(g, &users, &clicks, &roots);
    sys::lap("scan_ids");
    let people = dedup(g, &parsed);
    sys::lap("dedup_ids");
    order_events(g, &people);
    sys::lap("order_events");
    g.ids = Some(Dictionaries {
        users: users.into_names(),
        clicks: clicks.into_names(),
    });
}
