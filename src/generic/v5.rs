//! The v5 path: exact on data whose IDs are opaque (keyed-bijection base-36 or 32-hex).
//!
//! No generator math. Every ID is reduced to a 64-bit key as parsed by the SIMD kernels:
//!   * short canonical IDs (prefix + 1..=8 lowercase base-36 digits, no leading zero) are
//!     their own key: the digits packed as an integer (equal key <=> equal bytes);
//!   * everything else (32-hex, upper-case, odd forms) is keyed by a 63-bit byte hash with
//!     the top bit set, plus a location (pointer|len<<48) so any key match is verified on the
//!     FULL bytes before two IDs are treated as equal.
//! Events are deduplicated per hash-shard (earliest ingest wins, then file order), routed to
//! person shards (people are components of the link graph; a person's name is the
//! byte-wise smallest ID in it), and sessionised with the same rules as the generic path.
//! Anything unexpected declines before any output is written (the caller then hands the
//! whole job to /fallback).

use super::core::{parse_links, Acc};
use super::parse::{chunk_target, chunks, day_of, manifest, split, B, DAY_MS};
use super::util::{hash, map_dir, new_map, par_run, Map, Mapped, Tab};
use super::core::UserRow;
use super::{clear_parquet, write_campaigns, write_users};
use crate::iface::*;
use std::cmp::Ordering as Cmp;
use std::io;
use std::path::Path;
use std::sync::atomic::{AtomicU8, AtomicUsize, Ordering};
use std::sync::Mutex;

const ES: usize = 256;
const PS: usize = 256;
const ODD: u64 = 1 << 63;
const LINKED: u64 = 1 << 62;
const GAP_MS: i64 = 1_800_000;
const WEEK_MS: i64 = 7 * 86_400_000;
const T_PURCHASE: u32 = 2;

pub enum Decline {
    Why(&'static str),
    Io(io::Error),
}
impl From<io::Error> for Decline {
    fn from(e: io::Error) -> Self {
        Decline::Io(e)
    }
}
type R<T> = Result<T, Decline>;
fn why<T>(s: &'static str) -> R<T> {
    Err(Decline::Why(s))
}

#[inline(always)]
fn mixk(k: u64) -> u64 {
    let x = (k ^ (k >> 29)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    (x ^ (x >> 32)).wrapping_mul(0x94D0_49BB_1331_11EB)
}
#[inline(always)]
fn shard_h(h: u64) -> usize {
    (h >> 56) as usize
}

// ---------- IDs ----------

#[inline(always)]
fn loc_of(p: *const u8, len: usize) -> u64 {
    (p as u64) | ((len as u64) << 48)
}
#[inline(always)]
unsafe fn loc_bytes(loc: u64) -> &'static [u8] {
    std::slice::from_raw_parts((loc & 0xFFFF_FFFF_FFFF) as *const u8, (loc >> 48) as usize)
}
/// Equal IDs <=> equal keys, and for hashed keys also equal bytes.
#[inline(always)]
fn id_eq(ka: u64, la: u64, kb: u64, lb: u64) -> bool {
    ka == kb && (ka & ODD == 0 || unsafe { loc_bytes(la) == loc_bytes(lb) })
}

#[inline(always)]
fn okey(h: u64) -> u64 {
    let k = h | ODD;
    if k == u64::MAX {
        k - 1
    } else {
        k
    }
}

/// Turns a kernel ID into (key, loc). `base` is the start of the buffer the kernel parsed.
#[inline(always)]
fn resolve(id: u64, base: *const u8) -> Option<(u64, u64)> {
    if id & ID_ODD == 0 {
        return Some((id, 0));
    }
    let len = odd_len(id);
    if len >= 255 {
        return None;
    }
    let p = unsafe { base.add(odd_offset(id)) };
    let h = hash(unsafe { std::slice::from_raw_parts(p, len) });
    Some((okey(h), loc_of(p, len)))
}

fn id_bytes(key: u64, loc: u64, prefix: u8, out: &mut Vec<u8>) {
    out.clear();
    if key & ODD != 0 {
        out.extend_from_slice(unsafe { loc_bytes(loc) });
    } else {
        out.push(prefix);
        crate::keyrec::base36_encode(key, out);
    }
}

/// Byte-wise order of two IDs (same prefix).
fn id_cmp(a: (u64, u64), b: (u64, u64), prefix: u8) -> Cmp {
    if a.0 & ODD == 0 && b.0 & ODD == 0 {
        return crate::enc36(a.0).0.cmp(&crate::enc36(b.0).0);
    }
    let (mut x, mut y) = (Vec::new(), Vec::new());
    id_bytes(a.0, a.1, prefix, &mut x);
    id_bytes(b.0, b.1, prefix, &mut y);
    x.cmp(&y)
}

struct Arena {
    cur: *mut u8,
    left: usize,
}
unsafe impl Send for Arena {}
impl Arena {
    fn new() -> Arena {
        Arena { cur: std::ptr::null_mut(), left: 0 }
    }
    fn put(&mut self, b: &[u8]) -> &'static [u8] {
        if b.len() > self.left {
            let n = (1usize << 20).max(b.len());
            let blk: &'static mut [u8] = Box::leak(vec![0u8; n].into_boxed_slice());
            self.cur = blk.as_mut_ptr();
            self.left = n;
        }
        let p = self.cur;
        unsafe {
            std::ptr::copy_nonoverlapping(b.as_ptr(), p, b.len());
            self.cur = p.add(b.len());
            self.left -= b.len();
            std::slice::from_raw_parts(p, b.len())
        }
    }
}

// ---------- links ----------

#[derive(Clone, Copy)]
#[repr(C)]
struct Slot {
    key: u64,
    node: u32,
    root: u32,
}
const EMPTY: u64 = u64::MAX;

struct LinkTab {
    slots: Vec<Slot>,
    mask: usize,
    nkey: Vec<u64>,
    nloc: Vec<u64>,
    root: Vec<u32>,
    name: Vec<u32>, // per node: node index of its component's smallest ID (valid at roots)
}

fn uf_find(parent: &mut [u32], mut x: u32) -> u32 {
    let mut r = x;
    while parent[r as usize] != r {
        r = parent[r as usize];
    }
    while parent[x as usize] != r {
        let n = parent[x as usize];
        parent[x as usize] = r;
        x = n;
    }
    r
}

impl LinkTab {
    #[inline(always)]
    fn slot_of(&self, key: u64) -> usize {
        (mixk(key) as usize) & self.mask
    }
    #[inline(always)]
    fn prefetch(&self, key: u64) {
        unsafe { core::arch::x86_64::_mm_prefetch(self.slots.as_ptr().add(self.slot_of(key)) as *const i8, core::arch::x86_64::_MM_HINT_T0) };
    }
    /// The root node of the person this ID belongs to, if the ID is in any link.
    #[inline(always)]
    fn find_root(&self, key: u64, loc: u64) -> Option<u32> {
        let mut i = self.slot_of(key);
        loop {
            let s = unsafe { *self.slots.get_unchecked(i) };
            if s.key == EMPTY {
                return None;
            }
            if s.key == key && (key & ODD == 0 || unsafe { loc_bytes(self.nloc[s.node as usize]) == loc_bytes(loc) }) {
                return Some(s.root);
            }
            i = (i + 1) & self.mask;
        }
    }
    fn find(&self, key: u64, loc: u64) -> Option<u32> {
        let mut i = self.slot_of(key);
        loop {
            let s = self.slots[i];
            if s.key == EMPTY {
                return None;
            }
            if s.key == key && id_eq(self.nkey[s.node as usize], self.nloc[s.node as usize], key, loc) {
                return Some(s.node);
            }
            i = (i + 1) & self.mask;
        }
    }
}

fn classify(b: B, h: u64) -> R<(u64, u64)> {
    if b.len() >= 255 {
        return why("link id too long");
    }
    let v = crate::k_ref::parse_id(b, (0, b.len()), b'u');
    if v & ID_ODD == 0 {
        Ok((v, 0))
    } else {
        Ok((okey(h), loc_of(b.as_ptr(), b.len())))
    }
}

fn build_links(parts: &[Vec<(B, u64, B, u64)>]) -> R<LinkTab> {
    let n: usize = parts.iter().map(|p| p.len()).sum();
    let cap = (n * 3 + 16).next_power_of_two();
    let mask = cap - 1;
    let mut slots: Vec<Slot> = vec![Slot { key: EMPTY, node: 0, root: 0 }; cap];
    let mut nkey: Vec<u64> = Vec::with_capacity(n + n / 2);
    let mut nloc: Vec<u64> = Vec::with_capacity(n + n / 2);
    let mut parent: Vec<u32> = Vec::with_capacity(n + n / 2);
    for p in parts {
        for &(a, ha, b, hb) in p {
            let mut ids = [0u32; 2];
            for (k, (s, h)) in [(a, ha), (b, hb)].into_iter().enumerate() {
                let (key, loc) = classify(s, h)?;
                let mut i = (mixk(key) as usize) & mask;
                ids[k] = loop {
                    let sl = slots[i];
                    if sl.key == EMPTY {
                        if nkey.len() * 10 > cap * 8 {
                            return why("link table full");
                        }
                        let id = nkey.len() as u32;
                        nkey.push(key);
                        nloc.push(loc);
                        parent.push(id);
                        slots[i] = Slot { key, node: id, root: 0 };
                        break id;
                    }
                    if sl.key == key && id_eq(sl.key, nloc[sl.node as usize], key, loc) {
                        break sl.node;
                    }
                    i = (i + 1) & mask;
                };
            }
            let rx = uf_find(&mut parent, ids[0]);
            let ry = uf_find(&mut parent, ids[1]);
            if rx != ry {
                if rx < ry {
                    parent[ry as usize] = rx
                } else {
                    parent[rx as usize] = ry
                }
            }
        }
    }
    let m = nkey.len();
    let mut root = vec![0u32; m];
    let mut name: Vec<u32> = (0..m as u32).collect();
    for i in 0..m as u32 {
        let r = uf_find(&mut parent, i);
        root[i as usize] = r;
        let cur = name[r as usize];
        if id_cmp((nkey[i as usize], nloc[i as usize]), (nkey[cur as usize], nloc[cur as usize]), b'u') == Cmp::Less {
            name[r as usize] = i;
        }
    }
    for sl in slots.iter_mut() {
        if sl.key != EMPTY {
            sl.root = root[sl.node as usize];
        }
    }
    Ok(LinkTab { slots, mask, nkey, nloc, root, name })
}

// ---------- records ----------

#[derive(Clone, Copy)]
struct Claim {
    key: u64,
    loc: u64,
    ing: i64,
    ord: u32,
    seq: u32,
}
#[derive(Clone, Copy)]
struct Pay {
    pkey: u64,
    ploc: u64,
    ts: u32,
    pp: u32, // price | kind << 24
    seq: u32,
    cat: u32,
}
#[derive(Clone, Copy)]
struct CPay {
    pkey: u64,
    ploc: u64,
    ckey: u64,
    cloc: u64,
    ts: u32,
    camp: u32,
}

const SEG: usize = 1024;
/// Append-only list in fixed segments: no copying on growth.
struct Seg<T> {
    segs: Vec<Vec<T>>,
    n: usize,
}
impl<T: Copy> Seg<T> {
    fn new() -> Self {
        Seg { segs: Vec::new(), n: 0 }
    }
    #[inline(always)]
    fn push(&mut self, v: T) {
        self.n += 1;
        if let Some(s) = self.segs.last_mut() {
            if s.len() < SEG {
                s.push(v);
                return;
            }
        }
        let mut s = Vec::with_capacity(SEG);
        s.push(v);
        self.segs.push(s);
    }
    fn len(&self) -> usize {
        self.n
    }
    fn iter(&self) -> impl Iterator<Item = &T> {
        self.segs.iter().flatten()
    }
}

struct TS {
    claims: Vec<Seg<Claim>>,
    pays: Vec<Seg<Pay>>,
    cpays: Vec<Seg<CPay>>,
    seq: u32,
}
impl TS {
    fn new() -> TS {
        TS { claims: (0..ES).map(|_| Seg::new()).collect(), pays: (0..PS).map(|_| Seg::new()).collect(), cpays: (0..PS).map(|_| Seg::new()).collect(), seq: 0 }
    }
}

struct Ctx<'a> {
    ws: i64,
    we: i64,
    cat_of: &'a [u32],
    unknown_cat: u32,
    camp_of: &'a [u32],
    links: &'a LinkTab,
    mode: u32,
}

struct Sink<'a, 'b> {
    c: &'a Ctx<'b>,
    st: &'a mut TS,
    eb: Box<EvBatch>,
    cb: Box<CkBatch>,
    base: *const u8,
    chunk: u32,
    ridx: u32,
    bad: Option<&'static str>,
}

#[inline(always)]
fn pf<T>(p: *const T) {
    unsafe { core::arch::x86_64::_mm_prefetch(p as *const i8, core::arch::x86_64::_MM_HINT_T0) };
}

impl<'a, 'b> Sink<'a, 'b> {
    /// Person key of an already resolved user ID.
    #[inline(always)]
    fn person(&self, key: u64, loc: u64) -> (u64, u64) {
        if self.c.mode == 3 {
            return (key, loc);
        }
        match self.c.links.find_root(key, loc) {
            Some(r) => (LINKED | r as u64, 0),
            None => (key, loc),
        }
    }

    #[inline(always)]
    fn res(&mut self, id: u64) -> (u64, u64) {
        match resolve(id, self.base) {
            Some(x) => x,
            None => {
                self.bad = Some("id too long");
                (0, 0)
            }
        }
    }

    fn ev_batch(&mut self) {
        let n = self.eb.n;
        self.eb.n = 0;
        if self.c.mode == 1 || n == 0 {
            return;
        }
        let mut ek = [(0u64, 0u64); NB];
        let mut uk = [(0u64, 0u64); NB];
        for i in 0..n {
            let eid = self.eb.eid[i];
            let uid = self.eb.uid[i];
            if eid != ID_EMPTY {
                ek[i] = self.res(eid);
            }
            if uid != ID_EMPTY {
                uk[i] = self.res(uid);
                self.c.links.prefetch(uk[i].0);
            }
        }
        for i in 0..n {
            let r = self.eb.row(i);
            self.ev_row(&r, ek[i], uk[i]);
        }
    }

    #[inline(always)]
    fn ev_row(&mut self, r: &EvRow, (ekey, eloc): (u64, u64), (ukey, uloc): (u64, u64)) {
        if r.eid == ID_EMPTY {
            return;
        }
        if r.ingest == TS_BAD {
            self.bad = Some("ingest not F1");
            return;
        }
        if self.ridx >= (1 << 20) {
            self.bad = Some("too many rows in a chunk");
            return;
        }
        let ord = self.chunk << 20 | self.ridx;
        self.ridx += 1;
        let seq = self.st.seq;
        self.st.seq += 1;
        let c = self.c;
        let rel = r.ts.wrapping_sub(c.ws);
        let valid = r.uid != ID_EMPTY
            && r.product != PROD_EMPTY
            && r.ts >= c.ws
            && r.ts < c.we
            && r.kind <= KIND_PURCHASE
            && (r.kind != KIND_PURCHASE || r.price != 0)
            && r.ingest.wrapping_sub(r.ts) <= DAY_MS;
        let h = mixk(ekey);
        self.st.claims[shard_h(h)].push(Claim { key: ekey, loc: eloc, ing: r.ingest, ord, seq });
        if !valid {
            return;
        }
        let mut cat = 0u32;
        let mut price = 0u32;
        if r.kind == KIND_PURCHASE {
            price = r.price;
            cat = if (r.product as usize) < c.cat_of.len() { c.cat_of[r.product as usize] } else { c.unknown_cat };
        }
        if c.mode == 2 {
            return;
        }
        let (pkey, ploc) = self.person(ukey, uloc);
        let p = shard_h(mixk(pkey));
        self.st.pays[p].push(Pay { pkey, ploc, ts: rel as u32, pp: price | (r.kind as u32) << 24, seq, cat });
    }

    fn ck_batch(&mut self) {
        let n = self.cb.n;
        self.cb.n = 0;
        if self.c.mode == 1 || n == 0 {
            return;
        }
        let mut uk = [(0u64, 0u64); NB];
        for i in 0..n {
            let uid = self.cb.uid[i];
            if uid != ID_EMPTY {
                uk[i] = self.res(uid);
                self.c.links.prefetch(uk[i].0);
            }
        }
        for i in 0..n {
            let r = self.cb.row(i);
            self.ck_row(&r, uk[i]);
        }
    }

    #[inline(always)]
    fn ck_row(&mut self, r: &ClRow, (ukey, uloc): (u64, u64)) {
        let c = self.c;
        if r.uid == ID_EMPTY || r.cid == ID_EMPTY {
            return;
        }
        let lo = c.ws - WEEK_MS;
        if r.ts < lo || r.ts >= c.we {
            return;
        }
        let camp = if (r.campaign as usize) < c.camp_of.len() { c.camp_of[r.campaign as usize] } else { u32::MAX };
        if camp == u32::MAX {
            return;
        }
        let (ckey, cloc) = self.res(r.cid);
        let (pkey, ploc) = self.person(ukey, uloc);
        let p = shard_h(mixk(pkey));
        self.st.cpays[p].push(CPay { pkey, ploc, ckey, cloc, ts: (r.ts - lo) as u32, camp });
    }
}

impl<'a, 'b> EventSink for Sink<'a, 'b> {
    fn ev(&mut self) -> *mut EvBatch {
        &mut *self.eb
    }
    fn ev_flush(&mut self) {
        self.ev_batch();
    }
}
impl<'a, 'b> ClickSink for Sink<'a, 'b> {
    fn ck(&mut self) -> *mut CkBatch {
        &mut *self.cb
    }
    fn ck_flush(&mut self) {
        self.ck_batch();
    }
}

// ---------- sessions (same rules as the generic path) ----------

#[derive(Clone, Copy)]
struct PE {
    ts: i64,
    pt: u32,
    cat: u32,
}
#[derive(Clone, Copy)]
struct PC {
    ts: i64,
    ckey: u64,
    cloc: u64,
    camp: u32,
}
struct Sess {
    start: i64,
    day: i32,
    rev: u64,
    purch: bool,
}
struct DayAcc {
    day: i32,
    sessions: u64,
    events: u64,
    revenue: u64,
}
struct Scratch {
    sess: Vec<Sess>,
    starts: Vec<i64>,
    days: Vec<i32>,
    day_end: Vec<u32>,
    catsum: Vec<u64>,
    touched: Vec<u32>,
    ct: Vec<(u32, i64)>,
    rs: Vec<u32>,
}
impl Scratch {
    fn new(ncat: usize) -> Self {
        Scratch { sess: vec![], starts: vec![], days: vec![], day_end: vec![], catsum: vec![0; ncat], touched: vec![], ct: vec![], rs: vec![] }
    }
}

fn flush_row(name: &'static [u8], d: &DayAcc, sc: &mut Scratch, rows: &mut Vec<UserRow<'static>>) {
    let mut top: Option<u32> = None;
    let mut best = 0u64;
    for &c in &sc.touched {
        let v = sc.catsum[c as usize];
        if top.is_none() || v > best || (v == best && c < top.unwrap()) {
            top = Some(c);
            best = v;
        }
        sc.catsum[c as usize] = 0;
    }
    sc.touched.clear();
    rows.push(UserRow { name, day: d.day, sessions: d.sessions, events: d.events, revenue: d.revenue, top });
}

#[inline]
fn ckey(camp: u32, day: i32) -> u64 {
    ((camp as u64) << 32) | (day as u32 as u64)
}

fn click_cmp(a: &PC, b: &PC) -> Cmp {
    a.ts.cmp(&b.ts).then_with(|| id_cmp((a.ckey, a.cloc), (b.ckey, b.cloc), b'k'))
}

fn person(name: &'static [u8], evs: &mut [PE], cks: &mut [PC], sc: &mut Scratch, rows: &mut Vec<UserRow<'static>>, organic: u32, acc: &mut Acc) {
    if evs.is_empty() {
        return;
    }
    evs.sort_unstable_by_key(|e| e.ts);
    sc.sess.clear();
    let mut d = DayAcc { day: day_of(evs[0].ts), sessions: 0, events: 0, revenue: 0 };
    let mut prev = evs[0].ts;
    for (i, e) in evs.iter().enumerate() {
        if i == 0 || e.ts - prev > GAP_MS {
            let day = day_of(e.ts);
            if day != d.day {
                flush_row(name, &d, sc, rows);
                d = DayAcc { day, sessions: 0, events: 0, revenue: 0 };
            }
            d.sessions += 1;
            sc.sess.push(Sess { start: e.ts, day, rev: 0, purch: false });
        }
        prev = e.ts;
        d.events += 1;
        if (e.pt >> 24) == T_PURCHASE {
            let price = (e.pt & 0x00ff_ffff) as u64;
            let s = sc.sess.last_mut().unwrap();
            s.rev += price;
            s.purch = true;
            d.revenue += price;
            let c = e.cat as usize;
            if sc.catsum[c] == 0 {
                sc.touched.push(e.cat);
            }
            sc.catsum[c] += price;
        }
    }
    flush_row(name, &d, sc, rows);

    if !cks.is_empty() {
        cks.sort_unstable_by(click_cmp);
    }
    credit(&sc.sess, cks, &mut sc.rs, organic, acc);
    if !cks.is_empty() {
        sc.starts.clear();
        sc.days.clear();
        sc.starts.extend(sc.sess.iter().map(|s| s.start));
        sc.days.extend(sc.sess.iter().map(|s| s.day));
        let n = sc.sess.len();
        sc.day_end.clear();
        sc.day_end.resize(n, 0);
        let mut end = n as u32;
        for i in (0..n).rev() {
            if i + 1 < n && sc.days[i + 1] != sc.days[i] {
                end = (i + 1) as u32;
            }
            sc.day_end[i] = end;
        }
        assists(&sc.starts, &sc.days, &sc.day_end, cks, &mut sc.ct, acc);
    }
}

fn credit(sess: &[Sess], cks: &[PC], rs: &mut Vec<u32>, organic: u32, acc: &mut Acc) {
    rs.clear();
    for i in 0..cks.len() {
        if i > 0 && cks[i].ts == cks[i - 1].ts {
            let r = rs[i - 1];
            rs.push(r);
        } else {
            rs.push(i as u32);
        }
    }
    let n = cks.len();
    let (mut lo, mut hi) = (0usize, 0usize);
    for s in sess {
        while hi < n && cks[hi].ts <= s.start {
            hi += 1;
        }
        while lo < hi && cks[lo].ts < s.start - WEEK_MS {
            lo += 1;
        }
        let camp = if hi > lo { cks[rs[hi - 1] as usize].camp } else { organic };
        let a = acc.entry(ckey(camp, s.day)).or_insert([0; 4]);
        a[0] += 1;
        a[1] += s.purch as u64;
        a[2] += s.rev;
    }
}

fn assists(starts: &[i64], days: &[i32], day_end: &[u32], cks: &[PC], ct: &mut Vec<(u32, i64)>, acc: &mut Acc) {
    ct.clear();
    ct.extend(cks.iter().map(|c| (c.camp, c.ts)));
    ct.sort_unstable();
    let n = ct.len();
    let mut flush = |camp: u32, a: i64, b: i64| {
        let lo = starts.partition_point(|&s| s < a);
        let hi = starts.partition_point(|&s| s <= b);
        let mut k = lo;
        while k < hi {
            let e = (day_end[k] as usize).min(hi);
            acc.entry(ckey(camp, days[k])).or_insert([0; 4])[3] += (e - k) as u64;
            k = e;
        }
    };
    let mut i = 0;
    while i < n {
        let camp = ct[i].0;
        let a = ct[i].1;
        let mut b = a + WEEK_MS;
        i += 1;
        while i < n && ct[i].0 == camp && ct[i].1 <= b {
            b = b.max(ct[i].1 + WEEK_MS);
            i += 1;
        }
        flush(camp, a, b);
    }
}

// ---------- driver ----------

fn canon_num(d: &[u8]) -> Option<u32> {
    if !(1..=8).contains(&d.len()) || !d.iter().all(|b| b.is_ascii_digit()) || (d[0] == b'0' && d.len() > 1) {
        return None;
    }
    Some(d.iter().fold(0u32, |a, &b| a * 10 + (b - b'0') as u32))
}

extern "C" {
    fn madvise(addr: *mut u8, len: usize, advice: i32) -> i32;
}

fn lap(log: bool, last: &mut std::time::Instant, t0: std::time::Instant, what: &str) {
    if log {
        let now = std::time::Instant::now();
        eprintln!("[v5] {what:<12} {:>8.1} ms (total {:>8.1} ms)", (now - *last).as_secs_f64() * 1e3, (now - t0).as_secs_f64() * 1e3);
        *last = now;
    }
}

pub fn run(data_dir: &Path, out_dir: &Path, ncpu: usize) -> R<()> {
    let ncpu = ncpu.max(1);
    let log = std::env::var_os("V1ND_LOG").is_some();
    let t0 = std::time::Instant::now();
    let mut last = t0;
    let man = Mapped::open(&data_dir.join("manifest.json"))?;
    let prodm = Mapped::open(&data_dir.join("products.csv"))?;
    let campm = Mapped::open(&data_dir.join("campaigns.csv"))?;
    let evm = map_dir(&data_dir.join("events"))?;
    let ckm = map_dir(&data_dir.join("clicks"))?;
    let lkm = map_dir(&data_dir.join("links"))?;
    let (ws, we) = manifest(man.bytes())?;
    if we <= ws || we - ws > (1 << 31) {
        return why("window");
    }

    // products: canonical ids only; first row wins; unknown category ranks as "unknown"
    let mut prod_rows: Vec<(B, B)> = Vec::new();
    for c in chunks(&[prodm.bytes()], usize::MAX / 2) {
        super::parse::for_lines(c, |l| {
            if let Some([p, cat]) = split::<2>(l) {
                prod_rows.push((p, cat));
            }
        });
    }
    let mut cat_names: Vec<B> = prod_rows.iter().map(|r| r.1).collect();
    cat_names.push(b"unknown");
    cat_names.sort_unstable();
    cat_names.dedup();
    let mut cat_rank: Map<B, u32> = new_map(cat_names.len());
    for (i, &c) in cat_names.iter().enumerate() {
        cat_rank.insert(c, i as u32);
    }
    let unknown_cat = cat_rank[&b"unknown"[..]];
    let mut cat_of: Vec<u32> = Vec::new();
    let mut seen: Vec<bool> = Vec::new();
    for &(p, c) in &prod_rows {
        let n = match p.first() {
            Some(b'p') => canon_num(&p[1..]),
            _ => None,
        };
        let Some(n) = n else { return why("non-canonical product id") };
        if n >= (1 << 22) {
            return why("product number too large");
        }
        let n = n as usize;
        if cat_of.len() <= n {
            cat_of.resize(n + 1, unknown_cat);
            seen.resize(n + 1, false);
        }
        if !seen[n] {
            seen[n] = true;
            cat_of[n] = cat_rank[c];
        }
    }
    drop(prod_rows);
    // campaigns
    let mut camp_ids: Vec<B> = Vec::new();
    let mut channels: Vec<B> = Vec::new();
    let mut camp_of: Vec<u32> = Vec::new();
    for c in chunks(&[campm.bytes()], usize::MAX / 2) {
        let mut bad = false;
        super::parse::for_lines(c, |l| {
            if let Some([id, ch]) = split::<2>(l) {
                let n = if id.starts_with(b"c_") { canon_num(&id[2..]) } else { None };
                let Some(n) = n.filter(|&n| n < (1 << 22)) else {
                    bad = true;
                    return;
                };
                let n = n as usize;
                if camp_of.len() <= n {
                    camp_of.resize(n + 1, u32::MAX);
                }
                if camp_of[n] == u32::MAX {
                    camp_of[n] = camp_ids.len() as u32;
                    camp_ids.push(id);
                    channels.push(ch);
                }
            }
        });
        if bad {
            return why("non-canonical campaign id");
        }
    }
    let organic = camp_ids.len() as u32;
    camp_ids.push(b"organic");
    channels.push(b"organic");
    let ncat = cat_names.len();

    let ev_files: Vec<B> = evm.iter().map(|m| m.bytes()).collect();
    let ck_files: Vec<B> = ckm.iter().map(|m| m.bytes()).collect();
    let lk_files: Vec<B> = lkm.iter().map(|m| m.bytes()).collect();
    let ev_chunks = chunks(&ev_files, chunk_target(&ev_files, ncpu));
    let ck_chunks = chunks(&ck_files, chunk_target(&ck_files, ncpu));
    let lk_chunks = chunks(&lk_files, chunk_target(&lk_files, ncpu));
    let mut file_ends: Vec<usize> = ev_files.iter().chain(ck_files.iter()).map(|f| f.as_ptr() as usize + f.len()).collect();
    file_ends.sort_unstable();
    lap(log, &mut last, t0, "setup");

    // ---- links ----
    let (lparts, _) = par_run(lk_chunks.len(), ncpu, || (), |_, i| parse_links(lk_chunks[i]));
    let links = build_links(&lparts)?;
    drop(lparts);
    lap(log, &mut last, t0, "links");

    // ---- parse events + clicks ----
    let ctx = Ctx { ws, we, cat_of: &cat_of, unknown_cat, camp_of: &camp_of, links: &links, mode: std::env::var("XL_V5MODE").ok().and_then(|v| v.parse().ok()).unwrap_or(0) };
    let (ne, nc) = (ev_chunks.len(), ck_chunks.len());
    if ne + nc >= (1 << 12) {
        return why("too many chunks");
    }
    let bad_slot: Mutex<Option<&'static str>> = Mutex::new(None);
    let (_, states) = par_run(
        ne + nc,
        ncpu,
        || (TS::new(), EvBatch::new(), CkBatch::new()),
        |s, i| {
            let (ts, eb, cb) = s;
            let is_ev = i < ne;
            let buf: B = if is_ev { ev_chunks[i] } else { ck_chunks[i - ne] };
            // the last chunk of a file gets a private padded copy: kernels read whole vectors
            let end = buf.as_ptr() as usize + buf.len();
            let buf: B = if file_ends.binary_search(&end).is_ok() {
                let mut v = Vec::with_capacity(buf.len() + 256);
                v.extend_from_slice(buf);
                v.resize(buf.len() + 256, 0);
                let l: &'static mut [u8] = Box::leak(v.into_boxed_slice());
                &l[..buf.len()]
            } else {
                buf
            };
            unsafe {
                let a = (buf.as_ptr() as usize) & !4095;
                madvise(a as *mut u8, buf.as_ptr() as usize + buf.len() - a, 22);
            }
            let eb = std::mem::replace(eb, EvBatch::new());
            let cb = std::mem::replace(cb, CkBatch::new());
            let mut sink = Sink { c: &ctx, st: ts, eb, cb, base: buf.as_ptr(), chunk: i as u32, ridx: 0, bad: None };
            if is_ev {
                <crate::K as Kernel>::events(buf, &mut sink);
            } else {
                <crate::K as Kernel>::clicks(buf, &mut sink);
            }
            if let Some(b) = sink.bad {
                *bad_slot.lock().unwrap() = Some(b);
            }
        },
    );
    if let Some(b) = bad_slot.into_inner().unwrap() {
        return why(b);
    }
    lap(log, &mut last, t0, "parse");
    let nthreads = states.len();
    let lost: Vec<Vec<AtomicU8>> = states
        .iter()
        .map(|s| {
            let v = vec![0u8; s.0.seq as usize];
            unsafe { std::mem::transmute::<Vec<u8>, Vec<AtomicU8>>(v) }
        })
        .collect();
    let ts_states: Vec<&TS> = states.iter().map(|s| &s.0).collect();

    // ---- dedup per event shard ----
    let _ = par_run(
        ES,
        ncpu,
        || (Tab::default(), Vec::<(Claim, u32)>::new()),
        |(tab, cl), s| {
            cl.clear();
            for (t, st) in ts_states.iter().enumerate() {
                for c in st.claims[s].iter() {
                    cl.push((*c, t as u32));
                }
            }
            tab.reset(cl.len());
            let mut best: Vec<u32> = Vec::new(); // indexes into cl of current winners, by table idx
            for k in 0..cl.len() {
                let (c, t) = cl[k];
                let h = mixk(c.key);
                match tab.find(h, |i| {
                    let w = &cl[best[i as usize] as usize].0;
                    id_eq(w.key, w.loc, c.key, c.loc)
                }) {
                    Ok((_, i)) => {
                        let wi = best[i as usize] as usize;
                        let (w, wt) = cl[wi];
                        if (c.ing, c.ord) < (w.ing, w.ord) {
                            lost[wt as usize][w.seq as usize].store(1, Ordering::Relaxed);
                            best[i as usize] = k as u32;
                        } else {
                            lost[t as usize][c.seq as usize].store(1, Ordering::Relaxed);
                        }
                    }
                    Err(slot) => {
                        tab.set(slot, h, best.len() as u32);
                        best.push(k as u32);
                    }
                }
            }
        },
    );
    lap(log, &mut last, t0, "dedup");

    // ---- people ----
    let (rows, pstates) = par_run(
        PS,
        ncpu,
        || (Scratch::new(ncat), Arena::new(), Tab::default(), Acc::default()),
        |(sc, arena, tab, acc), p| {
            // group events by person key
            let mut n_ev = 0usize;
            for st in &ts_states {
                n_ev += st.pays[p].len();
            }
            tab.reset(n_ev);
            let mut pk: Vec<(u64, u64)> = Vec::new();
            let mut pid: Vec<u32> = Vec::with_capacity(n_ev);
            let mut nkept = 0usize;
            for (t, st) in ts_states.iter().enumerate() {
                for y in st.pays[p].iter() {
                    if lost[t][y.seq as usize].load(Ordering::Relaxed) != 0 {
                        pid.push(u32::MAX);
                        continue;
                    }
                    nkept += 1;
                    let h = mixk(y.pkey);
                    let id = match tab.find(h, |i| {
                        let q = pk[i as usize];
                        id_eq(q.0, q.1, y.pkey, y.ploc)
                    }) {
                        Ok((_, i)) => i,
                        Err(slot) => {
                            let i = pk.len() as u32;
                            pk.push((y.pkey, y.ploc));
                            tab.set(slot, h, i);
                            i
                        }
                    };
                    pid.push(id);
                }
            }
            let np = pk.len();
            let mut off = vec![0u32; np + 1];
            for &i in &pid {
                if i != u32::MAX {
                    off[i as usize + 1] += 1;
                }
            }
            for i in 0..np {
                off[i + 1] += off[i];
            }
            let mut pos: Vec<u32> = off[..np].to_vec();
            let mut pe = vec![PE { ts: 0, pt: 0, cat: 0 }; nkept];
            let mut k = 0usize;
            for st in ts_states.iter() {
                for y in st.pays[p].iter() {
                    let i = pid[k];
                    k += 1;
                    if i != u32::MAX {
                        let q = &mut pos[i as usize];
                        pe[*q as usize] = PE { ts: ws + y.ts as i64, pt: y.pp, cat: y.cat };
                        *q += 1;
                    }
                }
            }
            // clicks of known people
            let lo = ws - WEEK_MS;
            let mut cid: Vec<u32> = Vec::new();
            let mut coff = vec![0u32; np + 1];
            for st in &ts_states {
                for x in st.cpays[p].iter() {
                    let h = mixk(x.pkey);
                    let id = match tab.find(h, |i| {
                        let q = pk[i as usize];
                        id_eq(q.0, q.1, x.pkey, x.ploc)
                    }) {
                        Ok((_, i)) => {
                            coff[i as usize + 1] += 1;
                            i
                        }
                        Err(_) => u32::MAX,
                    };
                    cid.push(id);
                }
            }
            for i in 0..np {
                coff[i + 1] += coff[i];
            }
            let mut cpos: Vec<u32> = coff[..np].to_vec();
            let mut pc = vec![PC { ts: 0, ckey: 0, cloc: 0, camp: 0 }; coff[np] as usize];
            let mut k = 0usize;
            for st in &ts_states {
                for x in st.cpays[p].iter() {
                    let i = cid[k];
                    k += 1;
                    if i != u32::MAX {
                        let q = &mut cpos[i as usize];
                        pc[*q as usize] = PC { ts: lo + x.ts as i64, ckey: x.ckey, cloc: x.cloc, camp: x.camp };
                        *q += 1;
                    }
                }
            }
            let mut rows: Vec<UserRow<'static>> = Vec::with_capacity(np * 2);
            let mut scratch = Vec::new();
            for i in 0..np {
                let (key, loc) = pk[i];
                let name: &'static [u8] = if key & LINKED != 0 && key & ODD == 0 {
                    let r = (key & !LINKED) as usize;
                    let nn = links.name[r] as usize;
                    id_bytes(links.nkey[nn], links.nloc[nn], b'u', &mut scratch);
                    arena.put(&scratch)
                } else if key & ODD != 0 {
                    unsafe { loc_bytes(loc) }
                } else {
                    id_bytes(key, 0, b'u', &mut scratch);
                    arena.put(&scratch)
                };
                let evs = &mut pe[off[i] as usize..off[i + 1] as usize];
                let cs = &mut pc[coff[i] as usize..coff[i + 1] as usize];
                person(name, evs, cs, sc, &mut rows, organic, acc);
            }
            rows
        },
    );
    lap(log, &mut last, t0, "people");
    drop(ts_states);
    drop(states);
    let mut acc: Acc = Acc::default();
    for (_, _, _, a) in pstates {
        if acc.is_empty() {
            acc = a;
            continue;
        }
        for (k, v) in a {
            let e = acc.entry(k).or_insert([0; 4]);
            for i in 0..4 {
                e[i] += v[i];
            }
        }
    }
    lap(log, &mut last, t0, "merge");
    let ud = out_dir.join("user_daily");
    let cd = out_dir.join("campaign_daily");
    std::fs::create_dir_all(&ud)?;
    std::fs::create_dir_all(&cd)?;
    clear_parquet(&ud)?;
    clear_parquet(&cd)?;
    let nparts = ncpu.min(PS).max(1);
    let per = PS.div_ceil(nparts);
    let (res, _) = par_run(nparts + 1, ncpu, || (), |_, k| {
        if k == nparts {
            write_campaigns(&cd.join("part-0.parquet"), &acc, &camp_ids, &channels)
        } else {
            let group = &rows[(k * per).min(PS)..((k + 1) * per).min(PS)];
            write_users(&ud.join(format!("part-{k}.parquet")), group, &cat_names)
        }
    });
    for r in res {
        r?;
    }
    lap(log, &mut last, t0, "write");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn v5_equal_hash_ids_stay_distinct() {
        // two different long/odd IDs forced onto the same 63-bit hash key
        let a: &[u8] = b"u0123456789abcdef0123456789abcdef";
        let b: &[u8] = b"u0123456789abcdef0123456789abcdee";
        let x: &[u8] = b"u1";
        let y: &[u8] = b"u2";
        let h = 0x1234_5678_9abc_def0u64;
        let (ka, la) = classify(a, h).unwrap();
        let (kb, lb) = classify(b, h).unwrap();
        assert_eq!(ka, kb);
        assert!(!id_eq(ka, la, kb, lb));
        assert!(id_eq(ka, la, ka, la));
        // same bytes at a different address are equal
        let a2 = a.to_vec();
        let (ka2, la2) = classify(&a2, h).unwrap();
        assert!(id_eq(ka, la, ka2, la2));
        // links: A-x and B-y share A/B's hash but must not join people
        let parts = vec![vec![(a, h, x, 11u64), (b, h, y, 12u64)]];
        let lt = build_links(&parts).unwrap();
        let na = lt.find(ka, la).unwrap();
        let nb = lt.find(kb, lb).unwrap();
        assert_ne!(na, nb);
        assert_ne!(lt.root[na as usize], lt.root[nb as usize]);
        let (kx, lx) = classify(x, 11).unwrap();
        let nx = lt.find(kx, lx).unwrap();
        assert_eq!(lt.root[na as usize], lt.root[nx as usize]);
    }

    #[test]
    fn v5_short_ids_are_their_own_key_and_order_is_bytewise() {
        let (k1, _) = classify(b"uzz", 0).unwrap();
        let (k2, _) = classify(b"u10", 0).unwrap();
        assert_ne!(k1, k2);
        assert_eq!(id_cmp((k1, 0), (k2, 0), b'u'), Cmp::Greater); // "uzz" > "u10" bytewise
        let (ko, lo) = classify(b"uZZ", 99).unwrap(); // upper-case is an odd ID, smaller than lower-case
        assert_eq!(id_cmp((ko, lo), (k2, 0), b'u'), Cmp::Less);
    }

    #[test]
    fn v5_smallest_id_names_the_person() {
        let parts = vec![vec![(&b"uzz"[..], 1u64, &b"uAB"[..], 2u64), (&b"u10"[..], 3u64, &b"uzz"[..], 1u64)]];
        let lt = build_links(&parts).unwrap();
        let (k, l) = classify(b"u10", 3).unwrap();
        let n = lt.find(k, l).unwrap();
        let r = lt.root[n as usize] as usize;
        let nm = lt.name[r] as usize;
        let mut out = Vec::new();
        id_bytes(lt.nkey[nm], lt.nloc[nm], b'u', &mut out);
        assert_eq!(out, b"uAB");
    }
}
