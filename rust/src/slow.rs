// Fallback: the previous verified engine (exact string interning). Used only if the fast engine
// detects a hash collision or an input it does not support.
use ahash::AHashMap as HashMap;
use rayon::prelude::*;
use std::sync::Arc;
use std::time::Instant;

const DAY: i64 = 86_400_000;
const GAP: i64 = 1_800_000;
const LOOK: i64 = 7 * DAY;

static mut T0: Option<Instant> = None;
fn lap(name: &str) {
    if crate::timing_enabled() {
        #[allow(static_mut_refs)]
        let t = unsafe { T0.unwrap() };
        eprintln!("{:8.1} ms  {}", t.elapsed().as_secs_f64() * 1000.0, name);
    }
}

// ------------------------------------------------------------------ CSV
fn csv_files(dir: &str) -> Vec<String> {
    let mut v: Vec<String> = match std::fs::read_dir(dir) {
        Ok(rd) => rd
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .filter(|p| p.extension().map(|x| x == "csv").unwrap_or(false) && p.is_file())
            .map(|p| p.to_string_lossy().into_owned())
            .collect(),
        Err(_) => vec![],
    };
    v.sort();
    v
}

fn read_all(files: &[String]) -> Vec<Vec<u8>> {
    files.par_iter().map(|f| std::fs::read(f).unwrap()).collect()
}

/// Byte ranges (data lines only, header skipped) of roughly `target` bytes, split at '\n'.
fn chunks<'a>(bufs: &'a [Vec<u8>], target: usize) -> Vec<&'a [u8]> {
    let mut out = vec![];
    for b in bufs {
        let start = match memchr::memchr(b'\n', b) {
            Some(i) => i + 1,
            None => continue,
        };
        let mut s = start;
        while s < b.len() {
            let mut e = (s + target).min(b.len());
            if e < b.len() {
                e = match memchr::memchr(b'\n', &b[e..]) {
                    Some(i) => e + i + 1,
                    None => b.len(),
                };
            }
            out.push(&b[s..e]);
            s = e;
        }
    }
    out
}

/// Iterate lines of a chunk ('\n' only; a trailing '\n' does not start a new line).
fn for_lines<'a>(c: &'a [u8], mut f: impl FnMut(&'a [u8])) {
    let mut s = 0;
    for i in memchr::memchr_iter(b'\n', c) {
        f(&c[s..i]);
        s = i + 1;
    }
    if s < c.len() {
        f(&c[s..]);
    }
}

/// Split a line into exactly N fields, unquoting; None if malformed.
#[inline]
fn fields<'a, const N: usize>(line: &'a [u8]) -> Option<[&'a [u8]; N]> {
    let mut out: [&[u8]; N] = [&[]; N];
    let mut k = 0;
    let mut s = 0;
    let n = line.len();
    let mut i = 0;
    loop {
        // find end of field
        while i < n && line[i] != b',' {
            i += 1;
        }
        if k >= N {
            return None;
        }
        let mut f = &line[s..i];
        if memchr::memchr(b'"', f).is_some() {
            if f.len() < 2 || f[0] != b'"' || f[f.len() - 1] != b'"' {
                return None;
            }
            f = &f[1..f.len() - 1];
            if memchr::memchr(b'"', f).is_some() {
                return None;
            }
        }
        out[k] = f;
        k += 1;
        if i >= n {
            break;
        }
        i += 1;
        s = i;
    }
    if k == N && !line.is_empty() { Some(out) } else { None }
}

// ------------------------------------------------------------------ timestamps
#[inline]
fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let doy = (153 * ((m + 9) % 12) + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146097 + doe - 719468
}
#[inline]
fn leap(y: i64) -> bool {
    y % 4 == 0 && (y % 100 != 0 || y % 400 == 0)
}
#[inline]
fn d2(s: &[u8], i: usize) -> Option<i64> {
    let a = s[i].wrapping_sub(b'0');
    let b = s[i + 1].wrapping_sub(b'0');
    if a < 10 && b < 10 { Some((a * 10 + b) as i64) } else { None }
}
/// ms since epoch, or None if bad. Overlong F3 saturates to i64::MAX (out of any window).
#[inline]
fn parse_ts(s: &[u8]) -> Option<i64> {
    if s.is_empty() {
        return None;
    }
    if s.iter().all(|c| c.is_ascii_digit()) {
        let mut v: i64 = 0;
        for &c in s {
            v = match v.checked_mul(10).and_then(|x| x.checked_add((c - b'0') as i64)) {
                Some(x) => x,
                None => return Some(i64::MAX),
            };
        }
        return Some(v);
    }
    let off = match s.len() {
        20 if s[19] == b'Z' => 0,
        25 if (s[19] == b'+' || s[19] == b'-') && s[22] == b':' => {
            let oh = d2(s, 20)?;
            let om = d2(s, 23)?;
            if oh > 23 || om > 59 {
                return None;
            }
            let o = (oh * 60 + om) * 60_000;
            if s[19] == b'+' { o } else { -o }
        }
        _ => return None,
    };
    if s[4] != b'-' || s[7] != b'-' || s[10] != b'T' || s[13] != b':' || s[16] != b':' {
        return None;
    }
    let y = d2(s, 0)? * 100 + d2(s, 2)?;
    let mo = d2(s, 5)?;
    let d = d2(s, 8)?;
    let h = d2(s, 11)?;
    let mi = d2(s, 14)?;
    let se = d2(s, 17)?;
    if !(1..=12).contains(&mo) || h > 23 || mi > 59 || se > 59 || d < 1 {
        return None;
    }
    let dim = [31, if leap(y) { 29 } else { 28 }, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31][(mo - 1) as usize];
    if d > dim {
        return None;
    }
    Some((days_from_civil(y, mo, d) * 86400 + h * 3600 + mi * 60 + se) * 1000 - off)
}

fn manifest_ts(m: &str, key: &str) -> i64 {
    let i = m.find(key).unwrap() + key.len();
    let rest = &m[i..];
    let a = rest.find('"').unwrap();
    let rest = &rest[a + 1..];
    let b = rest.find('"').unwrap();
    parse_ts(rest[..b].as_bytes()).unwrap()
}

#[inline]
fn hash_bytes(b: &[u8]) -> u64 {
    // FNV-1a-ish mixing is enough for sharding.
    let mut h: u64 = 0xcbf29ce484222325;
    for &c in b {
        h = (h ^ c as u64).wrapping_mul(0x100000001b3);
    }
    h ^ (h >> 29)
}

// ------------------------------------------------------------------ main
#[derive(Clone, Copy)]
struct RawEv<'a> {
    #[allow(dead_code)]
    id: &'a [u8],
    user: &'a [u8],
    ts: &'a [u8],
    ingest: i64,
    etype: &'a [u8],
    product: &'a [u8],
    price: &'a [u8],
}

struct Ev {
    pid: u32,
    ts: i64,
    price: i64, // -1 for non-purchase
    cat: u32,
}

const SH: usize = 256;

pub fn run() {
    if crate::timing_enabled() {
        unsafe { T0 = Some(Instant::now()) };
    }
    let data = std::env::var("DATA_DIR").unwrap_or("/data".into());
    let out = std::env::var("OUTPUT_DIR").unwrap_or("/output".into());
    let ncpu: usize = std::env::var("NCPU").ok().and_then(|s| s.parse().ok()).unwrap_or(8);
    rayon::ThreadPoolBuilder::new().num_threads(ncpu).build_global().ok();

    let man = std::fs::read_to_string(format!("{data}/manifest.json")).unwrap();
    let w0 = manifest_ts(&man, "\"window_start\"");
    let w1 = manifest_ts(&man, "\"window_end\"");

    // read everything (parallel over files)
    let evf = csv_files(&format!("{data}/events"));
    let ckf = csv_files(&format!("{data}/clicks"));
    let lkf = csv_files(&format!("{data}/links"));
    let small = vec![format!("{data}/products.csv"), format!("{data}/campaigns.csv")];
    let mut all: Vec<String> = vec![];
    all.extend(evf.iter().cloned());
    all.extend(ckf.iter().cloned());
    all.extend(lkf.iter().cloned());
    all.extend(small.iter().cloned());
    let bufs = read_all(&all);
    let (evb, rest) = bufs.split_at(evf.len());
    let (ckb, rest) = rest.split_at(ckf.len());
    let (lkb, smb) = rest.split_at(lkf.len());
    lap("read files");

    // products / campaigns
    let mut cat_names: Vec<&[u8]> = vec![];
    let mut cat_idx: HashMap<&[u8], u32> = HashMap::new();
    let mut prod: HashMap<&[u8], u32> = HashMap::new();
    for_lines(&smb[0][memchr::memchr(b'\n', &smb[0]).map(|i| i + 1).unwrap_or(smb[0].len())..], |l| {
        if let Some([p, c]) = fields::<2>(l) {
            let n = cat_names.len() as u32;
            let ci = *cat_idx.entry(c).or_insert_with(|| {
                cat_names.push(c);
                n
            });
            prod.entry(p).or_insert(ci);
        }
    });
    let unknown = cat_names.len() as u32;
    cat_names.push(b"unknown");
    let mut camp_names: Vec<&[u8]> = vec![];
    let mut camp_chan: Vec<&[u8]> = vec![];
    let mut camp: HashMap<&[u8], u32> = HashMap::new();
    for_lines(&smb[1][memchr::memchr(b'\n', &smb[1]).map(|i| i + 1).unwrap_or(smb[1].len())..], |l| {
        if let Some([c, ch]) = fields::<2>(l) {
            if !c.is_empty() && !camp.contains_key(c) {
                camp.insert(c, camp_names.len() as u32);
                camp_names.push(c);
                camp_chan.push(ch);
            }
        }
    });
    let ncamp = camp_names.len();

    // links: union-find
    let lch = chunks(lkb, 1 << 20);
    let edges: Vec<(&[u8], &[u8])> = lch
        .par_iter()
        .flat_map_iter(|c| {
            let mut v = vec![];
            for_lines(c, |l| {
                if let Some([a, b]) = fields::<2>(l) {
                    if !a.is_empty() && !b.is_empty() {
                        v.push((a, b));
                    }
                }
            });
            v
        })
        .collect();
    let mut lid: HashMap<&[u8], u32> = HashMap::with_capacity(edges.len() * 2);
    let mut ids: Vec<&[u8]> = vec![];
    let mut parent: Vec<u32> = vec![];
    fn find(p: &mut [u32], mut x: u32) -> u32 {
        while p[x as usize] != x {
            let g = p[p[x as usize] as usize];
            p[x as usize] = g;
            x = g;
        }
        x
    }
    for &(a, b) in &edges {
        let ia = *lid.entry(a).or_insert_with(|| {
            ids.push(a);
            parent.push(parent.len() as u32);
            (parent.len() - 1) as u32
        });
        let ib = *lid.entry(b).or_insert_with(|| {
            ids.push(b);
            parent.push(parent.len() as u32);
            (parent.len() - 1) as u32
        });
        let (ra, rb) = (find(&mut parent, ia), find(&mut parent, ib));
        if ra != rb {
            parent[ra.max(rb) as usize] = ra.min(rb);
        }
    }
    // person index for each link id; person names
    let mut pname: Vec<&[u8]> = vec![];
    let mut root_p: Vec<u32> = vec![u32::MAX; ids.len()];
    let mut link_person: Vec<u32> = vec![0; ids.len()];
    for i in 0..ids.len() {
        let r = find(&mut parent, i as u32) as usize;
        if root_p[r] == u32::MAX {
            root_p[r] = pname.len() as u32;
            pname.push(ids[i]);
        }
        let p = root_p[r];
        if ids[i] < pname[p as usize] {
            pname[p as usize] = ids[i];
        }
        link_person[i] = p;
    }
    let nlinkp = pname.len();
    lap(&format!("links: {} edges, {} ids, {} persons", edges.len(), ids.len(), nlinkp));

    // events: parse into shards by hash(event_id). Shards keep only (id, ingest, line)
    // to cut memory traffic; the other fields are reparsed for the kept copy only.
    #[derive(Clone, Copy)]
    struct Lite<'a> {
        id: &'a [u8],
        ingest: i64,
        line: &'a [u8],
    }
    let ech = chunks(evb, 1 << 19);
    let shards: Vec<Vec<Vec<Lite>>> = ech
        .par_iter()
        .map(|c| {
            let mut sh: Vec<Vec<Lite>> = (0..SH).map(|_| Vec::with_capacity(c.len() / 80 / SH + 4)).collect();
            for_lines(c, |l| {
                if let Some([id, _, _, ing, _, _, _]) = fields::<7>(l) {
                    if id.is_empty() {
                        return;
                    }
                    let ingest = parse_ts(ing).unwrap_or(i64::MAX);
                    sh[(hash_bytes(id) as usize) & (SH - 1)].push(Lite { id, ingest, line: l });
                }
            });
            sh
        })
        .collect();
    lap("events parsed");
    // dedup + validate per shard
    let nded = std::sync::atomic::AtomicUsize::new(0);
    // validated events keep only what later stages read: user, ts, price, category
    let valid: Vec<Vec<(&[u8], i64, i64, u32)>> = (0..SH)
        .into_par_iter()
        .map(|s| {
            let cap: usize = shards.iter().map(|x| x[s].len()).sum();
            let mut m: HashMap<&[u8], u32> = HashMap::with_capacity(cap);
            let mut recs: Vec<Lite> = Vec::with_capacity(cap);
            for part in &shards {
                for r in &part[s] {
                    match m.get(r.id) {
                        Some(&i) => {
                            let e = &mut recs[i as usize];
                            if r.ingest < e.ingest {
                                *e = *r;
                            }
                        }
                        None => {
                            m.insert(r.id, recs.len() as u32);
                            recs.push(*r);
                        }
                    }
                }
            }
            nded.fetch_add(recs.len(), std::sync::atomic::Ordering::Relaxed);
            let mut v = Vec::with_capacity(recs.len());
            for lr in recs {
                let Some([id, user, ts, _, et, pr, pc]) = fields::<7>(lr.line) else { continue };
                let r = RawEv { id, user, ts, ingest: lr.ingest, etype: et, product: pr, price: pc };
                if r.user.is_empty() || r.product.is_empty() {
                    continue;
                }
                let purchase = match r.etype {
                    b"view" | b"cart" => false,
                    b"purchase" => true,
                    _ => continue,
                };
                let ts = match parse_ts(r.ts) {
                    Some(t) if t >= w0 && t < w1 => t,
                    _ => continue,
                };
                if r.ingest == i64::MAX || r.ingest - ts > DAY {
                    continue;
                }
                let mut price = -1;
                if purchase {
                    let p = r.price;
                    if p.is_empty() || !p.iter().all(|c| c.is_ascii_digit()) {
                        continue;
                    }
                    let z = p.iter().take_while(|&&c| c == b'0').count();
                    let p = &p[z..];
                    if p.len() > 8 {
                        continue;
                    }
                    let v: i64 = p.iter().fold(0, |a, &c| a * 10 + (c - b'0') as i64);
                    if !(1..=10_000_000).contains(&v) {
                        continue;
                    }
                    price = v;
                }
                let cat = if purchase { *prod.get(r.product).unwrap_or(&unknown) } else { 0 };
                v.push((r.user, ts, price, cat));
            }
            v
        })
        .collect();
    let nvalid: usize = valid.iter().map(|v| v.len()).sum();
    lap(&format!("events dedup {} validate {}", nded.into_inner(), nvalid));

    // clicks: parse, shard by click_id, dedup, validate
    let cch = chunks(ckb, 1 << 19);
    let cshards: Vec<Vec<Vec<[&[u8]; 4]>>> = cch
        .par_iter()
        .map(|c| {
            let mut sh: Vec<Vec<[&[u8]; 4]>> = (0..SH).map(|_| vec![]).collect();
            for_lines(c, |l| {
                if let Some(f) = fields::<4>(l) {
                    if !f[0].is_empty() {
                        sh[(hash_bytes(f[0]) as usize) & (SH - 1)].push(f);
                    }
                }
            });
            sh
        })
        .collect();
    let cvalid: Vec<Vec<(&[u8], &[u8], u32, i64)>> = (0..SH)
        .into_par_iter()
        .map(|s| {
            let mut seen: ahash::AHashSet<&[u8]> = ahash::AHashSet::new();
            let mut v = vec![];
            for part in &cshards {
                for f in &part[s] {
                    if !seen.insert(f[0]) {
                        continue;
                    }
                    if f[1].is_empty() {
                        continue;
                    }
                    let Some(&ci) = camp.get(f[2]) else { continue };
                    match parse_ts(f[3]) {
                        Some(t) if t >= w0 - LOOK && t < w1 => v.push((f[0], f[1], ci, t)),
                        _ => {}
                    }
                }
            }
            v
        })
        .collect();
    let ncv: usize = cvalid.iter().map(|v| v.len()).sum();
    lap(&format!("clicks valid {}", ncv));

    // person ids for every user string: link persons first, then unlinked users
    // persons: link persons first, then unlinked users interned in hash buckets (parallel)
    const U: usize = 64;
    let ub = |u: &[u8]| ((hash_bytes(u) >> 20) as usize) & (U - 1);
    let ev_users = valid.par_iter().map(|v| {
        let mut sets: Vec<ahash::AHashSet<&[u8]>> = (0..U).map(|_| Default::default()).collect();
        for (u, _, _, _) in v { if !lid.contains_key(u) { sets[ub(u)].insert(u); } }
        sets
    });
    let ck_users = cvalid.par_iter().map(|v| {
        let mut sets: Vec<ahash::AHashSet<&[u8]>> = (0..U).map(|_| Default::default()).collect();
        for c in v { if !lid.contains_key(c.1) { sets[ub(c.1)].insert(c.1); } }
        sets
    });
    let locals: Vec<Vec<ahash::AHashSet<&[u8]>>> = ev_users.chain(ck_users).collect();
    let bucket_names: Vec<Vec<&[u8]>> = (0..U).into_par_iter().map(|b| {
        let mut s: ahash::AHashSet<&[u8]> = Default::default();
        for l in &locals { s.extend(l[b].iter().copied()); }
        s.into_iter().collect()
    }).collect();
    let mut base = vec![0u32; U];
    let mut pn: Vec<&[u8]> = pname.clone();
    for b in 0..U { base[b] = pn.len() as u32; pn.extend(bucket_names[b].iter().copied()); }
    let bucket_maps: Vec<HashMap<&[u8], u32>> = bucket_names.par_iter().enumerate().map(|(b, names)| {
        names.iter().enumerate().map(|(i, &n)| (n, base[b] + i as u32)).collect()
    }).collect();
    let pid_of = |u: &[u8]| -> u32 {
        match lid.get(u) { Some(&i) => link_person[i as usize], None => bucket_maps[ub(u)][u] }
    };
    let evs: Vec<Ev> = valid.par_iter().flat_map_iter(|v| v.iter().map(|&(u, ts, price, cat)| {
        Ev { pid: pid_of(u), ts, price, cat }
    })).collect();
    let mut evs = evs;
    let mut cks: Vec<(u32, i64, &[u8], u32)> = cvalid.par_iter().flat_map_iter(|v| v.iter().map(|c| (pid_of(c.1), c.3, c.0, c.2))).collect();
    let pname = pn;
    lap(&format!("persons mapped ({} names)", pname.len()));

    evs.par_sort_unstable_by_key(|e| (e.pid, e.ts));
    cks.par_sort_unstable_by(|a, b| (a.0, a.1, a.2).cmp(&(b.0, b.1, b.2)));
    lap("sorted");

    // sessions
    struct Sess {
        pid: u32,
        start: i64,
        events: i64,
        revenue: i64,
        purch: bool,
        first_ev: usize,
        n: usize,
    }
    let mut sess: Vec<Sess> = vec![];
    {
        let mut i = 0;
        while i < evs.len() {
            let e = &evs[i];
            let new = i == 0 || evs[i - 1].pid != e.pid || e.ts - evs[i - 1].ts > GAP;
            if new {
                sess.push(Sess { pid: e.pid, start: e.ts, events: 0, revenue: 0, purch: false, first_ev: i, n: 0 });
            }
            let s = sess.last_mut().unwrap();
            s.events += 1;
            s.n += 1;
            if e.price >= 0 {
                s.revenue += e.price;
                s.purch = true;
            }
            i += 1;
        }
    }
    lap(&format!("sessions {}", sess.len()));

    // click ranges per person
    let mut ck_start: HashMap<u32, (usize, usize)> = HashMap::new();
    {
        let mut i = 0;
        while i < cks.len() {
            let mut j = i;
            while j < cks.len() && cks[j].0 == cks[i].0 {
                j += 1;
            }
            ck_start.insert(cks[i].0, (i, j));
            i = j;
        }
    }
    // credit + assists, parallel over sessions
    let ncs = ncamp + 1; // index ncamp = organic
    let res: Vec<(u32, Vec<u32>)> = sess
        .par_iter()
        .map_init(
            || (vec![u32::MAX; ncamp], 0u32),
            |(stamp, generation), s| {
                *generation += 1;
                let mut credit = ncamp as u32;
                let mut assists = vec![];
                if let Some(&(a, b)) = ck_start.get(&s.pid) {
                    let cs = &cks[a..b];
                    // last index with ts <= start
                    let hi = cs.partition_point(|c| c.1 <= s.start);
                    if hi > 0 {
                        let t = cs[hi - 1].1;
                        if s.start - t <= LOOK {
                            let first = cs[..hi].partition_point(|c| c.1 < t);
                            credit = cs[first].3;
                        }
                        let lo = cs[..hi].partition_point(|c| c.1 < s.start - LOOK);
                        for c in &cs[lo..hi] {
                            if stamp[c.3 as usize] != *generation {
                                stamp[c.3 as usize] = *generation;
                                assists.push(c.3);
                            }
                        }
                    }
                }
                (credit, assists)
            },
        )
        .collect();
    lap("credit + assists");

    // aggregate user_daily
    let mut ud_user: Vec<&[u8]> = vec![];
    let mut ud_day: Vec<i32> = vec![];
    let mut ud_sess: Vec<i64> = vec![];
    let mut ud_ev: Vec<i64> = vec![];
    let mut ud_rev: Vec<i64> = vec![];
    let mut ud_top: Vec<Option<&[u8]>> = vec![];
    {
        let mut catsum: Vec<i64> = vec![0; cat_names.len()];
        let mut touched: Vec<u32> = vec![];
        let mut i = 0;
        while i < sess.len() {
            let day = sess[i].start.div_euclid(DAY) as i32;
            let pid = sess[i].pid;
            let mut j = i;
            let (mut ns, mut ne, mut nr) = (0, 0, 0);
            while j < sess.len() && sess[j].pid == pid && sess[j].start.div_euclid(DAY) as i32 == day {
                let s = &sess[j];
                ns += 1;
                ne += s.events;
                nr += s.revenue;
                for e in &evs[s.first_ev..s.first_ev + s.n] {
                    if e.price >= 0 {
                        if catsum[e.cat as usize] == 0 {
                            touched.push(e.cat);
                        }
                        catsum[e.cat as usize] += e.price;
                    }
                }
                j += 1;
            }
            let mut top: Option<&[u8]> = None;
            let mut best = -1;
            for &c in &touched {
                let v = catsum[c as usize];
                let nm = cat_names[c as usize];
                if v > best || (v == best && nm < top.unwrap()) {
                    best = v;
                    top = Some(nm);
                }
                catsum[c as usize] = 0;
            }
            touched.clear();
            ud_user.push(pname[pid as usize]);
            ud_day.push(day);
            ud_sess.push(ns);
            ud_ev.push(ne);
            ud_rev.push(nr);
            ud_top.push(top);
            i = j;
        }
    }
    // campaign_daily: key (campaign, day)
    let mut cd: HashMap<(u32, i32), [i64; 4]> = HashMap::new();
    for (s, (credit, assists)) in sess.iter().zip(&res) {
        let day = s.start.div_euclid(DAY) as i32;
        let e = cd.entry((*credit, day)).or_insert([0; 4]);
        e[0] += 1;
        e[1] += s.purch as i64;
        e[2] += s.revenue;
        for &a in assists {
            cd.entry((a, day)).or_insert([0; 4])[3] += 1;
        }
    }
    let _ = ncs;
    lap(&format!("aggregated: user_daily {} campaign_daily {}", ud_user.len(), cd.len()));

    // write parquet
    use arrow_array::*;
    use arrow_schema::{DataType, Field, Schema};
    std::fs::create_dir_all(format!("{out}/user_daily")).unwrap();
    std::fs::create_dir_all(format!("{out}/campaign_daily")).unwrap();
    let props = parquet::file::properties::WriterProperties::builder()
        .set_compression(parquet::basic::Compression::UNCOMPRESSED)
        .set_dictionary_enabled(false)
        .build();
    let write = |path: String, batch: RecordBatch| {
        let f = std::fs::File::create(path).unwrap();
        let mut w = parquet::arrow::ArrowWriter::try_new(f, batch.schema(), Some(props.clone())).unwrap();
        w.write(&batch).unwrap();
        w.close().unwrap();
    };
    let s1 = Arc::new(Schema::new(vec![
        Field::new("user_id", DataType::Binary, false),
        Field::new("day", DataType::Date32, false),
        Field::new("sessions", DataType::Int64, false),
        Field::new("events", DataType::Int64, false),
        Field::new("revenue_cents", DataType::Int64, false),
        Field::new("top_category", DataType::Binary, true),
    ]));
    // Binary would not be a string; use Utf8 (inputs are UTF-8).
    let s1 = Arc::new(Schema::new(
        s1.fields().iter().map(|f| if f.data_type() == &DataType::Binary { Field::new(f.name(), DataType::Utf8, f.is_nullable()) } else { f.as_ref().clone() }).collect::<Vec<_>>(),
    ));
    let utf = |v: Vec<Option<&[u8]>>| -> ArrayRef { Arc::new(StringArray::from(v.into_iter().map(|x| x.map(|b| unsafe { std::str::from_utf8_unchecked(b) })).collect::<Vec<_>>())) };
    let b1 = RecordBatch::try_new(
        s1,
        vec![
            utf(ud_user.iter().map(|&x| Some(x)).collect()),
            Arc::new(Date32Array::from(ud_day)),
            Arc::new(Int64Array::from(ud_sess)),
            Arc::new(Int64Array::from(ud_ev)),
            Arc::new(Int64Array::from(ud_rev)),
            utf(ud_top),
        ],
    )
    .unwrap();
    let mut cid: Vec<Option<&[u8]>> = vec![];
    let mut cch2: Vec<Option<&[u8]>> = vec![];
    let mut cday: Vec<i32> = vec![];
    let mut cv: [Vec<i64>; 4] = Default::default();
    for ((c, d), v) in &cd {
        let (n, ch): (&[u8], &[u8]) = if *c as usize == ncamp { (b"organic", b"organic") } else { (camp_names[*c as usize], camp_chan[*c as usize]) };
        cid.push(Some(n));
        cch2.push(Some(ch));
        cday.push(*d);
        for k in 0..4 {
            cv[k].push(v[k]);
        }
    }
    let s2 = Arc::new(Schema::new(vec![
        Field::new("campaign_id", DataType::Utf8, false),
        Field::new("channel", DataType::Utf8, false),
        Field::new("day", DataType::Date32, false),
        Field::new("sessions", DataType::Int64, false),
        Field::new("purchasing_sessions", DataType::Int64, false),
        Field::new("revenue_cents", DataType::Int64, false),
        Field::new("assisted_sessions", DataType::Int64, false),
    ]));
    let [a, b, c, d] = cv;
    let b2 = RecordBatch::try_new(
        s2,
        vec![utf(cid), utf(cch2), Arc::new(Date32Array::from(cday)), Arc::new(Int64Array::from(a)), Arc::new(Int64Array::from(b)), Arc::new(Int64Array::from(c)), Arc::new(Int64Array::from(d))],
    )
    .unwrap();
    rayon::join(
        || write(format!("{out}/user_daily/part-0.parquet"), b1),
        || write(format!("{out}/campaign_daily/part-0.parquet"), b2),
    );
    lap("parquet written");
    // Skip freeing the big buffers.
    std::process::exit(0);
}
