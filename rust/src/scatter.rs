//! P1 of the gum2 redesign: parse output is scattered by hash(uid) into `PARTS`
//! partitions. A producer owns one `Scatter` and touches no shared state: rows
//! are staged in small per-partition buffers (about 256 bytes, four cache
//! lines) and copied to a presized, demand-paged arena in whole batches. A
//! partition that outgrows its presized slice spills into a `Vec`, so an
//! estimate that is too small costs speed and never correctness.

use std::mem::{MaybeUninit, size_of};

pub(super) const PART_BITS: u32 = 10;
/// Number of uid-hash partitions. This is not the person partition count (`P`).
pub(super) const PARTS: usize = 1 << PART_BITS;

/// Partition of a 63-bit user key or event key. Packed short keys and hashed
/// long keys are both exact identities, so one key always maps to one partition.
#[inline]
pub(super) fn part_of(key: u64) -> usize {
    let mixed = (key ^ (key >> 29)).wrapping_mul(0x9e37_79b9_7f4a_7c15);
    (mixed >> (64 - PART_BITS)) as usize
}

/// One parsed event row. A row with `ts == INVALID` is a claim only: its event
/// ID still competes for the earliest ingest but it carries no payload.
/// `ukey`/`uchk` are the user identity (packed short key, or 63-bit hash plus
/// an independent check for a long ID); `name` is a reference to the full user
/// ID bytes (the long-ID slot, also used to name unlinked people).
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct EvRec {
    pub(super) ekey: u64,
    /// Nonzero for a long event ID: packed reference to its full bytes.
    pub(super) eref: u64,
    pub(super) ukey: u64,
    pub(super) name: u64,
    pub(super) ingest: i32,
    pub(super) ts: u32,
    /// Price in the low 24 bits and product category above them (purchases only).
    pub(super) price: u32,
    pub(super) uchk: u32,
}
const _: () = assert!(size_of::<EvRec>() == 48);
/// Rows per staging buffer: 5 x 48 = 240 bytes.
pub(super) const EV_STAGE: usize = 5;

/// One parsed click row that passed every validity rule.
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct ClRec {
    /// Click ID reference in the low 48 bits, campaign index above.
    pub(super) tag: u64,
    pub(super) ukey: u64,
    pub(super) uchk: u32,
    pub(super) ts: u32,
}
const _: () = assert!(size_of::<ClRec>() == 24);
/// Rows per staging buffer: 10 x 24 = 240 bytes.
pub(super) const CL_STAGE: usize = 10;

#[repr(align(64))]
struct Stage<T, const R: usize>([MaybeUninit<T>; R]);

pub(super) struct Scatter<T: Copy, const R: usize> {
    map: Option<memmap2::MmapMut>,
    /// Rows reserved per partition in the arena.
    stride: usize,
    lens: Box<[usize]>,
    fill: Box<[u8]>,
    stage: Box<[Stage<T, R>]>,
    overflow: Box<[Vec<T>]>,
}

// The arena is owned and written only through &mut self; shared readers see
// initialized prefixes. Rows are plain data.
unsafe impl<T: Copy + Send, const R: usize> Send for Scatter<T, R> {}
unsafe impl<T: Copy + Sync, const R: usize> Sync for Scatter<T, R> {}

impl<T: Copy, const R: usize> Scatter<T, R> {
    /// `rows_per_partition` is the expected row count per partition for this
    /// producer. The arena is virtual until touched.
    pub(super) fn new(rows_per_partition: usize) -> Self {
        let stride = if rows_per_partition == 0 { 0 } else { rows_per_partition.next_multiple_of(R) };
        let map = (stride != 0)
            .then(|| memmap2::MmapMut::map_anon(PARTS * stride * size_of::<T>()).ok())
            .flatten();
        #[cfg(target_os = "linux")]
        if let Some(map) = &map {
            let _ = map.advise(memmap2::Advice::HugePage);
        }
        let stride = if map.is_some() { stride } else { 0 };
        Self {
            map,
            stride,
            lens: vec![0; PARTS].into_boxed_slice(),
            fill: vec![0; PARTS].into_boxed_slice(),
            stage: (0..PARTS).map(|_| Stage([const { MaybeUninit::uninit() }; R])).collect(),
            overflow: (0..PARTS).map(|_| Vec::new()).collect(),
        }
    }

    #[inline]
    pub(super) fn push(&mut self, part: usize, row: T) {
        let fill = self.fill[part] as usize;
        self.stage[part].0[fill].write(row);
        if fill + 1 == R {
            self.flush(part, R);
            self.fill[part] = 0;
        } else {
            self.fill[part] = fill as u8 + 1;
        }
    }

    /// Move the first `count` staged rows of `part` to the arena, or to the
    /// overflow vector once the presized slice is full.
    fn flush(&mut self, part: usize, count: usize) {
        let src = self.stage[part].0.as_ptr().cast::<T>();
        let start = self.lens[part];
        let mapped = count.min(self.stride - start);
        if mapped != 0 {
            let base = self.map.as_mut().unwrap().as_mut_ptr().cast::<T>();
            // The staged prefix is initialized and this slice of the arena is
            // reserved for this partition and not yet written.
            unsafe { std::ptr::copy_nonoverlapping(src, base.add(part * self.stride + start), mapped) };
            self.lens[part] = start + mapped;
        }
        if mapped != count {
            let tail = &mut self.overflow[part];
            // The remaining staged rows are initialized.
            tail.extend_from_slice(unsafe { std::slice::from_raw_parts(src.add(mapped), count - mapped) });
        }
    }

    /// Flush every partly filled staging buffer. Call once, before reading.
    pub(super) fn finish(&mut self) {
        for part in 0..PARTS {
            let pending = self.fill[part] as usize;
            if pending != 0 {
                self.flush(part, pending);
                self.fill[part] = 0;
            }
        }
    }

    /// The rows of one partition in two pieces: the arena prefix, then the overflow.
    #[inline]
    pub(super) fn records(&self, part: usize) -> [&[T]; 2] {
        debug_assert_eq!(self.fill[part], 0, "finish() before records()");
        let prefix = match &self.map {
            // Only the recorded prefix is initialized; nothing writes through &self.
            Some(map) if self.lens[part] != 0 => unsafe {
                std::slice::from_raw_parts(map.as_ptr().cast::<T>().add(part * self.stride), self.lens[part])
            },
            _ => &[][..],
        };
        [prefix, &self.overflow[part]]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rec(i: u64) -> EvRec {
        EvRec { ekey: i, eref: 0, ukey: i * 3, name: i * 5, ingest: i as i32, ts: i as u32, price: 0, uchk: 0 }
    }

    fn all(s: &Scatter<EvRec, EV_STAGE>, part: usize) -> Vec<EvRec> {
        s.records(part).into_iter().flatten().copied().collect()
    }

    #[test]
    fn rows_survive_staging_arena_and_overflow_in_order() {
        // Strides of 0 (no arena), one batch, and a larger arena that overflows.
        for rows in [0usize, 5, 12, 1000] {
            let mut s = Scatter::<EvRec, EV_STAGE>::new(rows);
            for i in 0..777u64 {
                s.push((i % 3) as usize * 5, rec(i));
            }
            s.finish();
            for lane in 0..3u64 {
                let want: Vec<_> = (0..777u64).filter(|i| i % 3 == lane).map(rec).collect();
                assert_eq!(all(&s, lane as usize * 5), want, "stride {rows}");
            }
            assert!(all(&s, 1).is_empty());
        }
    }

    #[test]
    fn click_records_round_trip_through_a_batch_boundary() {
        let mut s = Scatter::<ClRec, CL_STAGE>::new(10);
        let rows: Vec<_> = (0..35u64).map(|i| ClRec { tag: i, ukey: !i, uchk: i as u32, ts: 7 }).collect();
        for &r in &rows {
            s.push(PARTS - 1, r);
        }
        s.finish();
        let got: Vec<_> = s.records(PARTS - 1).into_iter().flatten().copied().collect();
        assert_eq!(got, rows);
    }

    /// Distinct identities forced into one partition stay distinct records:
    /// scattering never merges or reorders identities.
    #[test]
    fn forced_collisions_keep_short_and_long_identities_apart() {
        let short = crate::nova::user_ident(b"u1234567");
        let short_twin = crate::nova::user_ident(b"U1234567");
        let long_a = crate::nova::user_ident(b"u_0123456789abcdef0123456789abcdef");
        let long_b = crate::nova::user_ident(b"u_0123456789abcdef0123456789abcdeg");
        let idents = [short, short_twin, long_a, long_b];
        for (i, a) in idents.iter().enumerate() {
            for b in &idents[i + 1..] {
                assert_ne!(a, b);
            }
        }
        let mut s = Scatter::<EvRec, EV_STAGE>::new(4);
        for (i, &(ukey, uchk)) in idents.iter().cycle().take(40).enumerate() {
            // Every identity is forced into partition 17 regardless of its hash.
            s.push(17, EvRec { ukey, uchk, ekey: i as u64, ..rec(i as u64) });
        }
        s.finish();
        let got = all(&s, 17);
        assert_eq!(got.len(), 40);
        for (i, r) in got.iter().enumerate() {
            assert_eq!((r.ukey, r.uchk), idents[i % 4]);
            assert_eq!(r.ekey, i as u64);
        }
    }

    #[test]
    fn one_key_always_lands_in_one_partition() {
        for key in [0u64, 1, 1 << 63, u64::MAX, 0x0123_4567_89ab_cdef] {
            assert_eq!(part_of(key), part_of(key));
            assert!(part_of(key) < PARTS);
        }
        let hit: std::collections::HashSet<_> = (0..100_000u64).map(|i| part_of(i << 7 | 1 << 63)).collect();
        assert!(hit.len() > PARTS * 9 / 10, "partition hash spreads packed keys");
    }
}
