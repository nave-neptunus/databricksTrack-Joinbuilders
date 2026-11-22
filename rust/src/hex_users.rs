//! Exact identities for common lowercase hexadecimal user IDs. Only the table
//! stores the 128-bit identity; the rest of the pipeline keeps 64-bit handles.

use std::sync::atomic::{
    AtomicBool, AtomicU64,
    Ordering::{Acquire, Relaxed, Release},
};

struct Entry {
    state: AtomicU64,
    high: AtomicU64,
    low: AtomicU64,
}

pub(super) struct HexUsers {
    entries: Vec<Entry>,
    overflow: AtomicBool,
}

fn identity(bytes: &[u8]) -> Option<(u64, u64)> {
    if bytes.len() != 33 || bytes[0] != b'u' {
        return None;
    }
    let mut words = [0u64; 2];
    for (word, digits) in words.iter_mut().zip(bytes[1..].chunks_exact(16)) {
        for &digit in digits {
            let value = match digit {
                b'0'..=b'9' => digit - b'0',
                b'a'..=b'f' => digit - b'a' + 10,
                _ => return None,
            };
            *word = (*word << 4) | u64::from(value);
        }
    }
    Some((words[0], words[1]))
}

impl HexUsers {
    pub(super) fn new(expected: usize) -> Self {
        let capacity = expected.saturating_mul(2).max(16).next_power_of_two();
        let entries = (0..capacity)
            .map(|_| Entry {
                state: AtomicU64::new(0),
                high: AtomicU64::new(0),
                low: AtomicU64::new(0),
            })
            .collect();
        Self {
            entries,
            overflow: AtomicBool::new(false),
        }
    }

    pub(super) fn overflowed(&self) -> bool {
        self.overflow.load(Relaxed)
    }

    pub(super) fn intern(&self, bytes: &[u8]) -> Option<u64> {
        if self.overflowed() {
            return None;
        }
        let (high, low) = identity(bytes)?;
        let mask = self.entries.len() - 1;
        // Mix both halves before choosing a slot. Sequential low halves would
        // otherwise share most home slots when the high half is constant.
        let mixed = (high ^ low.rotate_left(29)).wrapping_mul(0xd6e8_feb8_6659_fd93);
        let start = (mixed ^ (mixed >> 32)) as usize & mask;
        for step in 0..self.entries.len() {
            let index = (start + step) & mask;
            let entry = &self.entries[index];
            let mut state = entry.state.load(Acquire);
            if state == 0 {
                match entry.state.compare_exchange(0, 1, Acquire, Acquire) {
                    Ok(_) => {
                        entry.high.store(high, Relaxed);
                        entry.low.store(low, Relaxed);
                        entry.state.store(2, Release);
                        return Some(Self::handle(index));
                    }
                    Err(current) => state = current,
                }
            }
            while state == 1 {
                std::hint::spin_loop();
                state = entry.state.load(Acquire);
            }
            if entry.high.load(Relaxed) == high && entry.low.load(Relaxed) == low {
                return Some(Self::handle(index));
            }
        }
        // The caller declines before writing output. A generic handle here is
        // safe only for completing that declined attempt; slow.rs reruns exactly.
        self.overflow.store(true, Relaxed);
        None
    }

    fn handle(index: usize) -> u64 {
        // Odd source ordinals are disjoint from the generic interner's even
        // ordinals. Odd multiplication is bijective over the low 63 bits.
        let ordinal = ((index as u64 + 1) << 1) | 1;
        ordinal.wrapping_mul(0x9e37_79b9_7f4a_7c15) & (u64::MAX >> 1)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn complete_identity_and_spelling_are_preserved() {
        let table = HexUsers::new(8);
        let first = b"u0000000000000000000000000000000a";
        let second = b"u1000000000000000000000000000000a";
        assert_eq!(table.intern(first), table.intern(&first.to_vec()));
        assert_ne!(table.intern(first), table.intern(second));
        assert_eq!(table.intern(b"u0000000000000000000000000000000A"), None);
        assert_eq!(table.intern(b"u0000000000000000000000000000000g"), None);
        assert_eq!(table.intern(b"e0000000000000000000000000000000a"), None);
    }

    #[test]
    fn concurrent_insertions_publish_one_handle() {
        let table = HexUsers::new(8);
        std::thread::scope(|scope| {
            let workers: Vec<_> = (0..8)
                .map(|_| {
                    scope.spawn(|| {
                        (0..100)
                            .map(|_| table.intern(b"u0000000000000000000000000000000a").unwrap())
                            .collect::<Vec<_>>()
                    })
                })
                .collect();
            let expected = table.intern(b"u0000000000000000000000000000000a").unwrap();
            for worker in workers {
                assert!(
                    worker
                        .join()
                        .unwrap()
                        .into_iter()
                        .all(|handle| handle == expected)
                );
            }
        });
    }

    #[test]
    fn saturated_table_declines_without_repeated_full_table_scans() {
        let table = HexUsers::new(1);
        assert!(table.intern(b"u00000000000000000000000000000000").is_some());
        for value in 1..16 {
            assert!(table.intern(format!("u{value:032x}").as_bytes()).is_some());
        }
        assert_eq!(table.intern(b"u00000000000000000000000000000010"), None);
        assert!(table.overflowed());
        assert_eq!(table.intern(b"u00000000000000000000000000000000"), None);
    }
}
