use super::{Click, P, mask_rows};

/// A worker appends straight to its person partitions. Small and unusually
/// busy partitions use ordinary vectors. The mapping keeps the common
/// partition prefixes stable as they fill.
pub(super) struct Partitions {
    storage: Option<memmap2::MmapMut>,
    stride: usize,
    used: Vec<usize>,
    overflow: Vec<Vec<Click>>,
}

impl Partitions {
    pub(super) fn new(stride: usize) -> Self {
        let storage = (stride != 0)
            .then(|| memmap2::MmapMut::map_anon(stride * P * size_of::<Click>()).ok())
            .flatten();
        #[cfg(target_os = "linux")]
        if let Some(mapping) = &storage {
            let _ = mapping.advise(memmap2::Advice::HugePage);
        }
        let stride = if storage.is_some() { stride } else { 0 };
        Self {
            storage,
            stride,
            used: vec![0; P],
            overflow: (0..P).map(|_| Vec::new()).collect(),
        }
    }

    #[inline]
    pub(super) fn push(&mut self, click: Click) {
        let partition = click.person as usize & (P - 1);
        let used = &mut self.used[partition];
        if *used < self.stride {
            let base = self.storage.as_mut().unwrap().as_mut_ptr().cast::<Click>();
            // The worker only writes to the next uninitialized slot in its own mapping.
            unsafe { base.add(partition * self.stride + *used).write(click) };
            *used += 1;
        } else {
            self.overflow[partition].push(click);
        }
    }

    pub(super) fn len(&self, partition: usize) -> usize {
        self.used[partition] + self.overflow[partition].len()
    }

    #[inline]
    pub(super) fn records(&self, partition: usize) -> [&[Click]; 2] {
        let prefix = self.storage.as_ref().map_or(&[][..], |mapping| {
            // Only the initialized prefix is visible to readers.
            unsafe {
                std::slice::from_raw_parts(
                    mapping
                        .as_ptr()
                        .cast::<Click>()
                        .add(partition * self.stride),
                    self.used[partition],
                )
            }
        });
        [prefix, &self.overflow[partition]]
    }
}

#[inline]
pub(super) fn rows<'a>(bytes: &'a [u8], mut emit: impl FnMut([&'a [u8]; 4])) {
    #[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
    if std::is_x86_feature_detected!("bmi2") {
        // The build requires AVX2 and the runtime check above covers BMI2.
        return unsafe { avx2_rows(bytes, emit) };
    }
    mask_rows::<4>(bytes, |_, values| emit(values));
}

#[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
#[target_feature(enable = "avx2,bmi2")]
unsafe fn avx2_rows<'a>(mut bytes: &'a [u8], mut emit: impl FnMut([&'a [u8]; 4])) {
    use std::arch::x86_64::*;

    let compare = |a, b, byte| {
        let marker = _mm256_set1_epi8(byte as i8);
        let lo = _mm256_movemask_epi8(_mm256_cmpeq_epi8(a, marker)) as u32;
        let hi = _mm256_movemask_epi8(_mm256_cmpeq_epi8(b, marker)) as u32;
        u64::from(lo) | (u64::from(hi) << 32)
    };
    while bytes.len() >= 64 {
        let (a, b) = unsafe {
            let ptr = bytes.as_ptr();
            (
                _mm256_loadu_si256(ptr.cast()),
                _mm256_loadu_si256(ptr.add(32).cast()),
            )
        };
        let lines = compare(a, b, b'\n');
        if lines == 0 {
            let end = memchr::memchr(b'\n', &bytes[64..]).map_or(bytes.len(), |n| n + 64);
            if let Some(values) = super::row_fields::<4>(&bytes[..end]) {
                emit(values);
            }
            bytes = &bytes[(end + 1).min(bytes.len())..];
            continue;
        }
        let end = lines.trailing_zeros() as usize;
        let line = &bytes[..end];
        bytes = &bytes[end + 1..];
        let before_newline = (1u64 << end) - 1;
        let commas = compare(a, b, b',') & before_newline;
        if commas.count_ones() != 3 {
            continue;
        }
        let separators = commas | (1 << end);
        // No click can survive an empty field. Nonempty raw fields also mean
        // each field has its own endpoints in the quote masks.
        if commas & 1 != 0 || (commas << 1) & separators != 0 {
            continue;
        }
        let quotes = compare(a, b, b'"') & before_newline;
        let opens = (commas << 1) | 1;
        let closes = separators >> 1;
        let wrapped = _pext_u64(quotes, opens);
        if quotes & !(opens | closes) != 0
            || wrapped != _pext_u64(quotes, closes)
            || quotes.count_ones() != 2 * wrapped.count_ones()
        {
            continue;
        }
        let first = commas.trailing_zeros() as usize;
        let remaining = commas & (commas - 1);
        let second = remaining.trailing_zeros() as usize;
        let third = (remaining & (remaining - 1)).trailing_zeros() as usize;
        let starts = [0, first + 1, second + 1, third + 1];
        let ends = [first, second, third, end];
        emit(std::array::from_fn(|i| {
            let trim = ((wrapped >> i) & 1) as usize;
            // The quote checks above establish matching, distinct endpoints.
            unsafe { line.get_unchecked(starts[i] + trim..ends[i] - trim) }
        }));
    }
    mask_rows::<4>(bytes, |_, values| emit(values));
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn click_rows_match_scalar_for_nonempty_fields() {
        let mut bytes = Vec::new();
        for length in 0..150 {
            let id = "k".repeat(length);
            for quotes in 0..16 {
                let start = bytes.len();
                for (i, field) in [id.as_str(), "u12345678", "c_2000", "1814775638000"]
                    .iter()
                    .enumerate()
                {
                    if i != 0 {
                        bytes.push(b',');
                    }
                    if quotes & (1 << i) != 0 {
                        bytes.push(b'"');
                    }
                    bytes.extend_from_slice(field.as_bytes());
                    if quotes & (1 << i) != 0 {
                        bytes.push(b'"');
                    }
                }
                bytes.push(b'\n');
                let mut malformed = bytes[start..].to_vec();
                let at = length.min(malformed.len() - 1);
                malformed[at] = b'"';
                bytes.extend(malformed);
            }
        }
        bytes.extend_from_slice(b"k,u,c,\n,k,c,t\nk,,c,t\nk,u,,t\nk,u,c,\"\"\nk,u,c,t");
        let mut expected = Vec::new();
        super::super::for_lines(&bytes, |line| {
            if let Some(fields) = super::super::fields::<4>(line)
                && fields.iter().all(|field| !field.is_empty())
            {
                expected.push(fields);
            }
        });
        let mut actual = Vec::new();
        rows(&bytes, |fields| {
            if fields.iter().all(|field| !field.is_empty()) {
                actual.push(fields);
            }
        });
        assert_eq!(actual, expected);
    }
}
