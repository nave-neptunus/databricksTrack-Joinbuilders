//! Scan 64-byte groups into a short, ordered tape before splitting event rows.

use crate::nova::{EventSink, Fields};
use std::arch::x86_64::*;

const WIDTH: usize = 64;
const BATCH: usize = 2048;
const COMMAS: usize = 6;
const GUARD: usize = COMMAS + 1;

// Offsets identify the byte after a delimiter. The low bit marks whether
// that byte is a field's opening quote.
struct Row<'input, 'tape> {
    input: &'input [u8],
    start: u32,
    cuts: &'tape [u32; COMMAS],
    next: u32,
}

impl<'a> Fields<'a> for Row<'a, '_> {
    #[inline]
    fn field<const I: usize>(&self) -> &'a [u8] {
        const { assert!(I <= COMMAS) };
        let left = if I == 0 { self.start } else { self.cuts[I - 1] };
        let right = if I == COMMAS { self.next } else { self.cuts[I] };
        let wrapped = (left & 1) as usize;
        let from = (left >> 1) as usize + wrapped;
        let to = (right >> 1) as usize - 1 - wrapped;
        // Both tape positions delimit a validated field in the input.
        unsafe { self.input.get_unchecked(from..to) }
    }
}

pub(super) fn available() -> bool {
    is_x86_feature_detected!("bmi1") && is_x86_feature_detected!("pclmulqdq")
}

pub(super) fn scan<'a>(input: &'a [u8], sink: &mut impl EventSink<'a>) {
    if input.len() < (u32::MAX >> 1) as usize && available() {
        // AVX2 is required at compile time. The other two features were checked.
        return unsafe { scan_blocks(input, sink) };
    }
    crate::nova::scan_event_rows(input, sink);
}

#[inline]
fn start_marker(input: &[u8], index: usize) -> u32 {
    ((index as u32) << 1) | u32::from(input.get(index) == Some(&b'"'))
}

#[inline]
unsafe fn append_offsets(
    tape: *mut u32,
    mut cursor: usize,
    mut bits: u64,
    quote_after: u64,
    base: u32,
) -> usize {
    while bits != 0 {
        let bit = bits.trailing_zeros();
        let wrapped = ((quote_after >> bit) as u32) & 1;
        // The caller reserves a slot for each possible set bit.
        unsafe { tape.add(cursor).write(((base + bit + 1) << 1) | wrapped) };
        cursor += 1;
        bits &= bits - 1;
    }
    cursor
}

#[target_feature(enable = "avx2,bmi1,pclmulqdq")]
unsafe fn scan_blocks<'a>(input: &'a [u8], sink: &mut impl EventSink<'a>) {
    let mut commas = [0u32; BATCH + 2 * GUARD];
    let mut lines = [0u32; BATCH];
    let mut cursor = 0;
    let mut start = start_marker(input, 0);
    let mut comma_count = 0;
    let mut in_quotes = 0u64;
    let mut after_separator = 1;
    while input.len() - cursor >= WIDTH {
        let limit = (input.len() - cursor).min(BATCH) / WIDTH * WIDTH + cursor;
        let mut line_count = 0;
        let mut recover = false;
        while cursor < limit {
            // Both vectors are completely inside the input. A byte of lookahead
            // handles quote endpoints on lane 63.
            let (lo, hi) = unsafe {
                let p = input.as_ptr().add(cursor);
                (
                    _mm256_loadu_si256(p.cast()),
                    _mm256_loadu_si256(p.add(32).cast()),
                )
            };
            let mask = |byte| {
                let needle = _mm256_set1_epi8(byte as i8);
                let low = _mm256_movemask_epi8(_mm256_cmpeq_epi8(lo, needle)) as u32;
                let high = _mm256_movemask_epi8(_mm256_cmpeq_epi8(hi, needle)) as u32;
                u64::from(low) | (u64::from(high) << 32)
            };
            let comma = mask(b',');
            let newline = mask(b'\n');
            let quote = mask(b'"');
            let separators = comma | newline;
            let following = input.get(cursor + WIDTH).copied().unwrap_or(b'\n');
            let next_separator = u64::from(matches!(following, b',' | b'\n'));
            // Multiplication by a polynomial of ones gives the inclusive
            // prefix XOR in the low half. A previous open quote inverts it.
            let prefix =
                _mm_clmulepi64_si128::<0>(_mm_cvtsi64_si128(quote as i64), _mm_set1_epi64x(-1));
            let inside = (_mm_cvtsi128_si64(prefix) as u64) ^ in_quotes;
            let opening = quote & inside;
            let closing = quote ^ opening;
            let bad_open = opening & !((separators << 1) | after_separator);
            let bad_close = closing & !((separators >> 1) | (next_separator << 63));
            // This dialect forbids commas inside a value. LF always terminates
            // a physical row, including when an opening quote is unmatched.
            if (bad_open | bad_close | (separators & inside)) != 0 {
                recover = true;
                break;
            }
            let quote_after = (quote >> 1) | (u64::from(following == b'"') << 63);
            // Each tape sees at most one offset per byte in the batch. The
            // comma tape also includes up to GUARD offsets from a partial row.
            unsafe {
                comma_count = append_offsets(
                    commas.as_mut_ptr(),
                    comma_count,
                    comma & !inside,
                    quote_after,
                    cursor as u32,
                );
                line_count = append_offsets(
                    lines.as_mut_ptr(),
                    line_count,
                    newline,
                    quote_after,
                    cursor as u32,
                );
            }
            after_separator = separators >> 63;
            in_quotes = 0u64.wrapping_sub(inside >> 63);
            cursor += WIDTH;
        }

        // Guards allow the ordinary row path to load six comma offsets and
        // check the next one, even when this batch ends with too few commas.
        commas[comma_count..comma_count + GUARD].fill(u32::MAX);
        let mut used = 0;
        for &next in &lines[..line_count] {
            let from = (start >> 1) as usize;
            let to = (next >> 1) as usize - 1;
            // Newline offsets increase monotonically from a physical row start.
            let line = unsafe { input.get_unchecked(from..to) };
            // The sentinel region covers all seven reads even after the last
            // real comma. A row accepts exactly six commas before its newline.
            let cuts = unsafe { &*commas.as_ptr().add(used).cast::<[u32; COMMAS]>() };
            let following = unsafe { *commas.get_unchecked(used + COMMAS) };
            if cuts[COMMAS - 1] < next && following >= next {
                sink.row(
                    line,
                    Row {
                        input,
                        start,
                        cuts,
                        next,
                    },
                );
                used += COMMAS;
            } else {
                if let Some(fields) = crate::fields::<7>(line) {
                    sink.row(line, fields);
                }
                // The sentinel stops this loop even without any further commas.
                while unsafe { *commas.get_unchecked(used) } < next {
                    used += 1;
                }
            }
            start = next;
        }

        if recover {
            // Scan only the physical rows spanning the suspect vector. Resume
            // SIMD after the first LF at or after the end of that vector.
            let after = cursor + WIDTH;
            cursor =
                memchr::memchr(b'\n', &input[after..]).map_or(input.len(), |lf| after + lf + 1);
            exact(&input[(start >> 1) as usize..cursor], sink);
            start = start_marker(input, cursor);
            comma_count = 0;
            in_quotes = 0;
            after_separator = 1;
        } else {
            // At most seven outstanding commas are necessary across batches.
            // Seven are already enough to reject an overfull row.
            comma_count = (comma_count - used).min(GUARD);
            commas.copy_within(used..used + comma_count, 0);
        }
    }
    exact(&input[(start >> 1) as usize..], sink);
}

#[inline]
fn exact<'a>(input: &'a [u8], sink: &mut impl EventSink<'a>) {
    crate::for_lines(input, |line| {
        if let Some(fields) = crate::fields::<7>(line) {
            sink.row(line, fields);
        }
    });
}

pub(super) fn verify_adversarial() {
    #[derive(Default)]
    struct Collect<'a>(Vec<[&'a [u8]; 7]>);

    impl<'a> EventSink<'a> for Collect<'a> {
        fn row(&mut self, _: &'a [u8], fields: impl Fields<'a>) {
            self.0.push([
                fields.field::<0>(),
                fields.field::<1>(),
                fields.field::<2>(),
                fields.field::<3>(),
                fields.field::<4>(),
                fields.field::<5>(),
                fields.field::<6>(),
            ]);
        }
    }

    fn verify(bytes: &[u8]) {
        let mut expected = Collect::default();
        let mut actual = Collect::default();
        exact(bytes, &mut expected);
        scan(bytes, &mut actual);
        assert_eq!(actual.0, expected.0);
    }

    for width in 0..WIDTH {
        let prefix = "x".repeat(width);
        let row = format!("\"{prefix}\",u,\"\",,\"view\",p,\n");
        for middle in [
            "e,u,t,i,view,p,\n",
            "e,\"u,t\",i,view,p,\n",
            "e,u,t,i,view,p,\"bad\n",
            "e,u,t,i,view,p,\"bad\"\r\n",
            "e,\"u\"x,t,i,view,p,\n",
            "e,u,t,i,view,p\n",
            "e,u,t,i,view,p,,\n",
            "e,u,t,i,view,p,\"\"\"\n",
            "\n",
        ] {
            verify(format!("{row}{middle}{row}{row}e,\"u\",,i,view,p,\"\"").as_bytes());
        }
    }
    let field = "x".repeat(BATCH * 3 + 61);
    let extra = ",".repeat(BATCH * 2 + 7);
    verify(format!("\"{field}\",u,t,i,view,p,\n{extra}\ne,\"u\",t,i,view,p,\n").as_bytes());
    verify(b"");
}

#[cfg(test)]
#[test]
fn offset_tape_matches_exact_rows() {
    verify_adversarial();
}
