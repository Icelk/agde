//! Utilities for working with the diff library, currently [`den`].
//!
//! These functions are wrappers for integrating `agde` with it's diff library.
//! It handles converting data structures so the two can efficiently work together.

use crate::VecSection;
use den::{Difference, Segment, SegmentUnknown, Signature};

/// Creates a granular [`Difference`] between `base` and `target`.
///
/// If you [`Difference::apply`] this on `base`, you **should** get `target`.
pub fn diff(base: &[u8], target: &[u8]) -> Difference {
    let mut sig = Signature::new(256);
    sig.write(base);
    let sig = sig.finish();
    let rough_diff = sig.diff(target);
    #[allow(clippy::let_and_return)]
    let granular_diff = rough_diff
        .minify(8, base)
        .expect("The way we are using the function, this should never err.");
    granular_diff
}

/// Converts the `diff` to a list of [`VecSection`] using `base` to fill in gaps where the
/// representations don't work together.
#[must_use]
pub fn convert_to_sections(diff: Difference, base: &[u8]) -> Vec<VecSection> {
    fn apply_reference(
        segment_start: usize,
        segment_end: usize,
        last_unknown: &mut Option<SegmentUnknown>,
        last_ref_end: &mut Option<usize>,
        sections: &mut Vec<VecSection>,
        base: &[u8],
    ) {
        if let Some(mut last) = last_unknown.take() {
            if segment_start < last_ref_end.unwrap_or(0) {
                // We are moving back in the file. Not allowed using `Section`s.
                let base = &base[segment_start..segment_end];
                last.data_mut().extend(base);
                *last_unknown = Some(last);
            } else {
                let start = last_ref_end.unwrap_or(0);
                let end = segment_start;
                let section = VecSection::new(start, end, last.into_data());
                sections.push(section);
            }
        }
        *last_ref_end = Some(segment_end);
    }

    let block_size = diff.block_size();
    let mut sections = Vec::with_capacity(diff.segments().iter().fold(0, |acc, segment| {
        if let Segment::Unknown(_) = segment {
            acc + 1
        } else {
            acc
        }
    }));

    let mut last_unknown = None;
    let mut last_ref_end = None;
    for segment in diff.into_segments() {
        match segment {
            Segment::Unknown(seg) => last_unknown = Some(seg),
            Segment::Ref(seg) => apply_reference(
                seg.start(),
                seg.end(block_size),
                &mut last_unknown,
                &mut last_ref_end,
                &mut sections,
                base,
            ),
            Segment::BlockRef(seg) => apply_reference(
                seg.start(),
                seg.end(block_size),
                &mut last_unknown,
                &mut last_ref_end,
                &mut sections,
                base,
            ),
        }
    }
    if let Some(last) = last_unknown {
        let start = last_ref_end.unwrap_or(0);
        let end = base.len();
        let section = VecSection::new(start, end, last.into_data());
        sections.push(section);
    }
    sections
}
