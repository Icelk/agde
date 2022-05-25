//! Utilities for working with some datatypes used by agde.

use den::{Difference, Segment};

use crate::{utils, Duration, Event, EventKind, SystemTime, UNIX_EPOCH};

/// Returns the time since [`UNIX_EPOCH`].
#[must_use]
pub fn dur_now() -> Duration {
    systime_to_dur(SystemTime::now())
}
/// Convert `dur` (duration since [`UNIX_EPOCH`]) to [`SystemTime`].
/// Very cheap conversion.
#[must_use]
pub fn dur_to_systime(dur: Duration) -> SystemTime {
    SystemTime::UNIX_EPOCH + dur
}
/// Convert `systime` to [`Duration`] (since [`UNIX_EPOCH`]).
/// Very cheap conversion.
#[must_use]
pub fn systime_to_dur(systime: SystemTime) -> Duration {
    systime.duration_since(UNIX_EPOCH).unwrap_or(Duration::ZERO)
}

/// `a-b` but with better overflow properties than `a as isize - b as isize`.
#[must_use]
#[allow(clippy::cast_possible_wrap)]
pub fn sub_usize(a: usize, b: usize) -> isize {
    if a > b {
        (a - b) as isize
    } else {
        -((b - a) as isize)
    }
}
/// `a+b` but with better overflow properties than `(a as isize + b) as usize`.
/// Returns [`None`] if `(-b) > a`
#[must_use]
#[allow(clippy::cast_sign_loss)]
pub fn iusize_add(a: usize, b: isize) -> Option<usize> {
    if b.is_negative() {
        let b = (-b) as usize;
        if b > a {
            None
        } else {
            Some(a - b)
        }
    } else {
        Some(a + b as usize)
    }
}
/// `a+b` but with better overflow properties than `(a as isize + b) as usize`.
/// Saturates the output to 0 if `a-b < 0`
#[must_use]
pub fn iusize_add_saturating(a: usize, b: isize) -> usize {
    iusize_add(a, b).unwrap_or(0)
}

#[derive(Debug)]
struct Offset {
    idx: usize,
    len: isize,
}

/// The offsets previous events cause on any future events which have not taken the previous ones
/// into account.
#[derive(Debug)]
#[must_use]
pub struct Offsets {
    offsets: Vec<Offset>,
    len_diff: isize,
}
impl Offsets {
    /// Create a new set of offsets for some old differences.
    pub fn new() -> Self {
        Self {
            offsets: Vec::with_capacity(8),
            len_diff: 0,
        }
    }
    fn push(&mut self, offset: Offset) {
        if offset.len == 0 {
            return;
        }
        match self
            .offsets
            .binary_search_by(|probe| probe.idx.cmp(&offset.idx))
        {
            Ok(idx) => {
                self.offsets[idx].len += offset.len;
            }
            Err(idx) => self.offsets.insert(idx, offset),
        }
    }
    /// Add another diff to us.
    ///
    /// `len_diff` is the difference in length before and after applying `diff` to your data, like
    /// `after - before`. Consider using [`utils::sub_usize`].
    pub fn add_diff(&mut self, diff: &Difference, len_diff: isize) {
        enum Last {
            Ref { end: usize },
            Unknown { len: usize },
        }

        let mut last = Last::Unknown { len: 0 };

        for seg in diff.segments() {
            match seg {
                Segment::Ref(seg) => {
                    match last {
                        Last::Ref { end } => {
                            self.push(Offset {
                                idx: seg.start(),
                                len: utils::sub_usize(end, seg.start()),
                            });
                        }
                        Last::Unknown { len: 0 } => {}
                        #[allow(clippy::cast_possible_wrap)]
                        Last::Unknown { len } => self.push(Offset {
                            idx: seg.start(),
                            len: len as _,
                        }),
                    }
                    last = Last::Ref {
                        end: seg.end(diff.block_size()),
                    };
                }
                Segment::Unknown(seg) => {
                    last = Last::Unknown {
                        len: seg.source().len(),
                    };
                }
            }
        }

        self.len_diff += len_diff;
    }

    /// Apply these offsets to a single `diff`.
    ///
    /// Use [`Self::apply`] for a more convenient function.
    #[allow(clippy::missing_panics_doc)]
    pub fn apply_single(&self, diff: &mut Difference) {
        diff.with_block_size(1).unwrap();
        let block_size = diff.block_size();

        let mut cursor = 0;

        for seg in diff.segments_mut() {
            match seg {
                Segment::Ref(seg) => {
                    let mut start = seg.start();
                    let mut len = seg.len(block_size);
                    for offset in &self.offsets {
                        // `TODO`: or should `seg.start()` actually be the moving
                        // `start`?
                        if offset.idx < seg.start() {
                            start = utils::iusize_add_saturating(start, offset.len);
                            continue;
                        }
                        if offset.idx <= seg.end(block_size) {
                            len = utils::iusize_add_saturating(len, offset.len);
                        }
                    }
                    *seg = seg.with_start(start).with_blocks(len);
                    cursor += seg.len(block_size);
                }
                Segment::Unknown(seg) => {
                    let mut blocks = seg.source().len();
                    for offset in &self.offsets {
                        if offset.idx >= cursor && offset.idx < cursor + seg.source().len() {
                            blocks = utils::iusize_add_saturating(blocks, offset.len);
                        }
                    }
                    cursor += seg.source().len();
                }
            }
        }

        let len = utils::iusize_add(diff.original_data_len(), self.len_diff).unwrap_or(0);
        diff.set_original_data_len(len);
    }
    /// Apply the offsets on `events` to adjust them to the events that have since changed
    /// ([`Self::add_diff`]).
    ///
    /// Will set all the [`Event`]s' `block_size`s to `1`.
    ///
    /// `events` should be an iterator over the [`Event`]s with the relevant [`Event::resource`].
    #[allow(clippy::missing_panics_doc)]
    pub fn apply<'a>(self, events: impl Iterator<Item = &'a mut Event>) {
        for ev in events {
            let kind = ev.inner_mut();
            match kind {
                EventKind::Modify(ev) => {
                    self.apply_single(&mut ev.diff);
                }
                EventKind::Create(_) | EventKind::Delete(_) => {}
            }
        }
    }

    /// Returns where `index` is after the diffs we are tracking are applied.
    ///
    /// This is provided on a best-effort basis, just like the rest of this struct.
    #[must_use]
    pub fn transform_index(&self, mut index: usize) -> usize {
        for offset in &self.offsets {
            if offset.idx <= index {
                index = utils::iusize_add_saturating(index, offset.len);
            }
        }
        index
    }
}
impl Default for Offsets {
    fn default() -> Self {
        Self::new()
    }
}

#[allow(clippy::cast_possible_wrap)]
mod tests {
    #[test]
    fn offsets() {
        use den::{Segment, SegmentRef, Signature};

        let diff = {
            let mut sig = Signature::new(8);
            sig.write(b"");
            let sig = sig.finish();
            let mut diff = sig.diff(b"");
            *diff.segments_mut() = [
                Segment::unknown("ye! "),
                Segment::Ref(SegmentRef {
                    start: 0,
                    block_count: 1,
                }),
                Segment::Ref(SegmentRef {
                    start: 16,
                    block_count: 1,
                }),
                Segment::Ref(SegmentRef {
                    start: 16,
                    block_count: 7,
                }),
                Segment::unknown("ou?"),
            ]
            .to_vec();
            diff
        };
        let mut total = 0_isize;

        let mut offsets = super::Offsets::new();
        offsets.add_diff(&diff, 0);
        for offset in offsets.offsets {
            total += offset.len;
        }
        assert_eq!(total, 4);
    }
}
