//! Utilities for working with some datatypes used by agde.

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

#[derive(Debug)]
struct Offset {
    idx: usize,
    len: usize,
    negative: bool,
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
    pub fn add_diff(&mut self, diff: &den::Difference, len_diff: isize) {
        enum Last {
            Ref { end: usize },
            Unknown { len: usize },
        }

        let mut last = Last::Unknown { len: 0 };

        for seg in diff.segments() {
            match seg {
                den::Segment::Ref(seg) => {
                    match last {
                        Last::Ref { end } => {
                            if seg.start() >= end {
                                self.push(Offset {
                                    idx: seg.start(),
                                    len: seg.start() - end,
                                    negative: true,
                                });
                            }
                        }
                        Last::Unknown { len: 0 } => {}
                        Last::Unknown { len } => self.push(Offset {
                            idx: seg.start(),
                            len,
                            negative: false,
                        }),
                    }
                    last = Last::Ref {
                        end: seg.end(diff.block_size()),
                    };
                }
                den::Segment::Unknown(seg) => {
                    last = Last::Unknown {
                        len: seg.source().len(),
                    };
                }
            }
        }

        self.len_diff += len_diff;
    }
    /// Apply the offsets on `events` to adjust them to the events that have since changed
    /// ([`Self::add_diff`]).
    ///
    /// Will set all the [`Event`]s' `block_size`s to `1`.
    ///
    /// `events` should be an iterator over the [`Event`]s with the relevant [`Event::resource`].
    #[allow(clippy::missing_panics_doc)]
    pub fn apply<'a>(self, events: impl Iterator<Item = &'a mut Event>) {
        println!("Offsets: {self:#?}");

        for ev in events {
            let kind = ev.inner_mut();
            match kind {
                EventKind::Modify(ev) => {
                    ev.diff.with_block_size(1).unwrap();
                    let block_size = ev.diff().block_size();
                    let segments = ev.diff.segments_mut();

                    let mut cursor = 0;

                    for seg in segments {
                        match seg {
                            den::Segment::Ref(seg) => {
                                let mut start = seg.start();
                                let mut len = seg.len(block_size);
                                for offset in &self.offsets {
                                    // `TODO`: or should `seg.start()` actually be the moving
                                    // `start`?
                                    if offset.idx < seg.start() {
                                        if offset.negative {
                                            start = start.saturating_sub(offset.len);
                                        } else {
                                            start += offset.len;
                                        }
                                        continue;
                                    }
                                    if offset.idx <= seg.end(block_size) {
                                        if offset.negative {
                                            len = len.saturating_sub(offset.len);
                                        } else {
                                            len += offset.len;
                                        }
                                    }
                                }
                                *seg = seg.with_start(start).with_blocks(len);
                                cursor += seg.len(block_size);
                            }
                            den::Segment::Unknown(seg) => {
                                let mut blocks = seg.source().len();
                                for offset in &self.offsets {
                                    if offset.idx >= cursor
                                        && offset.idx < cursor + seg.source().len()
                                    {
                                        if offset.negative {
                                            blocks = blocks.saturating_sub(offset.len);
                                        } else {
                                            blocks += offset.len;
                                        }
                                    }
                                }
                                cursor += seg.source().len();
                            }
                        }
                    }

                    let len =
                        utils::iusize_add(ev.diff.original_data_len(), self.len_diff).unwrap_or(0);
                    ev.diff.set_original_data_len(len);
                }
                EventKind::Create(_) | EventKind::Delete(_) => {}
            }
        }
    }
}
impl Default for Offsets {
    fn default() -> Self {
        Self::new()
    }
}
