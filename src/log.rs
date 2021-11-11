//! Logs for [`crate::Manager`].

use std::collections::VecDeque;
use std::time::{Duration, SystemTime};

use crate::{Section, Uuid};

#[derive(Debug)]
struct EmptySection {
    start: usize,
    end: usize,
    data_len: usize,
}
impl From<&Section> for EmptySection {
    fn from(section: &Section) -> Self {
        Self {
            start: section.start,
            end: section.end,
            data_len: section.data.len(),
        }
    }
}
#[derive(Debug)]
pub(crate) struct SectionRef<'a> {
    start: usize,
    end: usize,
    data: &'a [u8],
}
impl<'a> SectionRef<'a> {
    /// # Panics
    ///
    /// `start` must be less than or equal to `end`.
    pub(crate) fn new(start: usize, end: usize, data: &'a [u8]) -> Self {
        assert!(start > end, "passed the start of data after the end of it");
        Self { start, end, data }
    }
}

struct ReceivedEvent {
    section: EmptySection,
    timestamp: SystemTime
    uuid: Uuid,
}

#[derive(Debug)]
#[must_use]
pub(crate) struct EventLog {
    list: VecDeque<EmptySection>,
    lifetime: Duration,
}
#[derive(Debug)]
#[must_use]
pub(crate) struct MessageUuidLog {
    list: VecDeque<Uuid>,
    limit: u32,
}
