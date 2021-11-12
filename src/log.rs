//! Logs for [`crate::Manager`].

use std::collections::VecDeque;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::{Event, Section, Uuid};

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
    /// A [`Duration`] of time after UNIX_EPOCH.
    timestamp: Duration,
    uuid: Uuid,
}

pub enum Error {
    /// The given [`Event`] has occurred in the future!
    EventInFuture,
}

#[derive(Debug)]
#[must_use]
pub(crate) struct EventLog {
    list: VecDeque<ReceivedEvent>,
    lifetime: Duration,
}
impl EventLog {
    pub(crate) fn new(lifetime: Duration) -> Self {
        Self {
            list: VecDeque::new(),
            lifetime,
        }
    }
    /// Requires the `self.list` is sorted.
    fn trim(&mut self) {
        let now = SystemTime::now();
        let since_epoch = now.duration_since(UNIX_EPOCH).unwrap_or(Duration::ZERO);
        let limit = since_epoch
            .checked_sub(self.lifetime)
            .unwrap_or(Duration::ZERO);

        loop {
            let last = self.list.back();
            if let Some(last) = last {
                if last.timestamp < limit {
                    self.list.pop_back();
                    continue;
                }
            }
            break;
        }
    }
    pub fn insert(&mut self, event: &Event,uuid: Uuid, timestamp: SystemTime) -> Result<(), Error> {
        let now = SystemTime::now();
        // The timestamp is after now!
        if (timestamp
            .checked_sub(Duration::new(10, 0))
            .unwrap_or(Duration::ZERO))
        .duration_since(now)
        .ok()
        {
            return Err(Error::EventInFuture)
        }
        // `TODO` represent as a EmptyEvent.
        // How to handle moves in [`EventSorter`] later?
        let section = EmptySection {
            start: event
        }
        let received = ReceivedEvent {
            timestamp,
            uuid,
        }
    }
}

#[derive(Debug)]
#[must_use]
pub(crate) struct MessageUuidLog {
    list: VecDeque<Uuid>,
    limit: u32,
}
impl MessageUuidLog {
    pub(crate) fn new(limit: u32) -> Self {
        Self {
            list: VecDeque::new(),
            limit,
        }
    }
    pub(crate) fn trim(&mut self) {}
}
