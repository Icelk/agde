//! Logs for [`crate::Manager`].

use std::collections::VecDeque;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::{EmptySection, Event, Section, Uuid};

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

enum EmptyEvent {
    Modify(String),
}

#[derive(Debug)]
struct ReceivedEvent {
    event: Event<EmptySection>,
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
    list: Vec<ReceivedEvent>,
    lifetime: Duration,
}
impl EventLog {
    pub(crate) fn new(lifetime: Duration) -> Self {
        Self {
            list: Vec::new(),
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

        let mut to_drop = 0;

        loop {
            let last = self.list.get(to_drop);
            if let Some(last) = last {
                if last.timestamp < limit {
                    to_drop += 1;
                    continue;
                }
            }
            break;
        }

        drop(self.list.drain(..to_drop));
    }
    pub fn insert(
        &mut self,
        event: &Event<impl Section>,
        uuid: Uuid,
        timestamp: Duration,
    ) -> Result<(), Error> {
        use std::cmp::Ordering::*;

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or(Duration::ZERO);
        // The timestamp is after now!
        if (timestamp
            .checked_sub(Duration::new(10, 0))
            .unwrap_or(Duration::ZERO))
        .checked_sub(now)
        .is_some()
        {
            return Err(Error::EventInFuture);
        }
        let event = event.into();
        let received = ReceivedEvent {
            event,
            timestamp,
            uuid,
        };
        self.list.push(received);
        self.list
            .sort_by(|a, b| match a.timestamp.cmp(&b.timestamp) {
                Equal => a.uuid.cmp(&b.uuid),
                ord => ord,
            });
        Ok(())
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
