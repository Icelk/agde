//! Logs for [`crate::Manager`].

use std::collections::VecDeque;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::{EmptySection, Event, Section, Uuid};

#[derive(Debug)]
struct ReceivedEvent {
    event: Event<EmptySection>,
    /// A [`Duration`] of time after UNIX_EPOCH.
    timestamp: Duration,
    /// The [`Uuid`] of the message.
    uuid: Uuid,
}

#[derive(Debug)]
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
    pub(crate) fn insert(
        &mut self,
        event: &Event<impl Section>,
        uuid: Uuid,
        timestamp: Duration,
    ) -> Result<(), Error> {
        use std::cmp::Ordering::Equal;

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
        self.trim();
        Ok(())
    }
    /// `resource` should be part of the [`crate::Message`] with the `uuid`.
    pub(crate) fn event_applier<'a>(&'a self, resource: &'a str, uuid: Uuid) -> EventApplier<'a> {
        fn slice<'a>(log: &'a EventLog, resource: &str, uuid: Uuid) -> &'a [ReceivedEvent] {
            // `pos` is from the end of the list.
            for (pos, event) in log.list.iter().enumerate().rev() {
                if event.uuid == uuid && event.event.resource() == resource {
                    // Match!
                    let slice = &log.list[pos..];
                    return slice;
                }
            }
            // Event is not in list.
            // This is a slow push.
            &log.list
        }
        fn name<'a>(events: &'a [ReceivedEvent], resource: &'a str) -> Option<&'a str> {
            let mut resource = resource;

            for event in events.iter().rev() {
                if event.event.resource() == resource {
                    match &event.event {
                        Event::Delete(_) => return None,
                        Event::Move(ev) => {
                            debug_assert_eq!(
                                ev.resource, resource,
                                "The previous resource should be the same as in the move event.\
                                This is guaranteed by [`Manager::apply_event`]"
                            );
                            resource = &ev.target;
                        }
                        // Do nothing; the file is just modified.
                        Event::Modify(_) => {}
                    }
                }
            }

            Some(resource)
        }

        let slice = slice(self, resource, uuid);
        let resource = name(slice, resource);

        EventApplier {
            events: slice,
            modern_resource_name: resource,
        }
    }
}
#[derive(Debug)]
pub struct EventApplier<'a> {
    /// Ordered from last (temporally).
    events: &'a [ReceivedEvent],
    modern_resource_name: Option<&'a str>,
}
impl<'a> EventApplier<'a> {
    /// The name of the modified resource on the local store.
    /// If this is [`None`], the resource this event applies to has been deleted since.
    /// Don't call [`Self::apply`] if that's the case.
    pub fn resource(&self) -> Option<&str> {
        self.modern_resource_name
    }
    /// `resource` must be at least `current_resource.len() + (new_event.len() - (new_event.end() -
    /// new_event.start()))`
    pub fn apply(&self, resource: &mut [u8]) {
        // Create a stack of the data of the reverted things.
        // Match only for the current resource.
        // On move events, only change resource.
        // On delete messages, panic. A bug.
        // ↑ should be contracted by the creator. // Add note about this
        //
        // When back there, implement the event. Should be a function on modify.
        //
        // Unwind the stack, redoing all the events.

        // How will the revert and implement functions on modify work?
        // Can revert be a inverse and just call implement?
        // We need to modify the whole buffer when reverting
        // Directly modify `resource` as the buffer.
        // Have a `PartiallyUnknown` data type for the Sections which have removed data.
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
