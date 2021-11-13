//! Logs for [`crate::Manager`].

use std::collections::VecDeque;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::{ApplyError, DataSection, EmptySection, Event, EventKind, Section, SliceBuf, Uuid};

#[derive(Debug)]
#[must_use]
struct ReceivedEvent {
    event: Event<EmptySection>,
    /// A [`Duration`] of time after UNIX_EPOCH.
    timestamp: Duration,
}

#[derive(Debug)]
pub enum Error {
    /// The given [`Event`] has occurred in the future!
    EventInFuture,
    /// The guarantees of [`Section`] don't hold up.
    SectionInvalid,
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
    /// `timestamp` should be the one in [`EventMessage`]
    pub(crate) fn insert(
        &mut self,
        event: &Event<impl Section>,
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
        let received = ReceivedEvent { event, timestamp };
        self.list.push(received);
        self.list
            .sort_by(|a, b| match a.timestamp.cmp(&b.timestamp) {
                Equal => a.event.uuid().cmp(&b.event.uuid()),
                ord => ord,
            });
        self.trim();
        Ok(())
    }
    /// `resource` should be part of the [`crate::Message`] with the `uuid`.
    /// `uuid` is the event's UUID, not the message's.
    // `TODO`: remove 'a for Event
    pub(crate) fn event_applier<'a, S: DataSection>(
        &'a mut self,
        event: &'a Event<S>,
    ) -> EventApplier<'a, S> {
        fn slice<'a>(log: &'a EventLog, resource: &str, uuid: Uuid) -> &'a [ReceivedEvent] {
            // `pos` is from the end of the list.
            for (pos, event) in log.list.iter().enumerate().rev() {
                if event.event.uuid() == uuid && event.event.resource() == resource {
                    // Match!
                    // Also takes the current event in the slice.
                    // ↑ is however not true for the backup return below.
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

            for log_event in events.iter().rev() {
                if log_event.event.resource() == resource {
                    match &log_event.event.inner() {
                        EventKind::Delete(_) => return None,
                        EventKind::Move(ev) => {
                            debug_assert_eq!(
                                ev.source(),
                                resource,
                                "The previous resource should be the same as in the move event.\
                                This is guaranteed by [`Manager::apply_event`]"
                            );
                            resource = &ev.target;
                        }
                        // Do nothing; the file is just modified.
                        EventKind::Modify(_) => {}
                    }
                }
            }

            Some(resource)
        }

        let slice = slice(self, event.resource(), event.uuid());
        let resource = name(slice, event.resource());

        match event.inner() {
            EventKind::Modify(_ev) => {
                // Upholds the contracts described in the comment before [`EventApplier`].
                EventApplier {
                    events: slice,
                    modern_resource_name: resource,
                    event,
                }
            }
            EventKind::Delete(_) => {
                // Upholds the contracts described in the comment before [`EventApplier`].
                EventApplier {
                    events: slice,
                    modern_resource_name: resource,
                    event,
                }
            }
            EventKind::Move(_) => {
                // Change history of the `resource` in the name loop,
                // change resource to `new_resource`
                //
                // Because the new version of the file, without the move, is the same, but on a
                // different path, ~~we can just move the current file to the new position?~~ No, but
                // check the following
                unimplemented!("See note above.");
            }
        }
    }
}

// Instances must contain the target event.
// Unwinding `events` may not return [`Event::Delete`] if `modern_resource_name` is [`Some`]
#[derive(Debug)]
#[must_use]
pub struct EventApplier<'a, S: DataSection> {
    /// Ordered from last (temporally).
    /// May not contain `Self::event`.
    events: &'a [ReceivedEvent],
    modern_resource_name: Option<&'a str>,
    /// Needed because the event might be sorted out of the list; slow push.
    event: &'a Event<S>,
}
impl<'a, S: DataSection> EventApplier<'a, S> {
    /// The name of the modified resource on the local store.
    /// If this is [`None`], the resource this event applies to has been deleted since.
    /// Don't call [`Self::apply`] if that's the case.
    pub fn resource(&self) -> Option<&str> {
        self.modern_resource_name
    }
    pub fn event(&self) -> &Event<S> {
        self.event
    }
    /// Must only be called if [`Self::event`] is [`EventKind::Modify`] &
    /// [`Self::resource`] returns [`Some`],
    /// as you know which resource to load.
    ///
    /// # Errors
    ///
    /// Returns a [`ApplyError::BufTooSmall`] if the following predicate is not met.
    /// `resource` must be at least `current_resource.len() + new_resource.section().len_difference()`
    pub fn apply(&self, resource: &mut SliceBuf) -> Result<(), ApplyError> {
        // Create a stack of the data of the reverted things.
        let mut reverted_stack = Vec::new();
        // Match only for the current resource.
        let mut current_resource_name = if let Some(name) = self.modern_resource_name {
            name
        } else {
            return Ok(());
        };
        let ev = if let EventKind::Modify(ev) = self.event.inner() {
            ev
        } else {
            return Err(ApplyError::InvalidEvent);
        };
        // On move events, only change resource.
        // On delete messages, panic. A bug.
        // ↑ should be contracted by the creator.
        for received_ev in self
            .events
            .iter()
            .rev()
            .take(self.events.len().saturating_sub(1))
        {
            if received_ev.event.resource() != current_resource_name {
                continue;
            }
            match received_ev.event.inner() {
                EventKind::Modify(ev) => {
                    let section = ev.section().revert(resource)?;
                    reverted_stack.push(section);
                }
                EventKind::Delete(_) => unreachable!(
                    "Unexpected delete event in unwinding of event log.\
                    Please report this bug."
                ),
                EventKind::Move(ev) => {
                    debug_assert_eq!(ev.target, current_resource_name);
                    // Since we're moving backward,
                    current_resource_name = ev.source();
                }
            }
        }
        // When back there, implement the event.
        ev.section().apply(resource)?;
        // Unwind the stack, redoing all the events.
        while let Some(section) = reverted_stack.pop() {
            section.apply(resource)?;
        }

        // How will the revert and implement functions on modify work?
        // Can revert be a inverse and just call implement?
        // We need to modify the whole buffer when reverting
        // Directly modify `resource` as the buffer.
        // Have a `PartiallyUnknown` data type for the Sections which have removed data.
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
