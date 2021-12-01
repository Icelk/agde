//! Logs for [`crate::Manager`].

use std::collections::{HashMap, VecDeque};
use std::hash::Hasher;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use twox_hash::xxh3::HasherExt;

use crate::{
    dur_now, ApplyError, DataSection, EmptySection, Event, EventKind, Section, SliceBuf, Uuid,
};

/// A received event.
///
/// Contains the necessary metadata to sort the event.
/// Does not contain any raw data, as that's calculated by undoing all the events.
#[derive(Debug)]
#[must_use]
struct ReceivedEvent {
    event: Event<EmptySection>,
    /// A [`Duration`] of time after UNIX_EPOCH.
    /// [`Event::timestamp`]
    timestamp: Duration,
    /// Message UUID.
    uuid: Uuid,
}
impl PartialEq for ReceivedEvent {
    fn eq(&self, other: &Self) -> bool {
        self.timestamp == other.timestamp && self.uuid == other.uuid
    }
}
impl Eq for ReceivedEvent {}
impl PartialOrd for ReceivedEvent {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for ReceivedEvent {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        use std::cmp::Ordering::Equal;

        match self.timestamp.cmp(&other.timestamp) {
            Equal => self.uuid.cmp(&other.uuid),
            ord => ord,
        }
    }
}

/// An error with editing of any of the logs.
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
        let since_epoch = dur_now();
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
    /// `timestamp` should be the one in [`Event::timestamp`]
    pub(crate) fn insert(
        &mut self,
        event: &Event<impl Section>,
        message_uuid: Uuid,
    ) -> Result<(), Error> {
        let now = dur_now();
        // The timestamp is after now!
        if (event
            .timestamp()
            .checked_sub(Duration::new(10, 0))
            .unwrap_or(Duration::ZERO))
        .checked_sub(now)
        .is_some()
        {
            return Err(Error::EventInFuture);
        }
        let timestamp = event.timestamp();
        let event = event.into();
        let received = ReceivedEvent {
            event,
            timestamp,
            uuid: message_uuid,
        };
        self.list.push(received);
        self.list.sort();
        self.trim();
        Ok(())
    }
    pub(crate) fn event_applier<'a, S: DataSection>(
        &'a mut self,
        event: &'a Event<S>,
        message_uuid: Uuid,
    ) -> EventApplier<'a, S> {
        fn slice_start(log: &EventLog, resource: &str, uuid: Uuid) -> usize {
            // `pos` is from the end of the list.
            for (pos, event) in log.list.iter().enumerate().rev() {
                if event.uuid == uuid && event.event.resource() == resource {
                    // Match!
                    // Also takes the current event in the slice.
                    // ↑ is however not true for the backup return below.
                    return pos;
                }
            }
            // Event is not in list.
            // This is a slow push.
            0
        }
        fn name<'a>(events: &'a [ReceivedEvent], resource: &'a str) -> Option<&'a str> {
            for log_event in events.iter() {
                if log_event.event.resource() == resource {
                    match &log_event.event.inner() {
                        EventKind::Delete(_) | EventKind::Create(_) => return None,
                        // Do nothing; the file is just modified.
                        EventKind::Modify(_) => {}
                    }
                }
            }

            Some(resource)
        }

        let slice_index = slice_start(self, event.resource(), message_uuid);

        if let EventKind::Delete(delete_ev) = event.inner() {
            if let Some(successor) = delete_ev.successor() {
                for received in &mut self.list[slice_index..] {
                    if received.uuid == message_uuid {
                        continue;
                    }

                    if received.event.resource() == event.resource() {
                        match received.event.inner_mut() {
                            EventKind::Delete(ev) => {
                                if Some(successor) == ev.successor() {
                                    ev.resource = ev.successor.take().unwrap();
                                } else {
                                    ev.resource = successor.into();
                                }
                            }
                            EventKind::Modify(ev) => {
                                ev.resource = successor.into();
                            }
                            EventKind::Create(_) => break,
                        }
                    }
                }
            }
        }

        let slice = &self.list[slice_index..];
        let resource = name(slice, event.resource());
        // Upholds the contracts described in the comment before [`EventApplier`].
        EventApplier {
            events: slice,
            modern_resource_name: resource,
            event,
        }
    }
}

/// A helper-struct that apples an event.
///
/// It's main function is to unwind the log of events, apply the new event (it might have occurred
/// before any of the other events) and then apply the unwound events.
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
    #[must_use]
    pub fn resource(&self) -> Option<&str> {
        self.modern_resource_name
    }
    /// Gets a reference to the event to be applied.
    pub fn event(&self) -> &Event<S> {
        self.event
    }
    /// Must only be called if [`Self::event`] is [`EventKind::Modify`] &
    /// [`Self::resource`] returns [`Some`],
    /// as you know which resource to load.
    /// If the target resource doesn't exist, don't call this.
    ///
    /// # Errors
    ///
    /// tl;dr, this can be unwrapped if you have called [`SliceBuf::extend_to_needed`] or
    /// [`Section::apply_len`] and made sure [`Self::event`] is a [`EventKind::Modify`].
    ///
    /// Returns [`ApplyError::InvalidEvent`] if this [`EventApplier`] wasn't instantiated
    /// with a [`EventKind::Modify`].
    ///
    /// Returns a [`ApplyError::BufTooSmall`] if the following predicate is not met.
    /// `resource` must be at least `max(resource.filled() + event.section().len_difference() + 1,
    /// event.section().end() + 1)`
    /// [`Section::apply_len`] guarantees this.
    pub fn apply<T: AsMut<[u8]> + AsRef<[u8]>>(
        &self,
        resource: &mut SliceBuf<T>,
    ) -> Result<(), ApplyError> {
        // Create a stack of the data of the reverted things.
        let mut reverted_stack = Vec::new();
        // Match only for the current resource.
        let current_resource_name = if let Some(name) = self.modern_resource_name {
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
                    for section in ev.sections().iter().rev() {
                        let section = section.revert(resource)?;
                        reverted_stack.push(section);
                    }
                }
                EventKind::Delete(_) | EventKind::Create(_) => unreachable!(
                    "Unexpected delete or create event in unwinding of event log.\
                    Please report this bug."
                ),
            }
        }
        // When back there, implement the event.
        for section in ev.sections() {
            section.apply(resource)?;
        }
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
pub(crate) enum EventUuidLogError {
    CountTooBig,
    /// Cutoff is missing in list.
    CutoffMissing,
}

#[derive(Debug)]
#[must_use]
pub(crate) struct EventUuidLog {
    // We need ordering and fast insertions at the front and deletions at the back, here a
    // `VecDeque` is a perfect fit!
    list: VecDeque<(Uuid, Duration)>,
    limit: u32,
}
impl EventUuidLog {
    pub(crate) fn new(limit: u32) -> Self {
        Self {
            list: VecDeque::new(),
            limit,
        }
    }
    pub(crate) fn insert(&mut self, message_uuid: Uuid, event_timestamp: Duration) {
        self.trim();
        self.list.push_front((message_uuid, event_timestamp));
        self.sort_last();
    }
    pub(crate) fn len(&self) -> usize {
        self.list.len()
    }
    /// # Panics
    ///
    /// Panics if `self.list` has no items.
    fn sort_last(&mut self) {
        use std::cmp::Ordering;

        let last = self.list.front().unwrap();
        let mut position = 0;
        for (pos, (uuid, timestamp)) in self.list.iter().enumerate() {
            match timestamp.cmp(&last.1) {
                Ordering::Less => break,
                Ordering::Greater => position = pos,
                Ordering::Equal => match uuid.cmp(&last.0) {
                    Ordering::Less | Ordering::Equal => break, // Well, fuck. Two identical event UUIDs. Not probable.
                    Ordering::Greater => position = pos,
                },
            }
        }
        self.list.swap(0, position);
    }
    pub(crate) fn trim(&mut self) {
        while self.len() > self.limit as usize {
            self.list.pop_back();
        }
    }
    pub(crate) fn cutoff_from_uuid(&self, cutoff_uuid: Uuid) -> Option<usize> {
        for (pos, (uuid, _)) in self.list.iter().copied().enumerate() {
            if uuid == cutoff_uuid {
                return Some(pos);
            }
        }
        None
    }
    pub(crate) fn cutoff_from_time(&self, cutoff: Duration) -> Option<usize> {
        for (pos, (_, time)) in self.list.iter().copied().enumerate() {
            if time <= cutoff {
                return Some(pos);
            }
        }
        None
    }
    /// Returns an appropriate cutoff for [`Self::get`] represented as
    /// it's position from the start (front) of the list and the target cutoff timestamp.
    ///
    /// Returns [`None`] if `self.list` has no elements.
    pub(crate) fn appropriate_cutoff(&self) -> Option<(usize, Duration)> {
        let now = dur_now();
        let target = now - Duration::from_secs(2);
        self.cutoff_from_time(target).map(|cutoff| (cutoff, target))
    }
    /// Gets the hashed log with `count` events and `cutoff` as the latest event to be included.
    ///
    /// # Errors
    ///
    /// `cutoff` must exist in the internal list and
    /// `count` events must exist in the remaining list.
    pub(crate) fn get(
        &self,
        count: u32,
        cutoff: usize,
        cutoff_timestamp: Duration,
    ) -> Result<EventUuidLogCheck, EventUuidLogError> {
        let uuid = if let Some(uuid) = self.list.get(cutoff) {
            uuid.0
        } else {
            return Err(EventUuidLogError::CutoffMissing);
        };
        let end = cutoff;
        let start = if let Some(start) = end.checked_sub(count as usize) {
            start
        } else {
            return Err(EventUuidLogError::CountTooBig);
        };
        let iter = self.list.range(start..end);
        let mut hasher = twox_hash::xxh3::Hash128::default();
        for (uuid, _) in iter {
            hasher.write(&uuid.inner().to_le_bytes());
        }
        let hash = hasher.finish_ext();

        let check = EventUuidLogCheck {
            log_hash: hash.to_le_bytes(),
            count,
            cutoff: uuid,
            cutoff_timestamp,
        };
        Ok(check)
    }
}
/// A check of the event UUID log.
///
/// A discrepancy indicates differing thought of what the data should be.
/// Often, a [`crate::MessageKind::HashCheck`] is sent if that's the case.
#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Clone)]
#[must_use]
pub struct EventUuidLogCheck {
    log_hash: [u8; 16],
    count: u32,
    cutoff: Uuid,
    cutoff_timestamp: Duration,
}
impl EventUuidLogCheck {
    /// Gets the count of events to be included, temporally before the [`Self::cutoff`].
    #[must_use]
    pub fn count(&self) -> u32 {
        self.count
    }
    /// Gets the UUID of the latest event to be included.
    pub fn cutoff(&self) -> Uuid {
        self.cutoff
    }
    /// Gets the timestamp before or at which all events are included.
    #[must_use]
    pub fn cutoff_timestamp(&self) -> Duration {
        self.cutoff_timestamp
    }
    /// Gets a reference to the 16-byte long hash.
    pub fn hash(&self ) -> &[u8; 16] {
        &self.log_hash
    }
}
/// The action to execute after receiving a [`crate::MessageKind::EventUuidLogCheck`].
#[derive(Debug)]
#[must_use]
pub enum EventUuidLogCheckAction {
    /// The logs match. Send a [`crate::MessageKind::EventUuidLogCheckReply`] with this
    /// [`EventUuidLogCheck`].
    Send(EventUuidLogCheck),
    /// The logs don't match. Do the same as with [`Self::Send`] AND wait a few seconds (e.g. 10)
    /// and then do a full check of the files. Something's not adding up.
    SendAndFurtherCheck(EventUuidLogCheck),
    /// Our log was too small. We can therefore not participate in this exchange.
    /// Do nothing.
    Nothing,
}
type Conversation = HashMap<Uuid, EventUuidLogCheck>;
#[derive(Debug)]
pub(crate) struct EventUuidReplies {
    conversations: HashMap<Uuid, Conversation>,
}
impl EventUuidReplies {
    pub(crate) fn new() -> Self {
        Self {
            conversations: HashMap::new(),
        }
    }
    /// `conversation` is the UUID of the current conversation. `check` is the data `source` sent.
    pub(crate) fn insert(&mut self, conversation: Uuid, check: EventUuidLogCheck, source: Uuid) {
        let conversation = self
            .conversations
            .entry(conversation)
            .or_insert_with(HashMap::new);
        conversation.insert(source, check);
    }
    pub(crate) fn get(&self, conversation: Uuid) -> Option<&HashMap<Uuid, EventUuidLogCheck>> {
        self.conversations.get(&conversation)
    }
}
impl Default for EventUuidReplies {
    fn default() -> Self {
        Self::new()
    }
}
