//! Logs for [`crate::Manager`].

use std::cmp;
use std::collections::HashMap;
use std::fmt::Debug;
use std::hash::Hasher;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use twox_hash::xxh3::HasherExt;

use crate::event;
use crate::{event::dur_now, Event, EventKind, Uuid};

/// A received event.
///
/// Contains the necessary metadata to sort the event.
/// Does not contain any raw data, as that's calculated by undoing all the events.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[must_use]
pub(crate) struct ReceivedEvent {
    // `TODO`: store events with the unknown data segments containing no data.
    pub(crate) event: Event,
    /// Message UUID.
    pub(crate) uuid: Uuid,
}
impl PartialEq for ReceivedEvent {
    fn eq(&self, other: &Self) -> bool {
        self.event.timestamp() == other.event.timestamp() && self.uuid == other.uuid
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

        match self.event.timestamp().cmp(&other.event.timestamp()) {
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
}

/// An error during [`den::Difference::apply`] and [`crate::log::EventApplier::apply`].
#[derive(Debug)]
pub enum ApplyError {
    /// A reference in the difference is out of bounds.
    /// See [`den::ApplyError::RefOutOfBounds`].
    RefOutOfBounds,
    /// The function called must not be called on the current event.
    InvalidEvent,
}
impl From<den::ApplyError> for ApplyError {
    fn from(err: den::ApplyError) -> Self {
        match err {
            den::ApplyError::RefOutOfBounds => Self::RefOutOfBounds,
        }
    }
}

#[derive(Debug)]
#[must_use]
pub(crate) struct Log {
    pub(crate) list: Vec<ReceivedEvent>,
    pub(crate) lifetime: Duration,
    pub(crate) limit: u32,
}
impl Log {
    pub(crate) fn new(lifetime: Duration, limit: u32) -> Self {
        Self {
            list: Vec::new(),
            lifetime,
            limit,
        }
    }
    /// Get a reference to the event log's lifetime.
    #[inline]
    pub(crate) fn lifetime(&self) -> Duration {
        self.lifetime
    }
    /// Get the limit of the internal list.
    #[must_use]
    pub(crate) fn limit(&self) -> u32 {
        self.limit
    }
    /// Requires the `self.list` to be sorted.
    fn trim(&mut self) {
        let since_epoch = dur_now();
        let limit = since_epoch.saturating_sub(self.lifetime);

        let mut to_drop = 0;

        loop {
            let last = self.list.get(to_drop);
            if self.list.len() > self.limit as usize {
                to_drop += 1;
                continue;
            }
            if let Some(last) = last {
                if last.event.timestamp() < limit {
                    to_drop += 1;
                    continue;
                }
            }
            break;
        }

        drop(self.list.drain(..to_drop));
    }
    fn _insert(&mut self, ev: ReceivedEvent) {
        // .rev to search from latest, as that's probably where the new event is at.
        if self.list.iter().rev().any(|item| *item == ev) {
            return;
        }
        self.list.push(ev);
        // `TODO`: optimize sort to stop when the one new added has been found.
        // Sort from latest.
        self.list.sort();
        self.trim();
    }
    /// `timestamp` should be the one in [`Event::timestamp`]
    #[inline]
    pub(crate) fn insert(&mut self, event: Event, message_uuid: Uuid) {
        let event = event;
        let received = ReceivedEvent {
            event,
            uuid: message_uuid,
        };
        self._insert(received);
    }
    pub(crate) fn replace(&mut self, new_log: Vec<ReceivedEvent>) -> Vec<ReceivedEvent> {
        let old_log = std::mem::replace(&mut self.list, new_log);
        self.list.sort();
        self.trim();
        old_log
    }
    #[inline]
    pub(crate) fn len(&self) -> usize {
        self.list.len()
    }

    #[inline]
    pub(crate) fn cutoff_from_uuid(&self, cutoff_uuid: Uuid) -> Option<usize> {
        for (pos, ev) in self.list.iter().enumerate() {
            if ev.uuid == cutoff_uuid {
                return Some(pos);
            }
        }
        None
    }
    #[inline]
    pub(crate) fn cutoff_from_time(&self, cutoff: Duration) -> Option<usize> {
        for (pos, ev) in self.list.iter().enumerate().rev() {
            println!(
                "Comparing cutoff{cutoff:?} with event {:?}",
                ev.event.timestamp()
            );
            if ev.event.timestamp() <= cutoff {
                println!("Returnign start at {:?}", pos + 1);
                return Some(pos + 1);
            }
        }
        None
    }
    /// Returns an appropriate cutoff for [`Self::get_uuid_hash`] represented as
    /// it's position from the start (front) of the list and the target cutoff timestamp.
    ///
    /// Returns [`None`] if `self.list` has no elements.
    #[inline]
    pub(crate) fn appropriate_cutoff(&self) -> Option<(usize, Duration)> {
        let now = dur_now();
        let target = now - Duration::from_secs(2);
        self.cutoff_from_time(target).map(|cutoff| (cutoff, target))
    }

    /// Get the hashed log with `count` events and `cutoff` as the latest event to be included.
    ///
    /// # Errors
    ///
    /// `cutoff` must exist in the internal list and
    /// `count` events must exist in the remaining list.
    pub(crate) fn get_uuid_hash(
        &self,
        count: u32,
        cutoff: usize,
        cutoff_timestamp: Duration,
    ) -> Result<UuidCheck, UuidError> {
        let uuid = if let Some(ev) = self.list.get(cutoff) {
            ev.uuid
        } else {
            return Err(UuidError::CutoffMissing);
        };
        let end = cutoff;
        let start = if let Some(start) = end.checked_sub(count as usize) {
            start
        } else {
            return Err(UuidError::CountTooBig);
        };
        // UNWRAP: We've checked the conditions above.
        let iter = self.list.get(start..end).unwrap();
        let mut hasher = twox_hash::xxh3::Hash128::default();
        for ev in iter {
            hasher.write(&ev.uuid.inner().to_le_bytes());
        }
        let hash = hasher.finish_ext();

        let check = UuidCheck {
            log_hash: hash.to_le_bytes(),
            count,
            cutoff: uuid,
            cutoff_timestamp,
        };
        Ok(check)
    }

    /// Caps `max` to the length of the inner list.
    pub(crate) fn get_max(&self, max: Option<usize>) -> &[ReceivedEvent] {
        &self.list[..max.map_or(self.len(), |max| cmp::min(max, self.len()))]
    }

    /// `timestamp` is duration since `UNIX_EPOCH` when the `old_name` was relevant.
    pub(crate) fn modern_resource_name<'a>(
        &self,
        old_name: &'a str,
        timestamp: Duration,
    ) -> Option<&'a str> {
        let slice_start = self.cutoff_from_time(timestamp).unwrap_or(0);

        let slice = &self.list[slice_start..];

        event::Unwinder::new(slice)
            .check_name(old_name)
            .ok()
            .map(|()| old_name)
    }

    /// Rewinds to `timestamp` or as far as we can.
    #[inline]
    pub(crate) fn unwind_to(&self, timestamp: Duration) -> event::Unwinder {
        let mut cutoff = 0;
        for (pos, received_ev) in self.list.iter().enumerate().rev() {
            if received_ev.event.timestamp() <= timestamp {
                cutoff = pos + 1;
            }
        }

        event::Unwinder::new(&self.list[cutoff..])
    }
    pub(crate) fn event_applier<'a>(
        &'a mut self,
        event: &'a Event,
        message_uuid: Uuid,
    ) -> EventApplier<'a> {
        fn slice_start(log: &Log, resource: &str, uuid: Uuid) -> usize {
            // `pos` is from the end of the list.
            for (pos, event) in log.list.iter().enumerate().rev() {
                if event.uuid == uuid && event.event.resource() == resource {
                    // Match!
                    // Also takes the current event in the slice.
                    // ↑ is however not true for the backup return below.
                    return pos + 1;
                }
            }
            // Event is not in list.
            // This is a slow push.
            0
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
                                    let successor = ev.take_successor().unwrap();
                                    ev.set_resource(successor);
                                } else {
                                    ev.set_resource(successor.into());
                                }
                            }
                            EventKind::Modify(ev) => {
                                ev.set_resource(successor.into());
                            }
                            EventKind::Create(_) => break,
                        }
                    }
                }
            }
        }

        let slice = &self.list[slice_index..];
        // This check is required for the code at [`EventApplier::apply`] to not panic.
        let resource = event::Unwinder::new(slice)
            .check_name(event.resource())
            .ok()
            .map(|_| event.resource());
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
pub struct EventApplier<'a> {
    /// Ordered from last (temporally).
    /// May possibly not contain `Self::event`.
    events: &'a [ReceivedEvent],
    modern_resource_name: Option<&'a str>,
    /// Needed because the event might be sorted out of the list; slow push.
    event: &'a Event,
}
impl<'a> EventApplier<'a> {
    /// The name of the modified resource on the local store.
    /// If this is [`None`], the resource this event applies to has been deleted since.
    /// Don't call [`Self::apply`] if that's the case.
    #[must_use]
    #[inline]
    pub fn resource(&self) -> Option<&str> {
        self.modern_resource_name
    }
    /// Get a reference to the event to be applied.
    #[inline]
    pub fn event(&self) -> &Event {
        self.event
    }
    /// Must only be called if [`Self::event`] is [`EventKind::Modify`] &
    /// [`Self::resource`] returns [`Some`],
    /// as you know which resource to load.
    /// If the target resource doesn't exist, don't call this.
    ///
    /// # Errors
    ///
    /// tl;dr, this can be unwrapped if you have made sure [`Self::event`] is a [`EventKind::Modify`].
    ///
    /// Returns [`ApplyError::InvalidEvent`] if this [`EventApplier`] wasn't instantiated
    /// with a [`EventKind::Modify`].
    ///
    /// Returns a [`ApplyError::RefOutOfBounds`] if the difference of this event is out of
    /// bounds.
    pub fn apply(
        &self,
        resource: &[u8],
    ) -> Result<Vec<u8>, ApplyError> {
        let ev = if let EventKind::Modify(ev) = self.event.inner() {
            ev
        } else {
            return Err(ApplyError::InvalidEvent);
        };
        // Match only for the current resource.
        let current_resource_name = if let Some(name) = self.modern_resource_name {
            name
        } else {
            // `TODO`: remove the `.to_vec`
            return Ok(resource.to_vec());
        };
        let mut unwinder = event::Unwinder::new(self.events);

        let mut resource = match unwinder.unwind(resource, current_resource_name) {
            Ok(r) => r,
            Err(event::UnwindError::Apply(err)) => return Err(err.into()),
            Err(event::UnwindError::ResourceDestroyed) => {
                unreachable!("This is guaranteed by the check in [`EventLog::event_applier`].");
            }
        };
        println!("Unwound diff {:?} to resource: {:?}", ev.diff(), std::str::from_utf8(&resource));
        // When back there, implement the event.
        if ev.diff().apply_overlaps() {
            let mut other = Vec::with_capacity(resource.len() + 32);
            ev.diff().apply(&resource, &mut other)?;
            resource = other;
        } else {
            ev.diff().apply_in_place(&mut resource)?;
        }
        println!("Applied");

        resource = unwinder.rewind(&resource)?;
        println!("Rewound");

        // How will the revert and implement functions on modify work?
        // Can revert be a inverse and just call implement?
        // We need to modify the whole buffer when reverting
        // Directly modify `resource` as the buffer.
        // Have a `PartiallyUnknown` data type for the Sections which have removed data.
        Ok(resource)
    }
}

#[derive(Debug)]
pub(crate) enum UuidError {
    CountTooBig,
    /// Cutoff is missing in list.
    CutoffMissing,
}

/// A check of the event UUID log.
///
/// A discrepancy indicates differing thought of what the data should be.
/// Often, a [`crate::MessageKind::HashCheck`] is sent if that's the case.
#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Clone)]
#[must_use]
pub struct UuidCheck {
    log_hash: [u8; 16],
    count: u32,
    cutoff: Uuid,
    cutoff_timestamp: Duration,
}
impl UuidCheck {
    /// Get the count of events to be included, temporally before the [`Self::cutoff`].
    #[must_use]
    #[inline]
    pub fn count(&self) -> u32 {
        self.count
    }
    /// Get the UUID of the latest event to be included.
    #[inline]
    pub fn cutoff(&self) -> Uuid {
        self.cutoff
    }
    /// Get the timestamp before or at which all events are included.
    #[must_use]
    #[inline]
    pub fn cutoff_timestamp(&self) -> Duration {
        self.cutoff_timestamp
    }
    /// Get a reference to the 16-byte long hash.
    #[must_use]
    #[inline]
    pub fn hash(&self) -> &[u8; 16] {
        &self.log_hash
    }
}
/// The action to execute after receiving a [`crate::MessageKind::EventUuidLogCheck`].
#[derive(Debug)]
#[must_use]
pub enum UuidCheckAction {
    /// The logs match. Send a [`crate::MessageKind::EventUuidLogCheckReply`] with this
    /// [`UuidCheck`].
    Send(UuidCheck),
    /// The logs don't match. Do the same as with [`Self::Send`] AND wait a few seconds (e.g. 10)
    /// and then do a full check of the files. Something's not adding up.
    SendAndFurtherCheck(UuidCheck),
    /// Our log was too small. We can therefore not participate in this exchange.
    /// Do nothing.
    Nothing,
}
type Conversation = HashMap<Uuid, UuidCheck>;
#[derive(Debug)]
pub(crate) struct UuidReplies {
    conversations: HashMap<Uuid, Conversation>,
}
impl UuidReplies {
    pub(crate) fn new() -> Self {
        Self {
            conversations: HashMap::new(),
        }
    }
    /// `conversation` is the UUID of the current conversation. `check` is the data `source` sent.
    #[inline]
    pub(crate) fn insert(&mut self, conversation: Uuid, check: UuidCheck, source: Uuid) {
        let conversation = self
            .conversations
            .entry(conversation)
            .or_insert_with(HashMap::new);
        conversation.insert(source, check);
    }
    pub(crate) fn get(&self, conversation: Uuid) -> Option<&HashMap<Uuid, UuidCheck>> {
        self.conversations.get(&conversation)
    }
}
impl Default for UuidReplies {
    fn default() -> Self {
        Self::new()
    }
}
