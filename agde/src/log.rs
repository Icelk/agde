//! Logs for [`crate::Manager`].

use std::cmp::{self, Ordering};
use std::collections::HashMap;
use std::fmt::Debug;
use std::time::Duration;

use dach::ExtendVec;
use serde::{Deserialize, Serialize};

use crate::{event, Manager};
use crate::{utils, utils::dur_now, Event, EventKind, Uuid};

/// Implements [`ExtendVec`] to fill with zeroes. Reduces memory usage when storing log.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ZeroFiller {
    len: usize,
}
impl ExtendVec for ZeroFiller {
    #[inline]
    fn extend(&self, vec: &mut Vec<u8>) {
        vec.reserve(self.len);
        let old_len = Vec::len(vec);
        unsafe { vec.set_len(old_len + self.len) };
        let slice = &mut vec[old_len..old_len + self.len];
        slice.fill(0);
    }
    #[inline]
    #[allow(clippy::uninit_vec)] // we know what we're doing
    fn replace(&self, vec: &mut Vec<u8>, position: usize) {
        let old_len = Vec::len(vec);
        let reserve = (position + self.len).saturating_sub(old_len);
        vec.reserve(reserve);
        unsafe { vec.set_len(old_len + reserve) };
        let slice = &mut vec[position..position + self.len];
        slice.fill(0);
    }
    fn equals(&self, bytes: &[u8]) -> bool {
        bytes.iter().copied().all(|b| b == b'\0')
    }
    #[inline]
    fn len(&self) -> usize {
        self.len
    }
}

/// A received event.
///
/// Contains the necessary metadata to sort the event.
/// Does not contain any raw data, as that's calculated by undoing all the events.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[must_use]
pub(crate) struct ReceivedEvent {
    pub(crate) event: Event,
    /// Message UUID.
    pub(crate) message_uuid: Uuid,
}
impl PartialEq for ReceivedEvent {
    fn eq(&self, other: &Self) -> bool {
        self.event.timestamp() == other.event.timestamp() && self.message_uuid == other.message_uuid
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
            Equal => self.message_uuid.cmp(&other.message_uuid),
            ord => ord,
        }
    }
}

/// An error with editing of any of the logs.
#[derive(Debug)]
pub enum Error {
    /// The given [`Event`] has occurred in the future!
    EventInFuture,
    /// You are currently fast forwarding.
    /// Please just ignore this and continue on your marry way.
    FastForwardInProgress,
}

/// An error during [`dach::Difference::apply`] and [`crate::log::EventApplier::apply`].
#[derive(Debug)]
pub enum ApplyError {
    /// A reference in the difference is out of bounds.
    /// See [`dach::ApplyError::RefOutOfBounds`].
    RefOutOfBounds,
    /// The function called must not be called on the current event.
    InvalidEvent,
}
impl From<dach::ApplyError> for ApplyError {
    fn from(err: dach::ApplyError) -> Self {
        match err {
            dach::ApplyError::RefOutOfBounds => Self::RefOutOfBounds,
        }
    }
}

#[derive(Debug)]
#[must_use]
pub(crate) struct Log {
    pub(crate) list: Vec<ReceivedEvent>,
    pub(crate) lifetime: Duration,
    pub(crate) limit: u32,

    /// A timestamp after which all events MUST be stored.
    pub(crate) required_event_timestamp: Option<Duration>,
}
impl Log {
    pub(crate) fn new(lifetime: Duration, limit: u32) -> Self {
        Self {
            list: Vec::new(),
            lifetime,
            limit,

            required_event_timestamp: None,
        }
    }
    /// Get the event log's lifetime.
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
            if self.list.len().saturating_sub(to_drop) > self.limit as usize {
                to_drop += 1;
                continue;
            }
            let last = self.list.get(to_drop);
            if let Some(last) = last {
                if last.event.timestamp_dur() < limit
                    && self
                        .required_event_timestamp
                        .map_or(true, |required| last.event.timestamp_dur() < required)
                {
                    to_drop += 1;
                    continue;
                }
            }
            break;
        }

        drop(self.list.drain(..to_drop));
    }
    fn _insert(&mut self, ev: ReceivedEvent) {
        match self.list.binary_search(&ev) {
            Err(idx) => self.list.insert(idx, ev),
            Ok(idx) => {
                let elem = &self.list[idx];
                match elem.event.sender().cmp(&ev.event.sender()) {
                    Ordering::Less => self.list.insert(idx, ev),
                    Ordering::Greater => self.list.insert(idx + 1, ev),
                    Ordering::Equal => {}
                }
            }
        }
    }
    /// `timestamp` should be the one in [`Event::timestamp`]
    #[inline]
    pub(crate) fn insert(&mut self, ev: Event, message_uuid: Uuid) {
        let received = ReceivedEvent {
            event: ev,
            message_uuid,
        };
        self._insert(received);
        self.trim();
    }
    pub(crate) fn merge(&mut self, mut new_log: Vec<ReceivedEvent>) {
        // we don't know it it's sorted, so we can't just take the last element
        let newest = new_log
            .iter()
            .max_by(|a, b| a.event.timestamp_dur().cmp(&b.event.timestamp_dur()));
        if let Some(newest) = newest {
            self.list.drain(
                ..self
                    .cutoff_from_time(newest.event.timestamp_dur() - Duration::from_secs(5))
                    .unwrap_or(0),
            );
        }
        self.list.append(&mut new_log);
        self.list.sort_unstable_by(|a, b| match a.cmp(b) {
            Ordering::Equal => a.event.sender().cmp(&b.event.sender()),
            o => o,
        });
        self.list
            .dedup_by(|a, b| a == b && a.event.sender() == b.event.sender());
        self.trim();
    }
    #[inline]
    pub(crate) fn len(&self) -> usize {
        self.list.len()
    }

    #[inline]
    pub(crate) fn cutoff_from_uuid(&self, cutoff_uuid: Uuid) -> Option<usize> {
        for (pos, ev) in self.list.iter().enumerate() {
            if ev.message_uuid == cutoff_uuid {
                return Some(pos);
            }
        }
        None
    }
    #[inline]
    pub(crate) fn cutoff_from_time(&self, cutoff: Duration) -> Option<usize> {
        for (pos, ev) in self.list.iter().enumerate().rev() {
            if ev.event.timestamp_dur() <= cutoff {
                return Some(pos + 1);
            }
        }
        None
    }
    /// Returns an appropriate cutoff for [`Self::get_uuid_hash`] represented as
    /// it's position from the start (temporally) of the list and the target cutoff timestamp.
    ///
    /// Returns [`None`] if `self.list` has no elements.
    #[inline]
    pub(crate) fn appropriate_cutoff(&self) -> Option<(usize, Duration)> {
        let now = dur_now();
        let target = now - Duration::from_secs(2);
        self.cutoff_from_time(target).map(|cutoff| (cutoff, target))
    }

    /// Get the latest event touching `resource`.
    ///
    /// This returns [`None`] if no events touch `resource`.
    pub(crate) fn latest_event(&self, resource: &str) -> Option<&ReceivedEvent> {
        self.list
            .iter()
            .rev()
            .find(|ev| ev.event.resource() == resource)
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
    ) -> Result<Check, UuidError> {
        let uuid = if let Some(ev) = self.list.get(cutoff) {
            ev.message_uuid
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
        let mut hasher = xxhash_rust::xxh3::Xxh3::default();
        for ev in iter {
            hasher.update(&ev.message_uuid.inner().to_le_bytes());
        }
        let hash = hasher.digest128();

        let check = Check {
            log_hash: hash.to_le_bytes(),
            count,
            cutoff: uuid,
            cutoff_timestamp,
        };
        Ok(check)
    }

    /// Caps `max` to the length of the inner list.
    pub(crate) fn get_max(&self, max: usize) -> &[ReceivedEvent] {
        &self.list[..cmp::min(max, self.len())]
    }

    /// `timestamp` is duration since `UNIX_EPOCH` when the `old_name` was relevant.
    pub(crate) fn modern_resource_name<'a>(
        &self,
        old_name: &'a str,
        timestamp: Duration,
    ) -> Option<&'a str> {
        let slice_start = self.cutoff_from_time(timestamp).unwrap_or(0);

        let slice = &self.list[slice_start..];

        event::Unwinder::new(slice, None)
            .check_name(old_name)
            .then(|| old_name)
    }

    /// Rewinds to `timestamp` or as far as we can.
    #[inline]
    pub(crate) fn unwind_to<'a>(
        &'a self,
        timestamp: Duration,
        manager: &'a Manager,
    ) -> event::Unwinder<'a> {
        let mut cutoff = 0;
        for (pos, received_ev) in self.list.iter().enumerate().rev() {
            if received_ev.event.timestamp_dur() <= timestamp {
                cutoff = pos + 1;
            }
        }

        event::Unwinder::new(&self.list[cutoff..], Some(manager))
    }
    pub(crate) fn event_applier<'a>(
        &'a mut self,
        event: &'a Event,
        message_uuid: Uuid,
    ) -> EventApplier<'a> {
        fn slice_start(log: &Log, resource: &str, message_uuid: Uuid) -> usize {
            // `pos` is from the end of the list.
            for (pos, event) in log.list.iter().enumerate().rev() {
                if event.message_uuid == message_uuid && event.event.resource() == resource {
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
                    if received.message_uuid == message_uuid {
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

        let slice = &mut self.list[slice_index..];
        // This check is required for the code at [`EventApplier::apply`] to not panic.
        let resource = event::Unwinder::new(slice, None)
            .check_name(event.resource())
            .then(|| event.resource());
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
    events: &'a mut [ReceivedEvent],
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
    /// If a [`ApplyError::RefOutOfBounds`] error occurs, the apply action is completed and the
    /// error and the applied data is returned.
    ///
    /// Returns [`ApplyError::InvalidEvent`] if this [`EventApplier`] wasn't instantiated
    /// with a [`EventKind::Modify`].
    ///
    /// Returns a [`ApplyError::RefOutOfBounds`] if the difference of this event is out of
    /// bounds.
    #[allow(clippy::too_many_lines, clippy::missing_panics_doc)]
    pub fn apply(
        &mut self,
        resource: impl Into<Vec<u8>>,
    ) -> Result<Vec<u8>, (ApplyError, Vec<u8>)> {
        let ev = if let EventKind::Modify(ev) = self.event.inner() {
            ev
        } else {
            return Err((ApplyError::InvalidEvent, resource.into()));
        };
        // Match only for the current resource.
        let current_resource_name = if let Some(name) = self.modern_resource_name {
            name
        } else {
            return Ok(resource.into());
        };

        let mut error = None;

        let mut unwinder = event::Unwinder::new(self.events, None);

        let mut resource = match unwinder.unwind(resource, current_resource_name) {
            Ok(r) => r,
            Err(event::UnwindError::Apply(err, vec)) => {
                error = Some(err);
                vec
            }
            Err(event::UnwindError::ResourceDestroyed(_)) => {
                unreachable!("This is guaranteed by the check in [`EventLog::event_applier`].");
            }
        };
        let len = resource.len();
        // When back there, implement the event.
        if ev.diff().in_bounds(&resource) {
            if ev.diff().apply_overlaps(resource.len()) {
                let mut other = Vec::with_capacity(resource.len() + 32);
                ev.diff()
                    .apply(&resource, &mut other)
                    .expect("we made sure the references were in bounds");
                resource = other;
            } else {
                ev.diff()
                    .apply_in_place(&mut resource)
                    .expect("we made sure the references were in bounds");
            }
        } else {
            error = Some(dach::ApplyError::RefOutOfBounds);
        }
        let len_diff = utils::sub_usize(resource.len(), len);
        let mut offsets = utils::Offsets::new();
        // UNWRAP: we know the ONE `ev.diff` has the same `block_size`
        offsets.add_diff(ev.diff(), len_diff);

        resource = match unwinder.rewind(resource) {
            Ok(v) => v,
            Err((err, v)) => {
                error = Some(err);
                v
            }
        };

        offsets.apply(
            self.events
                .iter_mut()
                .filter(|stored_ev| {
                    stored_ev.event.latest_event_timestamp() < self.event.timestamp()
                })
                .map(|ev| &mut ev.event),
        );

        if let Some(err) = error {
            Err((err.into(), resource))
        } else {
            Ok(resource)
        }
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
pub struct Check {
    log_hash: [u8; 16],
    count: u32,
    cutoff: Uuid,
    cutoff_timestamp: Duration,
}
impl Check {
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
/// The action to execute after receiving a [`crate::MessageKind::LogCheck`].
#[derive(Debug)]
#[must_use]
pub enum CheckAction {
    ///  Send a [`crate::MessageKind::LogCheckReply`] with this
    /// [`Check`].
    ///
    /// Run [`Manager::assure_log_check`] after a few seconds, to check our hash against other
    /// piers' hashes.
    Send(Check),
    /// Our log was too small. We can therefore not participate in this exchange.
    /// Do nothing.
    Nothing,
}
#[derive(Debug)]
struct Conversation {
    messages: HashMap<Uuid, Check>,
    last_modified: Duration,
}
impl Conversation {
    fn new() -> Self {
        Self {
            messages: HashMap::new(),
            last_modified: utils::dur_now(),
        }
    }
}
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
    pub(crate) fn insert(&mut self, conversation: Uuid, check: Check, source: Uuid) {
        let conversation = self
            .conversations
            .entry(conversation)
            .or_insert_with(Conversation::new);
        conversation.messages.insert(source, check);
        conversation.last_modified = utils::dur_now();
    }
    pub(crate) fn update(&mut self, conversation: Uuid) {
        let mut conversation = self.conversations.get_mut(&conversation);
        if let Some(c) = &mut conversation {
            c.last_modified = utils::dur_now();
        }
    }
    pub(crate) fn get(&self, conversation: Uuid) -> Option<&HashMap<Uuid, Check>> {
        let conversation = self.conversations.get(&conversation);
        conversation.map(|c| &c.messages)
    }
    pub(crate) fn remove(&mut self, conversation: Uuid) {
        self.conversations.remove(&conversation);
    }
    pub(crate) fn clean(&mut self) {
        let now = utils::dur_now();
        let threshold = now - Duration::from_secs(60 * 5);
        self.conversations
            .retain(|_, conversation| conversation.last_modified > threshold);
    }
    pub(crate) fn remove_pier(&mut self, pier: Uuid) {
        for conversation in self.conversations.values_mut() {
            conversation.messages.remove(&pier);
        }
    }
}
impl Default for UuidReplies {
    fn default() -> Self {
        Self::new()
    }
}
