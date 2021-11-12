//! A general decentralized sync library supporting text and binary.

#![deny(
    // clippy::pedantic,
    unreachable_pub,
    missing_debug_implementations,
    // missing_docs
)]
// `TODO`: remove
#![allow(clippy::missing_panics_doc)]

mod log;

use std::collections::HashMap;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

/// Describes the capabilities of the client. Sent in the initial [`Message`] exchange.
#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
#[must_use]
pub struct Capabilities {
    version: semver::Version,
    /// The client is striving to be persistent. These will regularly do [`Message::HashCheck`]
    persistent: bool,
    uuid: Uuid,
}

#[derive(Debug)]
#[must_use]
pub struct SliceBuf<'a> {
    slice: &'a mut [u8],
    len: usize,
}
impl<'a> SliceBuf<'a> {
    pub fn new(slice: &'a mut [u8]) -> Self {
        Self { slice, len: 0 }
    }
    /// Sets the size of the filled region of the buffer.
    ///
    /// # Panics
    ///
    /// Panics if `n > self.capacity()`.
    pub fn set_filled(&mut self, n: usize) {
        self.len = n;
        assert!(self.len <= self.slice.len());
    }
    /// Advances the size of the filled region of the buffer.
    ///
    /// # Panics
    ///
    /// Panics if `self.len() + n > self.capacity()`.
    pub fn advance(&mut self, n: usize) {
        self.set_filled(self.len + n);
    }
    /// The filled region of the buffer.
    #[must_use]
    pub fn filled(&self) -> usize {
        self.len
    }
    #[must_use]
    pub fn capacity(&self) -> usize {
        self.slice.len()
    }
}

#[derive(Debug)]
pub enum ApplyError {
    /// [`SliceBuf::capacity`] is too small.
    BufTooSmall,
}

/// [`Section::end`] must always be after [`Section::start`].
pub trait Section {
    fn start(&self) -> usize;
    fn end(&self) -> usize;
    fn resource_replace_len(&self) -> usize {
        self.end() - self.start()
    }
    /// Difference between old and new length of resource.
    ///
    /// # Panics
    ///
    /// Panics if the end is before the start.
    /// This guarantee should be upheld by the implementer of [`Section`].
    fn len_difference(&self) -> isize {
        self.len() as isize - (self.resource_replace_len() as isize)
    }
    /// Length of new data to fill between [`Section::start`] and [`Section::end`].
    fn len(&self) -> usize;
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}
pub trait DataSection: Section {
    fn data(&self) -> &[u8];
    /// Applies `self` to `resource`.
    ///
    /// Returns [`SliceBuf::filled`] if successful.
    ///
    /// # Errors
    ///
    /// Returns [`ApplyError::BufTooSmall`] if [`DataSection::data`] cannot fit in `resource`.
    fn apply(&self, resource: &mut SliceBuf) -> Result<usize, ApplyError> {
        let new_size = resource.filled() as isize + self.len_difference();
        if new_size > resource.capacity() as isize {
            return Err(ApplyError::BufTooSmall);
        }
        // Copy the old data that's in the way of the new.
        // SAFETY: We guarantee above that we can offset the bytes by `self.len_difference()`.
        unsafe {
            std::ptr::copy(
                &resource.slice[self.start()],
                &mut resource.slice[(self.start() as isize + self.len_difference()) as usize],
                resource.filled() - self.start(),
            )
        }
        // Copy data from `Section` to `resource`.
        // SAFETY: The write is guaranteed to have the space left, see `unsafe` block above.
        // They will never overlap, as the [`SliceBuf`] contains a mutable reference to the bytes,
        // they are exclusive.
        unsafe {
            std::ptr::copy_nonoverlapping(
                &self.data()[0],
                &mut resource.slice[self.start()],
                self.len(),
            )
        }
        resource.set_filled((resource.filled() as isize - self.len_difference()) as usize);
        Ok(resource.filled())
    }
}
impl<S: Section> Section for &S {
    fn start(&self) -> usize {
        (*self).start()
    }
    fn end(&self) -> usize {
        (*self).end()
    }
    fn len(&self) -> usize {
        (*self).len()
    }
}

#[derive(Debug)]
pub(crate) struct EmptySection {
    start: usize,
    end: usize,
    len: usize,
}
impl EmptySection {
    pub(crate) fn new<S: Section>(section: S) -> Self {
        Self {
            start: section.start(),
            end: section.end(),
            len: section.len(),
        }
    }
    pub(crate) fn revert(&self, resource: &mut SliceBuf) -> VecSection {
        assert!(resource.filled() + self.len() <= resource.capacity());
        assert!(self.end() <= resource.filled());
        let mut section = VecSection::new(self.start(), self.end(), Vec::with_capacity(self.len()));
        // Copy data from `resource` to `section`.
        unsafe {
            std::ptr::copy_nonoverlapping(
                &resource.slice[self.start()],
                &mut section.data[0],
                self.len(),
            );
            section.data.set_len(section.data().len())
        }
        // Copy the "old" data back in place of the new.
        unsafe {
            std::ptr::copy(
                &resource.slice[(self.start() as isize + self.len_difference()) as usize],
                &mut resource.slice[self.start()],
                resource.filled() - self.start(),
            )
        }

        // If we added bytes here,
        if self.len_difference() < 0 {
            let to_fill = (0 - self.len_difference()) as usize;
            unsafe {
                std::ptr::write_bytes(
                    &mut resource.slice[(self.start() as isize + self.len_difference()) as usize],
                    0,
                    to_fill,
                );
            }
        }

        resource.set_filled((resource.filled() as isize + self.len_difference()) as usize);
        section
    }
}
impl Section for EmptySection {
    fn start(&self) -> usize {
        self.start
    }
    fn end(&self) -> usize {
        self.end
    }
    fn len(&self) -> usize {
        self.len
    }
}

/// A selection of data in a document.
///
/// Comparable to `slice` functions in various languages (e.g. [JS](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/Array/splice)).
#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
#[must_use]
pub struct VecSection {
    /// The start of the previous data in the resource.
    start: usize,
    /// The end of the previous data in the resource.
    end: usize,
    /// A reference to the data.
    data: Vec<u8>,
}
impl VecSection {
    /// Assembles a new selection using `start` as the start of the original data, `end` as the
    /// terminator, and `data` to fill the space between `start` and `end`.
    ///
    /// If `data` is longer than `end-start`, the buffer is grown. If it's shorter, the buffer will
    /// be truncated.
    ///
    /// # Panics
    ///
    /// `start` must be less than or equal to `end`.
    pub fn new(start: usize, end: usize, data: Vec<u8>) -> Self {
        assert!(
            start <= end,
            "passed the start of data ({:?}) after the end of it({:?})",
            start,
            end
        );
        Self { start, end, data }
    }
}
impl Section for VecSection {
    fn start(&self) -> usize {
        self.start
    }
    fn end(&self) -> usize {
        self.end
    }
    fn len(&self) -> usize {
        self.data.len()
    }
}
impl DataSection for VecSection {
    fn data(&self) -> &[u8] {
        &self.data
    }
}

/// A modification to a resource.
#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
#[must_use]
pub struct ModifyEvent<S> {
    resource: String,
    section: S,
}
impl<S: DataSection> ModifyEvent<S> {
    /// Calculates the diff if `source` is [`Some`].
    pub fn new(resource: String, section: S, source: Option<&[u8]>) -> Self {
        if source.is_some() {
            unimplemented!("Implement diff");
        }
        Self { resource, section }
    }
}
/// A deletion of a resource.
#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
#[must_use]
pub struct DeleteEvent {
    resource: String,
}
/// A move of a resource. Changes the resource path.
#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
#[must_use]
pub struct MoveEvent {
    resource: String,
    target: String,
}

macro_rules! into_event {
    ($type:ty, $enum_name:ident) => {
        impl<'a, T> From<$type> for Event<T> {
            fn from(event: $type) -> Self {
                Self::$enum_name(event)
            }
        }
    };
}

into_event!(ModifyEvent<T>, Modify);
into_event!(DeleteEvent, Delete);
into_event!(MoveEvent, Move);
impl<T: Into<Event<VecSection>>> From<T> for EventMessage {
    fn from(ev: T) -> Self {
        EventMessage::new(vec![ev.into()])
    }
}

/// A change of data.
#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
#[must_use]
pub enum Event<S> {
    /// Modification.
    Modify(ModifyEvent<S>),
    /// Deletion.
    Delete(DeleteEvent),
    /// Move.
    Move(MoveEvent),
}
impl<S> Event<S> {
    pub fn resource(&self) -> &str {
        match self {
            Self::Modify(ev) => &ev.resource,
            Self::Delete(ev) => &ev.resource,
            Self::Move(ev) => &ev.resource,
        }
    }
}
/// Clones the `resource` [`String`].
impl<S: Section> From<&Event<S>> for Event<EmptySection> {
    fn from(ev: &Event<S>) -> Self {
        match ev {
            Event::Modify(ev) => Event::Modify(ModifyEvent {
                resource: ev.resource.clone(),
                section: EmptySection::new(&ev.section),
            }),
            Event::Move(ev) => Event::Move(ev.clone()),
            Event::Delete(ev) => Event::Delete(ev.clone()),
        }
    }
}
pub type DatafulEvent = Event<VecSection>;

/// The data of [`MessageKind`] corresponding to a list of [`Event`]s.
#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
#[must_use]
pub struct EventMessage {
    events: Vec<DatafulEvent>,
    /// Duration since UNIX_EPOCH
    timestamp: Duration,
}
impl EventMessage {
    /// Creates a new event message with the timestamp [`SystemTime::now`] and `events`.
    pub fn new(events: Vec<DatafulEvent>) -> Self {
        Self::with_timestamp(events, SystemTime::now())
    }
    /// Assembles from `timestamp` and `events`.
    pub fn with_timestamp(events: Vec<DatafulEvent>, timestamp: SystemTime) -> Self {
        Self {
            events,
            timestamp: timestamp
                .duration_since(UNIX_EPOCH)
                .unwrap_or(Duration::ZERO),
        }
    }
    /// Gets an iterator of the internal events.
    pub fn event_iter(&self) -> impl Iterator<Item = &DatafulEvent> {
        self.events.iter()
    }
}

/// A UUID.
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Clone, Copy, Serialize, Deserialize)]
#[must_use]
pub struct Uuid(u64);
impl Uuid {
    /// Creates a new UUID.
    ///
    /// Uses [`rand::random()`]. Use [`Self::with_rng`] for a faster method.
    pub fn new() -> Self {
        Self(rand::random())
    }
    /// Uses `rng` to create a UUID.
    pub fn with_rng(mut rng: impl rand::Rng) -> Self {
        Self(rng.gen())
    }
    /// Gets the inner data.
    #[must_use]
    pub fn inner(&self) -> u64 {
        self.0
    }
}
impl Default for Uuid {
    fn default() -> Self {
        Self::new()
    }
}

/// The kinds of messages with their data. Part of a [`Message`].
#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
#[must_use]
pub enum MessageKind {
    /// The client sending this is connecting to the network.
    ///
    /// Will declare it's capabilities.
    ///
    /// Expects a [`Self::Welcome`] or [`Self::InvalidUuid`].
    Hello(Capabilities),
    /// Response to the [`Self::Hello`] message.
    Welcome(Capabilities),
    /// The [`MessageKind::Hello`] uses an occupied UUID.
    InvalidUuid,
    /// The [`Capabilities::version()`] is not compatible.
    ///
    /// The sending client will not add UUID of the [`Self::Hello`] message to the known clients.
    /// The sender of the Hello should ignore all future messages from this client.
    MismatchingVersions,
    /// A client has new data to share.
    Event(EventMessage),
    /// A client tries to get the most recent data.
    /// Contains the list of which documents were edited and size at last session.
    /// `TODO`: Don't send the one's that's been modified locally; we only want their changes.
    ///
    /// You should respond with a [`Self::FastForwardReply`].
    FastForward,
    /// A reply to a [`Self::FastForward`] request.
    FastForwardReply,
    /// A request to get the diffs and sync the specified resources.
    Sync,
    /// The response with hashes of the specified resources.
    SyncReply,
    /// Requests all the hashes of all the resources specified in the regex.
    HashCheck,
    /// A reply with all the hashes of all the requested files.
    HashCheckReply,
    /// Checks the internal message UUID log.
    ///
    /// `TODO`: specify last x messages. Sort them before sending; we have a sorted events
    /// guaranteed, so now we ignore order of messages.
    MessageUuidLogCheck,
    /// The target client cancelled the request.
    ///
    /// This may be the result of too many requests.
    /// Should not be sent as a signal of not supporting the feature.
    Canceled,
}
/// A message to be communicated between clients.
///
/// Contains a [`MessageKind`], sender UUID, and message UUID.
#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
#[must_use]
pub struct Message {
    kind: MessageKind,
    sender: Uuid,
    uuid: Uuid,
}
impl Message {
    /// Creates a new message.
    #[inline]
    pub fn new(inner: MessageKind, sender: Uuid, uuid: Uuid) -> Self {
        Self {
            kind: inner,
            sender,
            uuid,
        }
    }
    /// Gets the sender UUID.
    #[inline]
    pub fn sender(&self) -> Uuid {
        self.sender
    }
    /// Gets the message UUID.
    #[inline]
    pub fn uuid(&self) -> Uuid {
        self.uuid
    }
    /// Gets the inner [`MessageKind`].
    ///
    /// This contains all the data of the message.
    #[inline]
    pub fn inner(&self) -> &MessageKind {
        &self.kind
    }

    /// Converts the message to bytes.
    ///
    /// You can also use [`bincode`] or any other [`serde`]-based library to serialize the message.
    #[must_use]
    pub fn bin(self) -> Vec<u8> {
        // this should be good; we only use objects from ::std and our own derived
        bincode::encode_to_vec(
            bincode::serde::Compat(self),
            bincode::config::Configuration::standard(),
        )
        .unwrap()
    }
}
impl TryFrom<&[u8]> for Message {
    type Error = bincode::error::DecodeError;
    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        bincode::serde::decode_from_slice(value, bincode::config::Configuration::standard())
    }
}

/// The main manager of a client.
///
/// The `process_*` methods are for creating [`Message`]s.
/// The `apply_*` methods are for accepting and evaluating [`Message`]s.
#[derive(Debug)]
#[must_use]
pub struct Manager {
    rng: rand::rngs::ThreadRng,
    uuid: Uuid,
    clients: HashMap<Uuid, Capabilities>,

    event_log: log::EventLog,
    message_uuid_log: log::MessageUuidLog,
}
impl Manager {
    /// Creates a empty manager.
    ///
    /// Call [`Self::process_hello()`] to get a hello message.
    pub fn new(log_lifetime: Duration, message_log_limit: u32) -> Self {
        let mut rng = rand::thread_rng();
        let uuid = Uuid::with_rng(&mut rng);
        Self {
            rng,
            uuid,
            clients: HashMap::new(),

            event_log: log::EventLog::new(log_lifetime),
            message_uuid_log: log::MessageUuidLog::new(message_log_limit),
        }
    }
    /// Gets the UUID of this client.
    pub fn uuid(&self) -> Uuid {
        self.uuid
    }
    /// Generates a UUID using the internal [`rand::Rng`].
    #[inline]
    pub(crate) fn generate_uuid(&mut self) -> Uuid {
        Uuid::with_rng(&mut self.rng)
    }
    /// Takes a [`EventMessage`] and returns a [`Message`].
    #[inline]
    pub fn process_event(&mut self, event: EventMessage) -> Message {
        Message::new(MessageKind::Event(event), self.uuid, self.generate_uuid())
    }
    /// # Errors
    ///
    /// Fails if the `event` is more than 10s from the future.
    ///
    /// This prevents other clients from hogging our memory with items which never expire.
    /// If their clock is more than 10s off relative to our, we have a serious problem!
    pub fn apply_event<'a>(
        &'a mut self,
        event: &'a DatafulEvent,
        uuid: Uuid,
        timestamp: Duration,
    ) -> Result<log::EventApplier<'a>, log::Error> {
        self.event_log.insert(event, uuid, timestamp)?;
        Ok(self.event_log.event_applier(event.resource(), uuid))
    }
}
impl Default for Manager {
    fn default() -> Self {
        Self::new(Duration::new(64, 0), 200)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn send_diff() {
        let (message, sender_uuid): (Vec<u8>, Uuid) = {
            let mut manager = Manager::default();

            let event = ModifyEvent::new(
                "test.txt".into(),
                VecSection::new(0, 0, b"Some test data.".to_vec()),
                None,
            );

            let message = manager.process_event(event.into());

            (message.bin(), manager.uuid())
        };

        // The message are sent to a different client.

        let message: Message = message.as_slice().try_into().unwrap();

        assert_eq!(message.sender(), sender_uuid);

        let mut receiver = Manager::default();

        match message.inner() {
            MessageKind::Event(ev_msg) => {
                let mut events = ev_msg.event_iter();

                assert_eq!(
                    events.next(),
                    Some(&Event::Modify(ModifyEvent::new(
                        "test.txt".into(),
                        VecSection::new(0, 0, b"Some test data.".to_vec()),
                        None
                    )))
                );
                assert_eq!(events.next(), None);

                for event in ev_msg.event_iter() {
                    let event_applier = receiver
                        .apply_event(event, message.uuid(), ev_msg.timestamp)
                        .expect("Got event from future.");
                    assert_eq!(event_applier.resource(), Some("test.txt"));
                }
            }
            kind => {
                panic!("Got {:?}, but expected a Event!", kind);
            }
        }

        // receive action...
        // assert the only action is to modify `test.txt` with the same section as before.
    }

    // Test doing this ↑ but simplifying as it had previous data, stored how?

    // Test when the underlying data has changed without events; then this library is called again.
    // A special call to the library, which will request all the files, mtime & size to see which
    // have changed.
}
