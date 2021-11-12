//! A general decentralized sync library supporting text and binary.

#![deny(
    clippy::pedantic,
    unreachable_pub,
    missing_debug_implementations,
    missing_docs
)]
// `TODO`: remove
#![allow(clippy::missing_panics_doc)]

mod log;

use std::collections::HashMap;
use std::time::{Duration, SystemTime};

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

/// A modification to a resource.
#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
#[must_use]
pub struct ModifyEvent {
    resource: String,
    section: Section,
}
impl ModifyEvent {
    /// Calculates the diff if `source` is [`Some`].
    pub fn new(resource: String, section: Section, source: Option<&[u8]>) -> Self {
        if source.is_some() {
            unimplemented!("Implement diff");
        }
        Self { resource, section }
    }
}
/// A deletion of a resource.
#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
#[must_use]
pub struct DeleteEvent;
/// A move of a resource. Changes the resource path.
#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
#[must_use]
pub struct MoveEvent;

macro_rules! into_event {
    ($type:ty, $enum_name:ident) => {
        impl<'a> From<$type> for Event {
            fn from(event: $type) -> Self {
                Self::$enum_name(event)
            }
        }
    };
}

into_event!(ModifyEvent, Modify);
impl<T: Into<Event>> From<T> for EventMessage {
    fn from(ev: T) -> Self {
        EventMessage::new(vec![ev.into()])
    }
}

/// A change of data.
#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
#[must_use]
pub enum Event {
    /// Modification.
    Modify(ModifyEvent),
    /// Deletion.
    Delete(DeleteEvent),
    /// Move.
    Move(MoveEvent),
}

/// A selection of data in a document.
///
/// Comparable to `slice` functions in various languages (e.g. [JS](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/Array/splice)).
#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
#[must_use]
pub struct Section {
    /// The start of the previous data in the resource.
    start: usize,
    /// The end of the previous data in the resource.
    end: usize,
    /// A reference to the data.
    data: Vec<u8>,
}
impl Section {
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
        assert!(start > end, "passed the start of data after the end of it");
        Self { start, end, data }
    }
}

/// The data of [`MessageKind`] corresponding to a list of [`Event`]s.
#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
#[must_use]
pub struct EventMessage {
    events: Vec<Event>,
    /// Duration since UNIX_EPOCH
    timestamp: Duration,
}
impl EventMessage {
    /// Creates a new event message with the timestamp [`SystemTime::now`] and `events`.
    pub fn new(events: Vec<Event>) -> Self {
        Self::with_timestamp(events, SystemTime::now())
    }
    /// Assembles from `timestamp` and `events`.
    pub fn with_timestamp(events: Vec<Event>, timestamp: SystemTime) -> Self {
        Self { events, timestamp }
    }
    /// Gets an iterator of the internal events.
    pub fn event_iter(&self) -> impl Iterator<Item = &Event> {
        self.events.iter()
    }
}

/// A UUID.
#[derive(Debug, PartialEq, Eq, Clone, Copy, Serialize, Deserialize)]
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
    pub fn bin(self) -> Vec<u8> {
        // this should be good; we only use objects from ::std and our own derived
        bincode::encode_to_vec(
            bincode::serde::Compat(self),
            bincode::config::Configuration::standard(),
        )
        .unwrap()
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
            event_log: log::EventLog::new(log_lifetime),
            message_uuid_log: log::MessageUuidLog::new(message_log_limit),
            ..Default::default()
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
    pub fn apply_event(&mut self, event: &Event, timestamp: Duration) -> Result<log::EventSorter, log::Error> {
        self.event_log.insert(event, timestamp)?;
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
        let mut manager = Manager::default();

        let event = ModifyEvent::new(
            "test.txt".into(),
            Section::new(0, 0, b"Some test data.".to_vec()),
            None,
        );

        let sender_uuid = manager.uuid();

        let mut message = manager.process_event(event.clone().into());

        // The message are sent to a different client.

        assert_eq!(message.sender(), sender_uuid);

        let mut receiver = Manager::default();

        match message.inner() {
            MessageKind::Event(ev) => {
                let timestamp = ev.timestamp;
                let mut events = ev
                    .event_iter()
                    .map(|action| receiver.apply_event(action, timestamp))
                    .flatten();

                assert_eq!(events.next(), Some(event));
                assert_eq!(events.next(), None);
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
