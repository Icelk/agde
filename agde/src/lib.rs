//! A general decentralized sync library supporting text and binary.
//!
//! # Vocabulary
//!
//! Here are some terms I use throughout the codebase and documentation.
//!
//! - resource - a piece of data (e.g. a file).
//!     May also be used to denote the location of a resource.
//! - pier - another client on the network.
//! - help desire - how much a pier wants to help others in the network.
//! - conversation - a exchange of some related data (e.g. [`MessageKind::EventUuidLogCheck`]).
//! - [`Section`] a splice operation to a resource.
//! - UUID - a unique identifier for a unit (e.g. conversation, [`Message`])
//! - logs - internal lists to compensate for inconsistencies in message arrival time.

#![deny(
    clippy::pedantic,
    unreachable_pub,
    missing_debug_implementations,
    missing_docs
)]

pub mod diff;
pub mod log;
pub mod section;

use std::borrow::Cow;
use std::cmp;
use std::collections::HashMap;
use std::ops::DerefMut;
use std::sync::Mutex;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use rand::Rng;
use serde::{Deserialize, Serialize};

pub use log::{EventUuidLogCheck, EventUuidLogCheckAction};
pub use section::{DataSection, Section, SliceBuf, VecSection};

/// The current version of this `agde` library.
pub const VERSION: semver::Version = semver::Version::new(0, 1, 0);

/// Describes the capabilities and properties of the client. Sent in the initial [`Message`] exchange.
#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
#[must_use]
pub struct Capabilities {
    version: semver::Version,
    uuid: Uuid,
    persistent: bool,
    /// The desire to communicate with others in the system.
    ///
    /// This will play part in deciding which client to send check requests to.
    /// Normally, the always-on clients (those started alongside the server, if there is any),
    /// will have a high value.
    help_desire: i16,
}
impl Capabilities {
    /// Creates a new set of capabilities and properties used by this client.
    ///
    /// See the various getters for more information about the usage of these options.
    pub fn new(uuid: Uuid, persistent: bool, help_desire: i16) -> Self {
        Self {
            version: VERSION,
            uuid,
            persistent,
            help_desire,
        }
    }
    /// Version of this client.
    #[must_use]
    pub fn version(&self) -> &semver::Version {
        &self.version
    }
    /// The UUID of this client.
    pub fn uuid(&self) -> Uuid {
        self.uuid
    }
    /// The client is striving to be persistent. These will regularly do [`MessageKind::HashCheck`].
    #[must_use]
    pub fn persistent(&self) -> bool {
        self.persistent
    }
    /// The desire to communicate with others in the system.
    ///
    /// This value will go from [`i16::MIN`], which means "I want none o' this" to
    /// [`i16::MAX`] which means the client is happy to help others.
    /// See [`Self::effective_help_desire`] for how this range is used.
    /// This will usually be set in accordance to the free processing power a client has to offer.
    ///
    /// This will play part in deciding which client to send check requests to.
    /// Normally, the always-on clients (those started alongside the server, if there is any),
    /// will have a high value.
    #[must_use]
    pub fn help_desire(&self) -> i16 {
        self.help_desire
    }
    /// Returns a desire to help in a lower range.
    ///
    /// This can be used to calculate the relative desire.
    /// For now, the whole [`i16`] range is collapsed to [0..9)
    /// This uses a root function (`⁵√self.help_desire`).
    /// This leads to a `help_desire` of `1024` being just
    /// half as likely to be chosen as [`i16::MAX`].
    /// A `help_desire` of `0` returns `1` from this function.
    /// The value returned from this function will never be `0` or less.
    #[allow(clippy::missing_panics_doc)] // this is only a internal check.
    #[must_use]
    pub fn effective_help_desire(&self) -> f32 {
        let desire = f32::from(self.help_desire());
        let effective = desire.powf(0.2);
        // Here, we need effective to be in range [-8..8]
        assert!(
            effective > -8.0 && effective < 8.0,
            "Effective must be in range [-8..8]. Please report this internal bug."
        );
        if effective.is_sign_positive() || effective == 0.0 {
            // Map (0..8]
            // to
            // (1..9]
            effective + 1.0
        } else {
            // Map [-8..0]
            // to
            // [0..8]
            // to
            // [0..1]
            (effective + 8.0) / 8.0
        }
    }
}

/// A modification to a resource.
///
/// The resource must be initialised using [`CreateEvent`].
#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
#[must_use]
pub struct ModifyEvent<S> {
    resource: String,
    sections: Vec<S>,
}
impl ModifyEvent<VecSection> {
    /// Calculates the diff if `source` is [`Some`].
    ///
    /// The sections **MUST NOT** overlap. That results in undefined logic and potentially
    /// loss of data.
    pub fn new(resource: String, sections: Vec<VecSection>, base: Option<&[u8]>) -> Self {
        let mut sections = sections;
        if let Some(base) = base {
            let target = {
                if sections.len() == 1 {
                    sections.first().and_then(|section| {
                        if section.old_len() >= base.len() {
                            Some(section.data())
                        } else {
                            None
                        }
                    })
                } else {
                    None
                }
            };
            let target = if let Some(base) = target {
                Cow::Borrowed(base)
            } else {
                let mut target = base.to_vec();
                let mut filled = target.len();
                for section in sections {
                    section.apply_len_single(&mut target, filled, 0);
                    let mut buf = SliceBuf::new(&mut target);
                    section
                        .apply(&mut buf)
                        .expect("Buffer is guaranteed to not be too small.");
                    filled = buf.filled().len();
                }
                target.truncate(filled);
                Cow::Owned(target)
            };
            let diff = diff::diff(base, &target);
            sections = diff::convert_to_sections(diff, base);
        }
        Self { resource, sections }
    }
}
impl<S: Section> ModifyEvent<S> {
    /// Gets a reference to the sections of data this event modifies.
    #[must_use]
    pub fn sections(&self) -> &[S] {
        &self.sections
    }
}
impl<S> ModifyEvent<S> {
    /// Returns a reference to the target resource name.
    #[must_use]
    pub fn resource(&self) -> &str {
        &self.resource
    }
}
/// Deletion of a resource.
///
/// The resource must be initialised using [`CreateEvent`].
#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
#[must_use]
pub struct DeleteEvent {
    resource: String,
    /// An optional successor to the [`Self::resource()`]
    successor: Option<String>,
}
impl DeleteEvent {
    /// Creates a new delete event.
    ///
    /// The successor, if present, directs all more recent modifications of the deleted resource
    /// ([`Self::resource`]) to another resource.
    /// This is useful for when a file is renamed.
    ///
    /// > Having a `MoveEvent` was experimented with, but ultimately failed.
    /// > Dig into the old git commits to see comments about it.
    pub fn new(resource: String, successor: Option<String>) -> Self {
        Self {
            resource,
            successor,
        }
    }

    /// Returns a reference to the target resource name.
    #[must_use]
    pub fn resource(&self) -> &str {
        &self.resource
    }
    /// Returns a reference to the optional successor.
    #[must_use]
    pub fn successor(&self) -> Option<&str> {
        self.successor.as_deref()
    }
}
/// The creation of a resource.
///
/// Creates an empty file. Overrides the file if it already exists.
#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
#[must_use]
pub struct CreateEvent {
    resource: String,
}
impl CreateEvent {
    /// Signals `resource` should be created, or overridden if present.
    pub fn new(resource: String) -> Self {
        Self { resource }
    }

    /// Returns a reference to the target resource name.
    #[must_use]
    pub fn resource(&self) -> &str {
        &self.resource
    }
}

macro_rules! event_kind_impl {
    ($type:ty, $enum_name:ident) => {
        impl<'a, T> From<$type> for EventKind<T> {
            fn from(event: $type) -> Self {
                Self::$enum_name(event)
            }
        }
        impl<'a, T> IntoEvent<T> for $type {
            fn into_ev(self, manager: &Manager) -> Event<T> {
                Event::new(self.into(), manager.uuid())
            }
        }
    };
}

/// Helper trait to convert from `*Event` structs to [`Event`].
///
/// Should not be implemented but used with [`Manager::process_event`].
pub trait IntoEvent<SectionKind> {
    /// Converts `self` into an [`Event`].
    fn into_ev(self, manager: &Manager) -> Event<SectionKind>;
}
impl<T> IntoEvent<T> for Event<T> {
    fn into_ev(self, _manager: &Manager) -> Event<T> {
        self
    }
}

event_kind_impl!(ModifyEvent<T>, Modify);
event_kind_impl!(CreateEvent, Create);
event_kind_impl!(DeleteEvent, Delete);

/// Returns the time since [`UNIX_EPOCH`].
pub(crate) fn dur_now() -> Duration {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::ZERO)
}

/// The kind of change of data.
#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
#[must_use]
pub enum EventKind<S> {
    /// Modification.
    ///
    /// You need to make a [`Self::Create`] event before modifying the resource.
    /// If you don't do this, the modification MUST NOT be applied.
    Modify(ModifyEvent<S>),
    /// Creation.
    ///
    /// A new resource has been created. Before any other event can affect this resource,
    /// you'll have to initialise it with this event.
    Create(CreateEvent),
    /// Deletion.
    ///
    /// Can contain a [`DeleteEvent::successor`] to hint on where the file has been moved to. This
    /// enables subsequent [`Event`]s to be redirected to the successor.
    /// The redirections will stop when a new [`Self::Create`] event is triggered.
    Delete(DeleteEvent),
}
/// A change of data.
#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
#[must_use]
pub struct Event<S> {
    kind: EventKind<S>,
    /// Duration since UNIX_EPOCH
    timestamp: Duration,
    sender: Uuid,
}
impl<S> Event<S> {
    /// Creates a new event from `kind`.
    pub fn new(kind: EventKind<S>, sender: Uuid) -> Self {
        Self::with_timestamp(kind, sender, SystemTime::now())
    }
    /// Creates a new event from `kind` with the `timestamp`.
    ///
    /// **NOTE**: Be very careful with this. `timestamp` MUST be within a second of real time,
    /// else the sync will risk wrong results, forcing [`MessageKind::HashCheck`].
    pub fn with_timestamp(kind: EventKind<S>, sender: Uuid, timestamp: SystemTime) -> Self {
        Self {
            kind,
            timestamp: timestamp
                .duration_since(UNIX_EPOCH)
                .unwrap_or(Duration::ZERO),
            sender,
        }
    }
    /// Returns a reference to the target resource name.
    #[allow(clippy::inline_always)]
    #[inline(always)]
    #[must_use]
    pub fn resource(&self) -> &str {
        match &self.kind {
            EventKind::Modify(ev) => ev.resource(),
            EventKind::Create(ev) => ev.resource(),
            EventKind::Delete(ev) => ev.resource(),
        }
    }
    /// Returns a reference to the inner [`EventKind`] where all the event data is stored.
    #[allow(clippy::inline_always)]
    #[inline(always)]
    pub fn inner(&self) -> &EventKind<S> {
        &self.kind
    }
    /// Returns a mutable reference to the inner [`EventKind`].
    #[inline]
    pub(crate) fn inner_mut(&mut self) -> &mut EventKind<S> {
        &mut self.kind
    }
    /// Gets the timestamp of this event.
    ///
    /// The returned [`Duration`] is the time since [`SystemTime::UNIX_EPOCH`].
    #[inline]
    #[must_use]
    pub fn timestamp(&self) -> Duration {
        self.timestamp
    }
    /// Returns the UUID of the sender of this event.
    #[inline]
    pub fn sender(&self) -> Uuid {
        self.sender
    }
}
/// Clones the `resource` [`String`].
impl<S: Section> From<&Event<S>> for Event<section::Empty> {
    fn from(ev: &Event<S>) -> Self {
        let kind = match ev.inner() {
            EventKind::Modify(ev) => EventKind::Modify(ModifyEvent {
                resource: ev.resource().into(),
                sections: ev.sections().iter().map(section::Empty::new).collect(),
            }),
            EventKind::Create(ev) => EventKind::Create(ev.clone()),
            EventKind::Delete(ev) => EventKind::Delete(ev.clone()),
        };
        Event {
            kind,
            timestamp: ev.timestamp(),
            sender: ev.sender(),
        }
    }
}
/// A [`Event`] with internal data.
///
/// This is the type that is sent between clients.
pub type DatafulEvent = Event<VecSection>;

/// A UUID.
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Clone, Copy, Serialize, Deserialize, Hash)]
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
///
/// On direct messages, send a conversation UUID which can be [`Self::Canceled`].
// `TODO`: implement the rest of these.
#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
#[must_use]
pub enum MessageKind {
    /// The client sending this is connecting to the network.
    ///
    /// Will declare it's capabilities.
    ///
    /// # Replies
    ///
    /// Expects a [`Self::Welcome`], [`Self::InvalidUuid`], or [`Self::MismatchingVersions`].
    Hello(Capabilities),
    /// Response to the [`Self::Hello`] message.
    Welcome(Capabilities),
    /// The sender of [`MessageKind::Hello`] of the contained [`Uuid`] uses an occupied UUID.
    ///
    /// If a critical count of piers respond with this,
    /// change UUID and send [`Self::Hello`] again.
    InvalidUuid(Uuid),
    /// The [`Capabilities::version()`] is not compatible.
    ///
    /// The sending client (with the contained [`Uuid`])
    /// will not add UUID of the [`Self::Hello`] message to the known clients.
    /// The sender of the Hello should ignore all future messages from this client.
    MismatchingVersions(Uuid),
    /// A client has new data to share.
    Event(DatafulEvent),
    /// A client tries to get the most recent data.
    /// Contains the list of which documents were edited and size at last session.
    /// `TODO`: Only sync the remote repo, as that's what we want to sync so we can commit.
    ///
    /// # Replies
    ///
    /// You should respond with a [`Self::FastForwardReply`].
    /// That contains which resources you should sync.
    ///
    /// Then, send a [`Self::Sync`] request and
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
    /// Checks the internal event UUID log.
    ///
    /// # Replies
    ///
    /// Always send back a [`Self::EventUuidLogCheckReply`] to tell others which "version" you
    /// have.
    ///
    /// Wait a few seconds (e.g. 10) and then call [`Manager::assure_event_uuid_log`].
    /// If any discrepancy is found, you should send back a [`Self::HashCheck`].
    /// If everything is ok, don't respond.
    EventUuidLogCheck {
        /// The UUID of this check conversation.
        uuid: Uuid,
        /// The data of the check.
        check: EventUuidLogCheck,
    },
    /// A reply to [`Self::EventUuidLogCheck`].
    ///
    /// This is used to determine which "version" of the data is the correct one.
    /// This should not be responded to, but maybe kept a few seconds to keep the piers with
    /// the "correct version".
    EventUuidLogCheckReply {
        /// The UUID of this check conversation.
        uuid: Uuid,
        /// The data of the check.
        check: EventUuidLogCheck,
    },
    /// The target client cancelled the request.
    ///
    /// This may be the result of too many requests.
    /// Should not be sent as a signal of not supporting the feature.
    ///
    /// If a pier request a check from all and we've reached our limit, don't send this.
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
    #[allow(clippy::missing_panics_doc)]
    #[must_use]
    pub fn bin(&self) -> Vec<u8> {
        // UNWRAP: this should be good; we only use objects from ::std and our own derived
        bincode::encode_to_vec(
            bincode::serde::Compat(self),
            bincode::config::Configuration::standard(),
        )
        .unwrap()
    }
    /// # Errors
    ///
    /// Returns an appropriate error if the deserialisation failed.
    pub fn from_bin(slice: &[u8]) -> Result<Self, bincode::error::DecodeError> {
        bincode::serde::decode_from_slice(slice, bincode::config::Configuration::standard())
    }
    /// Converts the message to a plain text compatible encoding, namely Base64.
    ///
    /// > This is a optimised version of converting [`Self::bin()`] to Base64.
    /// > Since I'm using readers and writers, less allocations are needed.
    #[allow(clippy::missing_panics_doc)]
    #[must_use]
    pub fn base64(&self) -> String {
        struct Writer<W: std::io::Write>(W);
        impl<W: std::io::Write> bincode::enc::write::Writer for Writer<W> {
            fn write(&mut self, bytes: &[u8]) -> Result<(), bincode::error::EncodeError> {
                self.0
                    .write_all(bytes)
                    .map_err(|err| bincode::error::EncodeError::Io {
                        error: err,
                        index: 0,
                    })
            }
        }
        let heuristic_size = std::mem::size_of_val(&self);
        let mut string = String::with_capacity(heuristic_size);
        let writer = Writer(base64::write::EncoderStringWriter::from(
            &mut string,
            base64::STANDARD,
        ));
        bincode::encode_into_writer(
            bincode::serde::Compat(self),
            writer,
            bincode::config::Configuration::standard(),
        )
        .unwrap();
        string
    }
    /// # Errors
    ///
    /// Returns an appropriate error if the deserialisation failed.
    /// If the Base64 encoding is wrong, the error returned is
    /// [`bincode::error::DecodeError::OtherString`] which starts with `base64 decoding failed`.
    pub fn from_base64(string: &str) -> Result<Self, bincode::error::DecodeError> {
        struct Reader<R: std::io::Read>(R);
        impl<R: std::io::Read> bincode::de::read::Reader for Reader<R> {
            fn read(&mut self, bytes: &mut [u8]) -> Result<(), bincode::error::DecodeError> {
                self.0.read_exact(bytes).map_err(|err| {
                    bincode::error::DecodeError::OtherString(format!(
                        "base64 decoding failed: {:?}",
                        err
                    ))
                })
            }
        }
        let mut cursor = std::io::Cursor::new(string);

        let reader = Reader(base64::read::DecoderReader::new(
            &mut cursor,
            base64::STANDARD,
        ));
        let decoded: Result<bincode::serde::Compat<Message>, bincode::error::DecodeError> =
            bincode::decode_from_reader(reader, bincode::config::Configuration::standard());
        decoded.map(|compat| compat.0)
    }
}

/// The main manager of a client.
///
/// The `process_*` methods are for creating [`Message`]s from internal data.
/// The `apply_*` methods are for accepting and evaluating [`Message`]s.
#[derive(Debug)]
#[must_use]
pub struct Manager {
    capabilities: Capabilities,
    rng: Mutex<rand::rngs::ThreadRng>,
    piers: HashMap<Uuid, Capabilities>,

    event_log: log::EventLog,
    event_uuid_log: log::EventUuidLog,
    event_uuid_conversation_piers: log::EventUuidReplies,
}
impl Manager {
    /// Creates a empty manager.
    ///
    /// Call [`Self::process_hello()`] to get a hello message.
    ///
    /// The options are partially explained in [`Capabilities::new`].
    /// I recommend a default of `60s` for `log_lifetime` and `512` for `message_log_limit`.
    pub fn new(
        persistent: bool,
        help_desire: i16,
        log_lifetime: Duration,
        event_log_limit: u32,
    ) -> Self {
        let mut rng = rand::thread_rng();
        let uuid = Uuid::with_rng(&mut rng);
        let capabilities = Capabilities::new(uuid, persistent, help_desire);

        Self {
            capabilities,
            rng: Mutex::new(rng),
            piers: HashMap::new(),

            event_log: log::EventLog::new(log_lifetime),
            event_uuid_log: log::EventUuidLog::new(event_log_limit),
            event_uuid_conversation_piers: log::EventUuidReplies::new(),
        }
    }
    /// Get the random number generator of this manager.
    fn rng(&self) -> impl DerefMut<Target = impl rand::Rng> + '_ {
        self.rng.lock().unwrap()
    }
    /// Gets the UUID of this client.
    pub fn uuid(&self) -> Uuid {
        self.capabilities.uuid()
    }
    /// Generates a UUID using the internal [`rand::Rng`].
    #[inline]
    pub(crate) fn generate_uuid(&mut self) -> Uuid {
        let mut rng = self.rng();
        Uuid::with_rng(&mut *rng)
    }
    /// Creates a [`Message`] with [`Self::uuid`] and a random message [`Uuid`].
    #[inline]
    fn process(&mut self, message: MessageKind) -> Message {
        Message::new(message, self.uuid(), self.generate_uuid())
    }
    /// Creates a [`MessageKind::Hello`] with the [`Capabilities`] of this manager.
    pub fn process_hello(&mut self) -> Message {
        self.process(MessageKind::Hello(self.capabilities.clone()))
    }
    /// Takes a [`DatafulEvent`] and returns a [`Message`].
    ///
    /// Be careful with [`EventKind::Modify`] as you NEED to have a
    /// [`EventKind::Create`] before it on the same resource.
    #[inline]
    pub fn process_event(&mut self, event: impl IntoEvent<VecSection>) -> Message {
        self.process(MessageKind::Event(event.into_ev(self)))
    }
    /// May return a message with it's `count` set lower than this.
    ///
    /// `count` should be around 1/4 of the limit of the event UUID log to maximise profits for all
    /// piers on the network.
    ///
    /// If there isn't enough events in the log, this returns [`None`].
    ///
    /// # Replies
    ///
    /// After sending this, wait a few seconds and then run [`Self::assure_event_uuid_log`]
    /// which checks if you have the "correct" version.
    ///
    /// When you call [`Self::apply_event_uuid_log_check`], the manager notes the
    /// [`MessageKind::EventUuidLogCheckReply`] messages.
    ///
    /// # Memory leaks
    ///
    /// You must call [`Self::assure_event_uuid_log`] after calling this.
    pub fn process_event_uuid_log_check(&mut self, count: u32) -> Option<Message> {
        // after this call, we are guaranteed to have at least 1 event in the log.
        let (pos, cutoff_timestamp) = self.event_uuid_log.appropriate_cutoff()?;
        // this should NEVER not fit inside an u32 as the limit is an u32.
        #[allow(clippy::cast_possible_truncation)]
        let possible_count = (self.event_uuid_log.len() - 1 - pos) as u32;
        // If possible_count is less than half the requested, return nothing.
        if possible_count * 2 < count {
            return None;
        }

        let count = cmp::min(count, possible_count);

        let check = self
            .event_uuid_log
            .get(count, pos, cutoff_timestamp)
            .expect(
                "with the values we give, this shouldn't panic. Report this bug if it has occured.",
            );
        let uuid = self.generate_uuid();

        self.event_uuid_conversation_piers
            .insert(uuid, check.clone(), self.uuid());

        Some(self.process(MessageKind::EventUuidLogCheck { uuid, check }))
    }
    /// Applies `event` to this manager. You get back a [`log::EventApplier`] on which you should
    /// handle the events.
    ///
    /// # Errors
    ///
    /// Fails if the `event` is more than 10s from the future.
    ///
    /// This prevents other clients from hogging our memory with items which never expire.
    /// If their clock is more than 10s off relative to our, we have a serious problem!
    pub fn apply_event<'a>(
        &'a mut self,
        event: &'a DatafulEvent,
        message_uuid: Uuid,
    ) -> Result<log::EventApplier<'a, VecSection>, log::Error> {
        self.event_log.insert(event, message_uuid)?;
        self.event_uuid_log.insert(message_uuid, event.timestamp);
        Ok(self.event_log.event_applier(event, message_uuid))
    }
    /// Handles a [`MessageKind::EventUuidLogCheck`].
    /// This will return an [`EventUuidLogCheckAction`] which tells you what to do.
    ///
    /// # Memory leaks
    ///
    /// You must call [`Self::assure_event_uuid_log`] after calling this.
    pub fn apply_event_uuid_log_check(
        &mut self,
        check: EventUuidLogCheck,
        conversation_uuid: Uuid,
        remote_uuid: Uuid,
    ) -> EventUuidLogCheckAction {
        fn new_cutoff(log: &log::EventUuidLog, cutoff: Duration, count: u32) -> EventUuidLogCheckAction {
            let pos = if let Some(pos) = log.cutoff_from_time(cutoff) {
                pos
            } else {
                return EventUuidLogCheckAction::Nothing;
            };
            match log.get(count, pos, cutoff) {
                Ok(check) => EventUuidLogCheckAction::SendAndFurtherCheck(check),
                Err(log::EventUuidLogError::CountTooBig) => EventUuidLogCheckAction::Nothing,
                Err(log::EventUuidLogError::CutoffMissing) => {
                    unreachable!("we got the cutoff above, this must exist in the log.")
                }
            }
        }

        let cutoff = if let Some(cutoff) = self.event_uuid_log.cutoff_from_uuid(check.cutoff()) {
            cutoff
        } else {
            return new_cutoff(
                &self.event_uuid_log,
                check.cutoff_timestamp(),
                check.count(),
            );
        };
        let action = match self
            .event_uuid_log
            .get(check.count(), cutoff, check.cutoff_timestamp())
        {
            Ok(check) => EventUuidLogCheckAction::Send(check),
            Err(err) => match err {
                // We don't have a large enough log. Ignore.
                // See comment in [`Self::process_event_uuid_log_check`].
                log::EventUuidLogError::CountTooBig => EventUuidLogCheckAction::Nothing,
                // We don't have the UUID of the cutoff!
                log::EventUuidLogError::CutoffMissing => new_cutoff(
                    &self.event_uuid_log,
                    check.cutoff_timestamp(),
                    check.count(),
                ),
            },
        };

        self.event_uuid_conversation_piers
            .insert(conversation_uuid, check, remote_uuid);

        action
    }
    /// Assures you are using the "correct" version of the files.
    /// This return [`None`] if that's the case.
    /// Otherwise returns an appropriate pier to get data from.
    ///
    /// Also returns [`None`] if the conversation wasn't found or no responses were sent.
    #[allow(clippy::missing_panics_doc)] // It's safe.
    pub fn assure_event_uuid_log(&mut self, conversation_uuid: Uuid) -> Option<SelectedPier> {
        let conversation = self.event_uuid_conversation_piers.get(conversation_uuid)?;
        let total_reponses = conversation.len();
        // We are one response.
        if total_reponses < 2 {
            return None;
        }
        let mut options: Vec<([u8; 16], usize)> =
            Vec::with_capacity(cmp::min(total_reponses / 3, 20));

        for option in conversation.values() {
            match options.binary_search_by(|(hash, _count)| hash.cmp(option.hash())) {
                Ok(pos) => options[pos].1 += 1,
                Err(pos) => options.insert(pos, (*option.hash(), 1)),
            }
        }
        let mut most_popular = (&options[0].0, options[0].1);
        for (hash, count) in &options {
            let count = *count;

            if count > most_popular.1 {
                most_popular = (hash, count);
            }
        }

        // Take the pier with highest help_desire and UUID
        if most_popular.1 * 3 < total_reponses * 2 {
            let mut highest = None;

            for (pier, check) in conversation.iter() {
                if check.hash() != most_popular.0 {
                    continue;
                }
                let pier = if let Some(pier) = self.piers.get(pier) {
                    pier
                } else {
                    continue;
                };
                match pier
                    .help_desire()
                    .cmp(&highest.map_or(i16::MIN, |(_, desire)| desire))
                {
                    cmp::Ordering::Less => {}
                    cmp::Ordering::Equal => match pier
                        .uuid()
                        .cmp(&highest.map_or(Uuid(0), |(uuid, _)| uuid))
                    {
                        // Oh, shit
                        cmp::Ordering::Equal | cmp::Ordering::Less => {}
                        cmp::Ordering::Greater => highest = Some((pier.uuid(), pier.help_desire())),
                    },
                    cmp::Ordering::Greater => highest = Some((pier.uuid(), pier.help_desire())),
                }
            }
            highest.map(|(uuid, _)| SelectedPier::new(uuid))
        } else {
            self.choose_pier(|uuid, _| {
                let pier_check = if let Some(check) = conversation.get(&uuid) {
                    check
                } else {
                    return false;
                };
                pier_check.hash() == most_popular.0
            })
        }
    }

    pub(crate) fn filter_piers<'a>(
        &'a self,
        filter: impl Fn(Uuid, &Capabilities) -> bool + 'a + Clone,
    ) -> impl Iterator<Item = (Uuid, &'a Capabilities)> + Clone {
        self.piers.iter().filter_map(move |(uuid, capabilities)| {
            if filter(*uuid, capabilities) {
                Some((*uuid, capabilities))
            } else {
                None
            }
        })
    }
    /// Returns [`None`] if no pier was accepted from the `filter`.
    pub(crate) fn choose_pier(
        &self,
        filter: impl Fn(Uuid, &Capabilities) -> bool + Clone,
    ) -> Option<SelectedPier> {
        let mut total_desire = 0.0;
        for (_, capabilities) in self
            .filter_piers(|uuid, capabilities| uuid != self.uuid() && filter(uuid, capabilities))
        {
            total_desire += capabilities.effective_help_desire();
        }
        if total_desire == 0.0 {
            return None;
        }
        let mut random = self.rng().gen_range(0.0..total_desire);

        for (uuid, capabilities) in self
            .filter_piers(|uuid, capabilities| uuid != self.uuid() && filter(uuid, capabilities))
        {
            let desire = capabilities.effective_help_desire();
            if random < desire {
                return Some(SelectedPier::new(uuid));
            }
            random -= desire;
        }
        // `random` will always be less than the total desire. (it's a exclusive range).
        // Therefore, if `random` is `total_desire - EPSILON`, the previous iterations will have
        // subtracted so it will be less than or equal to `desire - EPSILON`, which is less than
        // `desire`.
        unreachable!("Please report this internal bug regarding choosing an appropriate pier from it's help desire.")
    }
}

/// The appropriate pier selected from all piers [`Capabilities::help_desire`].
///
/// Used for methods of [`Manager`] to send data to specific piers.
#[derive(Debug)]
pub struct SelectedPier {
    uuid: Uuid,
}
impl SelectedPier {
    pub(crate) fn new(uuid: Uuid) -> Self {
        Self { uuid }
    }
}
