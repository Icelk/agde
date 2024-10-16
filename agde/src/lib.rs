//! A general decentralized sync library supporting text and binary.
//!
//! # Vocabulary
//!
//! Here are some terms I use throughout the codebase and documentation.
//!
//! - resource - a piece of data (e.g. a file, online shared document).
//!     May also be used to denote the location of a resource.
//! - pier - another client on the network.
//! - help desire - how much a pier wants to help others in the network.
//! - conversation - a exchange of some related data (e.g. [`MessageKind::LogCheck`]).
//! - [`dach::Difference`] a modification to a resource.
//! - [UUID](Uuid) - a unique identifier for a unit (e.g. conversation, [`Message`])
//! - log - internal list to compensate for inconsistencies in message arrival time.
//! - storage - "versions" of the data stored by the implementor
//!     - current storage - the current data. The resources stored here are the ones edited by the
//!       end-user.
//!     - public storage - what the others think our current storage looks like.
//!       Separating these enables getting the difference between the storages.
//!       This returns what we need to send to the other piers, how to mutate the public storage to
//!       get the current storage.

#![deny(
    clippy::pedantic,
    unreachable_pub,
    missing_debug_implementations,
    missing_docs
)]

pub mod event;
pub mod fast_forward;
pub mod hash_check;
pub mod log;
pub mod resource;
pub mod sync;
pub mod utils;

use std::cmp;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fmt::{self, Debug, Display};
use std::ops::DerefMut;
use std::sync::Mutex;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use rand::{Rng, SeedableRng};
use serde::{Deserialize, Serialize};

pub use dach;
pub use event::{Event, IntoEvent, Kind as EventKind};
pub use log::{Check as LogCheck, CheckAction as LogCheckAction};

/// The current version of this `agde` library.
pub const VERSION: semver::Version = semver::Version::new(0, 1, 0);

const BASE64_ENGINE: base64::engine::GeneralPurpose = base64::engine::GeneralPurpose::new(
    &base64::alphabet::STANDARD,
    base64::engine::GeneralPurposeConfig::new()
        .with_decode_padding_mode(base64::engine::DecodePaddingMode::Indifferent),
);

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
    /// Check is the versions are compatible.
    #[must_use]
    pub fn version_compatible(&self, other: &Capabilities) -> bool {
        let me = self.version();
        let other = other.version();
        let comparator = semver::Comparator {
            op: semver::Op::Caret,
            major: me.major,
            minor: Some(me.minor),
            patch: Some(me.patch),
            pre: me.pre.clone(),
        };
        comparator.matches(other)
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
        if effective >= 0.0 {
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

/// A UUID.
#[derive(PartialEq, Eq, PartialOrd, Ord, Clone, Copy, Serialize, Deserialize, Hash)]
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
    /// Get the inner data.
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
impl Debug for Uuid {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        Display::fmt(&self, f)
    }
}
impl Display for Uuid {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:x}", self.inner())
    }
}

/// The kinds of messages with their data. Part of a [`Message`].
///
/// On direct messages, send a conversation UUID which can be [`Self::Cancelled`].
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
    ///
    /// These capabilities are our own, giving the remote information about us.
    ///
    /// When the inner `recipient` is `None`, this updates the other piers'
    /// information about the client. Else, it's just a response to a greeting.
    Welcome {
        /// The target of this message.
        recipient: Option<Uuid>,
        /// Which capabilities we have.
        info: Capabilities,
    },
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
    Event(Event),
    /// A client tries to get the most recent data.
    /// Contains the list of which documents were edited and size at last session.
    ///
    /// # Replies
    ///
    /// You should respond with a [`Self::FastForwardReply`].
    /// That contains which resources you should sync.
    ///
    /// Then, send a [`Self::Sync`] request and handle the actual data transmission.
    FastForward(fast_forward::Request),
    /// A reply to a [`Self::FastForward`] request.
    FastForwardReply(fast_forward::Response),
    /// A request to get the diffs and sync the specified resources.
    Sync(sync::Request),
    /// The response with hashes of the specified resources.
    SyncReply(sync::Response),
    /// Requests all the hashes of all the resources specified in [`resource::Matcher`].
    HashCheck(hash_check::Request),
    /// A reply with all the hashes of all the requested files.
    HashCheckReply(hash_check::Response),
    /// Checks the internal event UUID log.
    ///
    /// # Replies
    ///
    /// Always send back a [`Self::LogCheckReply`] to tell others which "version" you
    /// have.
    ///
    /// Wait a few seconds (e.g. 10) and then call [`Manager::assure_log_check`].
    /// If any discrepancy is found, you should send back a [`Self::HashCheck`].
    /// If everything is ok, don't respond.
    LogCheck {
        /// The UUID of this check conversation.
        conversation_uuid: Uuid,
        /// The data of the check.
        check: LogCheck,
    },
    /// A reply to [`Self::LogCheck`].
    ///
    /// This is used to determine which "version" of the data is the correct one.
    /// This should not be responded to, but maybe kept a few seconds to keep the piers with
    /// the "correct version".
    LogCheckReply {
        /// The UUID of this check conversation.
        conversation_uuid: Uuid,
        /// The data of the check.
        check: LogCheck,
    },
    /// The target client cancelled the request.
    ///
    /// This may be the result of too many requests.
    /// Should not be sent as a signal of not supporting the feature.
    ///
    /// If a pier requests a check from all and we've reached our limit, don't send this.
    Cancelled(Uuid),
    /// The sender is disconnecting.
    Disconnect,
    /// Data sent by the user for other users.
    /// This can be useful when wanting to send other metadata along with the usual message
    /// exchange.
    User {
        /// The recipient for this message. Will be sent and accepted by all if this is [`None`].
        recipient: Option<Uuid>,
        /// The data the user wants to transmit to `recipient`.
        data: Vec<u8>,
    },
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
    /// Get the sender UUID.
    #[inline]
    pub fn sender(&self) -> Uuid {
        self.sender
    }
    /// Get the message UUID.
    #[inline]
    pub fn uuid(&self) -> Uuid {
        self.uuid
    }
    /// Get the inner [`MessageKind`].
    ///
    /// This contains all the data of the message.
    #[inline]
    pub fn inner(&self) -> &MessageKind {
        &self.kind
    }
    /// Get a mutable reference to the inner [`MessageKind`].
    ///
    /// This contains all the data of the message.
    #[inline]
    pub fn inner_mut(&mut self) -> &mut MessageKind {
        &mut self.kind
    }
    /// Get the inner [`MessageKind`].
    ///
    /// This contains all the data of the message.
    #[inline]
    pub fn into_inner(self) -> MessageKind {
        self.kind
    }

    /// Get the specific recipient if the [`MessageKind`] is targeted.
    ///
    /// Can be used to determine to send this message to only one pier or all.
    #[inline]
    pub fn recipient(&self) -> Recipient {
        Recipient::Selected(SelectedPier::new(match self.inner() {
            MessageKind::Welcome {
                recipient: Some(recipient),
                info: _,
            } => *recipient,
            MessageKind::Sync(sync) => sync.recipient(),
            MessageKind::SyncReply(sync) => sync.recipient(),
            MessageKind::HashCheck(request) => request.recipient(),
            MessageKind::HashCheckReply(response) => response.recipient(),
            MessageKind::Cancelled(uuid) => *uuid,
            MessageKind::FastForward(ff) => ff.recipient(),
            MessageKind::FastForwardReply(ff) => ff.recipient(),
            MessageKind::User {
                recipient: Some(r),
                data: _,
            } => *r,
            _ => return Recipient::All,
        }))
    }

    /// Converts the message to bytes.
    ///
    /// You can also use [`bincode`] or any other [`serde`]-based library to serialize the message.
    #[allow(clippy::missing_panics_doc)]
    #[must_use]
    pub fn to_bin(&self) -> Vec<u8> {
        // UNWRAP: this should be good; we only use objects from ::std and our own, derived
        bincode::serde::encode_to_vec(self, bincode::config::standard().write_fixed_array_length())
            .unwrap()
    }
    /// # Errors
    ///
    /// Returns an appropriate error if the deserialisation failed.
    pub fn from_bin(slice: &[u8]) -> Result<Self, bincode::error::DecodeError> {
        bincode::serde::decode_from_slice(
            slice,
            bincode::config::standard().write_fixed_array_length(),
        )
        .map(|(me, _)| me)
    }
    /// Converts the message to a plain text compatible encoding, namely Base64.
    ///
    /// > This is a optimised version of converting [`Self::to_bin()`] to Base64.
    /// > Since I'm using readers and writers, less allocations are needed.
    #[allow(clippy::missing_panics_doc)]
    #[must_use]
    pub fn to_base64(&self) -> String {
        struct Writer<W: std::io::Write>(W);
        impl<W: std::io::Write> bincode::enc::write::Writer for Writer<W> {
            fn write(&mut self, bytes: &[u8]) -> Result<(), bincode::error::EncodeError> {
                self.0
                    .write_all(bytes)
                    .map_err(|err| bincode::error::EncodeError::Io {
                        inner: err,
                        index: 0,
                    })
            }
        }
        let heuristic_size = std::mem::size_of_val(self);
        let mut string = String::with_capacity(heuristic_size);
        let writer = Writer(base64::write::EncoderStringWriter::from_consumer(
            &mut string,
            &BASE64_ENGINE,
        ));
        bincode::encode_into_writer(
            bincode::serde::Compat(self),
            writer,
            bincode::config::standard(),
        )
        .unwrap();
        string
    }
    /// # Errors
    ///
    /// Returns an appropriate error if the deserialisation failed.
    /// If the Base64 encoding is wrong, the error returned is
    /// [`bincode::error::DecodeError::OtherString`] which (always) starts with `base64 decoding failed`.
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
            &BASE64_ENGINE,
        ));
        let decoded: Result<bincode::serde::Compat<Message>, bincode::error::DecodeError> =
            bincode::decode_from_reader(reader, bincode::config::standard());
        decoded.map(|compat| compat.0)
    }
}

/// The recipient of a [`Message`].
#[derive(Debug, PartialEq, Eq)]
#[must_use]
pub enum Recipient {
    /// Send this message to all piers.
    All,
    /// Send this message to only the [`SelectedPier`].
    Selected(SelectedPier),
}

/// The main manager of a client.
///
/// The `process_*` methods are for creating [`Message`]s from internal data.
/// The `apply_*` methods are for accepting and evaluating [`Message`]s.
#[derive(Debug)]
#[must_use]
pub struct Manager {
    capabilities: Capabilities,
    rng: Mutex<rand::rngs::StdRng>,
    /// Other piers, not including ourselves.
    piers: HashMap<Uuid, Capabilities>,

    event_log: log::Log,
    event_uuid_conversation_piers: log::UuidReplies,

    fast_forward: fast_forward::State,
    /// The remote we're having a conversation with to check our hashes.
    hash_checking: Option<Uuid>,
    /// The piers which have cancelled our fast forward/hash check request.
    /// Should be cleared once the exchange is done (in `apply_sync_reply`).
    cancelled_piers: BTreeSet<Uuid>,
    cancelled_piers_last_changed: Duration,
}
impl Manager {
    /// Creates a empty manager.
    ///
    /// Call [`Self::process_hello()`] to get a hello message.
    ///
    /// The options are partially explained in [`Capabilities::new`].
    /// I recommend a default of `60s` for `log_lifetime` and `512` for `event_log_limit`.
    pub fn new(
        persistent: bool,
        help_desire: i16,
        log_lifetime: Duration,
        event_log_limit: u32,
    ) -> Self {
        let mut rng = rand::rngs::StdRng::from_entropy();
        let uuid = Uuid::with_rng(&mut rng);
        let capabilities = Capabilities::new(uuid, persistent, help_desire);

        Self {
            capabilities,
            rng: Mutex::new(rng),
            piers: HashMap::new(),

            event_log: log::Log::new(log_lifetime, event_log_limit),
            event_uuid_conversation_piers: log::UuidReplies::new(),

            fast_forward: fast_forward::State::NotRunning,
            hash_checking: None,
            cancelled_piers: BTreeSet::new(),
            cancelled_piers_last_changed: utils::dur_now(),
        }
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
    /// Takes a [`Event`] and returns a [`Message`].
    ///
    /// `last_event_send` is the timestamp of when you last sent messages. It's used as the
    /// creation date for the changes, as all our changes are diffed to that point in time.
    ///
    /// Be careful with [`EventKind::Modify`] as you NEED to have a
    /// [`EventKind::Create`] before it on the same resource.
    ///
    /// # Errors
    ///
    /// Returns [`fast_forward::Error::ExpectedNotRunning`] if [`Manager::is_fast_forwarding`] is
    /// true.
    #[inline]
    pub fn process_event(&mut self, event: impl IntoEvent) -> Result<Message, fast_forward::Error> {
        if self.fast_forward != fast_forward::State::NotRunning {
            return Err(fast_forward::Error::ExpectedNotRunning);
        }
        let event = event.into_ev(self);

        let uuid = self.generate_uuid();

        self.event_log.insert(event.clone(), uuid);

        Ok(Message::new(MessageKind::Event(event), self.uuid(), uuid))
    }
    /// May return a message with it's the number of events less than `count`.
    ///
    /// `count` should be around 1/4 of the [limit](Manager::event_log_limit) of the event UUID log to maximise profits for all
    /// piers on the network.
    ///
    /// If there isn't enough events in the log, this returns [`None`].
    ///
    /// This also returns the conversation UUID.
    ///
    /// # Replies
    ///
    /// After sending this, wait a few seconds and then run [`Self::assure_log_check`]
    /// which checks if you have the "correct" version.
    ///
    /// When you call [`Self::apply_event_uuid_log_check`], the manager notes the
    /// [`MessageKind::LogCheckReply`] messages.
    ///
    /// # Memory leaks
    ///
    /// You must call [`Self::assure_log_check`] after calling this.
    #[allow(clippy::missing_panics_doc)]
    pub fn process_event_log_check(&mut self, count: u32) -> Option<(Message, Uuid)> {
        // after this call, we are guaranteed to have at least 1 event in the log.
        let (pos, cutoff_timestamp) = self.event_log.appropriate_cutoff()?;
        // this should NEVER not fit inside an u32 as the limit for the log is an u32.
        #[allow(clippy::cast_possible_truncation)]
        let possible_count = ((self.event_log.len() - 1).saturating_sub(pos)) as u32;
        // If possible_count is less than half the requested, return nothing.
        if possible_count * 2 < count {
            return None;
        }

        let count = cmp::min(count, possible_count);

        let check = self
            .event_log
            .get_uuid_hash(count, pos, cutoff_timestamp)
            .expect(
                "with the values we give, this shouldn't panic. Report this bug if it has occured.",
            );
        let uuid = self.generate_uuid();

        self.event_uuid_conversation_piers
            .insert(uuid, check.clone(), self.uuid());

        Some((
            self.process(MessageKind::LogCheck {
                conversation_uuid: uuid,
                check,
            }),
            uuid,
        ))
    }
    /// Get a message with the reply to a [`MessageKind::LogCheck`].
    pub fn process_event_log_check_reply(
        &mut self,
        event_log_check: log::Check,
        conversation_uuid: Uuid,
    ) -> Message {
        self.process(MessageKind::LogCheckReply {
            conversation_uuid,
            check: event_log_check,
        })
    }
    /// Constructs a message with `pier` as a destination for a full hash check.
    ///
    /// This is between event log check and sync to make sure our data is valid.
    /// Then, we don't need to sync.
    ///
    /// # Errors
    ///
    /// Returns [`fast_forward::Error::ExpectedNotRunning`] if [`Self::is_fast_forwarding`].
    pub fn process_hash_check(
        &mut self,
        pier: SelectedPier,
    ) -> Result<Message, fast_forward::Error> {
        if let fast_forward::State::NotRunning = self.fast_forward {
        } else {
            return Err(fast_forward::Error::ExpectedNotRunning);
        }
        self.hash_checking = Some(pier.uuid());
        let msg = self.process(MessageKind::HashCheck(hash_check::Request::new(
            pier,
            resource::Matcher::all(),
            cmp::min(
                utils::dur_now().saturating_sub(Duration::from_secs(15)),
                self.event_log.lifetime() / 2,
            ),
        )));
        Ok(msg)
    }
    /// Make a message from the [`hash_check::Response`], created using
    /// [`hash_check::ResponseBuilder::finish`].
    ///
    /// The `response` is obtained from [`Self::apply_hash_check`].
    pub fn process_hash_check_reply(&mut self, response: hash_check::Response) -> Message {
        self.process(MessageKind::HashCheckReply(response))
    }
    /// You MUST pause the [`Self::apply_event`] between when the `signature` is created for
    /// [`sync::RequestBuilder::finish`] and when the [`sync::Response`] is applied.
    ///
    /// When you receive the [`sync::Response`], call the appropriate functions on it to apply data
    /// and [`Self::apply_sync_reply`].
    ///
    /// The `metadata_changes` are the changes from our main metadata to [`fast_forward::Response::metadata`].
    /// It should be [`None`] when we're not fast forwarding.
    pub fn process_sync(
        &mut self,
        request: sync::Request,
        metadata_changes: Option<Vec<fast_forward::MetadataChange>>,
    ) -> Message {
        if let Some(metadata_changes) = metadata_changes {
            if let fast_forward::State::WaitingForDiffs {
                pier: _,
                latest_event: _,
                fast_forward_metadata: _,
                changes,
            } = &mut self.fast_forward
            {
                *changes = metadata_changes;
            }
        }
        self.process(MessageKind::Sync(request))
    }
    /// Turn the [`sync`] response to a message.
    ///
    /// This should be sent back to the pier which requested the sync.
    pub fn process_sync_reply(&mut self, response: sync::Response) -> Message {
        self.process(MessageKind::SyncReply(response))
    }
    /// Returns [`None`] if we haven't registered any piers which are willing to talk to us (they
    /// haven't cancelled us).
    ///
    /// # Errors
    ///
    /// Returns [`fast_forward::Error::ExpectedNotRunning`] if [`Self::hash_checking()`] is [`Some`].
    pub fn process_fast_forward(&mut self) -> Result<Option<Message>, fast_forward::Error> {
        if self.hash_checking().is_some() {
            return Err(fast_forward::Error::ExpectedNotRunning);
        }
        self.clean_cancelled_piers();

        // so we can use `?` ↓ for the `None`.
        let mut msg = || {
            let pier = self.choose_pier(|uuid, _| !self.cancelled_piers.contains(&uuid))?;
            self.fast_forward = fast_forward::State::WaitingForMeta { pier: pier.uuid() };
            Some(
                self.process(MessageKind::FastForward(fast_forward::Request::new(
                    pier.uuid(),
                ))),
            )
        };
        Ok(msg())
    }
    /// The `public_metadata` is the metadata of the public storage.
    pub fn process_fast_forward_response(
        &mut self,
        public_metadata: fast_forward::Metadata,
        pier: Uuid,
    ) -> Message {
        self.process(MessageKind::FastForwardReply(fast_forward::Response::new(
            pier,
            public_metadata,
            self.event_log.list.last().map(|ev| ev.message_uuid),
        )))
    }
    /// Create a disconnect message.
    pub fn process_disconnect(&mut self) -> Message {
        self.process(MessageKind::Disconnect)
    }

    /// Handles an incoming [`MessageKind::Hello`].
    /// Immediately send the returned message.
    pub fn apply_hello(&mut self, hello: &Capabilities) -> Message {
        if !hello.version_compatible(hello) {
            return self.process(MessageKind::MismatchingVersions(hello.uuid()));
        }
        if self.choose_pier(|uuid, _| uuid == hello.uuid()).is_some() {
            self.process(MessageKind::InvalidUuid(hello.uuid()))
        } else {
            self.piers.insert(hello.uuid(), hello.clone());
            self.process(MessageKind::Welcome {
                info: self.capabilities.clone(),
                recipient: Some(hello.uuid()),
            })
        }
    }
    /// Records the [`Capabilities`] of the client.
    ///
    /// This is later used to determine which pier to send certain requests to.
    pub fn apply_welcome(&mut self, welcome: Capabilities) {
        self.piers.insert(welcome.uuid(), welcome);
    }
    /// Applies `event` to this manager. You get back a [`log::EventApplier`] on which you should
    /// handle the events.
    ///
    /// Please appropriately sanitize the `event` before calling this, as the event might
    /// try to modify unrelated files (if that's what "resources" are implemented as).
    ///
    /// # Errors
    ///
    /// Fails if the `event` is more than 10s from the future.
    ///
    /// This prevents other clients from hogging our memory with items which never expire.
    /// If their clock is more than 10s off relative to our, we have a serious problem!
    pub fn apply_event<'a>(
        &'a mut self,
        event: &'a Event,
        message_uuid: Uuid,
    ) -> Result<log::EventApplier<'a>, log::Error> {
        let now = utils::dur_now();
        // The timestamp is after now!
        if event.timestamp_dur().saturating_sub(Duration::new(10, 0)) >= now {
            return Err(log::Error::EventInFuture);
        }

        self.event_log.insert(event.clone(), message_uuid);
        let applier = self.event_log.event_applier(event, message_uuid);
        if self.fast_forward == fast_forward::State::NotRunning {
            Ok(applier)
        } else {
            Err(log::Error::FastForwardInProgress)
        }
    }
    /// Handles a [`MessageKind::LogCheck`].
    /// This will return an [`LogCheckAction`] which tells you what to do.
    ///
    /// # Memory leaks
    ///
    /// You must call [`Self::assure_log_check`] after calling this.
    pub fn apply_event_uuid_log_check(
        &mut self,
        check: LogCheck,
        conversation_uuid: Uuid,
        sender: Uuid,
    ) -> LogCheckAction {
        fn new_cutoff(log: &log::Log, cutoff: Duration, count: u32) -> LogCheckAction {
            let pos = if let Some(pos) = log.cutoff_from_time(cutoff) {
                pos
            } else {
                return LogCheckAction::Nothing;
            };
            match log.get_uuid_hash(count, pos, cutoff) {
                Ok(check) => LogCheckAction::Send(check),
                Err(log::UuidError::CountTooBig) => LogCheckAction::Nothing,
                Err(log::UuidError::CutoffMissing) => {
                    unreachable!("we got the cutoff above, this must exist in the log.")
                }
            }
        }

        if self.is_fast_forwarding() {
            return LogCheckAction::Nothing;
        }

        let cutoff = if let Some(cutoff) = self.event_log.cutoff_from_uuid(check.cutoff()) {
            cutoff
        } else {
            return new_cutoff(&self.event_log, check.cutoff_timestamp(), check.count());
        };
        let action =
            match self
                .event_log
                .get_uuid_hash(check.count(), cutoff, check.cutoff_timestamp())
            {
                Ok(check) => LogCheckAction::Send(check),
                Err(err) => match err {
                    // We don't have a large enough log. Ignore.
                    // See comment in [`Self::process_log_check`].
                    log::UuidError::CountTooBig => LogCheckAction::Nothing,
                    // We don't have the UUID of the cutoff!
                    log::UuidError::CutoffMissing => {
                        new_cutoff(&self.event_log, check.cutoff_timestamp(), check.count())
                    }
                },
            };

        self.apply_log_check_reply(check, conversation_uuid, sender);

        action
    }
    /// Apply a reply to the [`MessageKind::LogCheck`].
    /// This tracks the `check` from `sender` to be used in the next [`Manager::assure_log_check`].
    pub fn apply_log_check_reply(
        &mut self,
        check: LogCheck,
        conversation_uuid: Uuid,
        sender: Uuid,
    ) {
        self.event_uuid_conversation_piers
            .insert(conversation_uuid, check, sender);
    }
    /// Trims the memory usage of the cache used by [`Self::apply_event_uuid_log_check`].
    ///
    /// Call this maybe once per minute.
    /// This just checks modification timestamps, so it's pretty fast.
    ///
    /// This removes conversations not used by agde for 5 minutes. This is a implementation detail
    /// and can not be relied upon.
    #[inline]
    pub fn clean_log_checks(&mut self) {
        self.event_uuid_conversation_piers.clean();
    }
    /// Assures you are using the "correct" version of the files.
    /// This return [`None`] if that's the case.
    /// Otherwise returns an appropriate pier to get data from.
    ///
    /// Also returns [`None`] if the conversation wasn't found or no responses were sent.
    ///
    /// This will clear the conversation with `conversation_uuid`.
    #[allow(clippy::missing_panics_doc)] // It's safe.
    pub fn assure_log_check(&mut self, conversation_uuid: Uuid) -> Option<SelectedPier> {
        self.event_uuid_conversation_piers.update(conversation_uuid);
        let conversation = self.event_uuid_conversation_piers.get(conversation_uuid)?;
        let total_reponses = conversation.len();
        // We are the one response.
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

            for (pier, check) in conversation {
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
            // if we for some reason didn't set our results (our log isn't long enough)
            if let Some(ours) = conversation.get(&self.uuid()) {
                if most_popular.0 == ours.hash() {
                    return None;
                }
            } else {
                // our log wasn't long enough, don't attend this conversation.
                return None;
            }
            let pier = self.choose_pier(|uuid, _| {
                let pier_check = if let Some(check) = conversation.get(&uuid) {
                    check
                } else {
                    return false;
                };
                pier_check.hash() == most_popular.0
            });
            self.event_uuid_conversation_piers.remove(conversation_uuid);
            pier
        }
    }
    /// Use the [`hash_check::ResponseBuilder::unwinder`] before inserting any hashes.
    ///
    /// For each resource which [`hash_check::Request::matches`], execute
    /// [`hash_check::ResponseBuilder::insert`]. When all are inserted, run
    /// [`hash_check::ResponseBuilder::finish`].
    ///
    /// `sender` is the UUID of the pier who sent this, [`Message::sender`].
    #[allow(clippy::unused_self)] // Consistency between functions.
    pub fn apply_hash_check(
        &self,
        check: hash_check::Request,
        sender: Uuid,
    ) -> hash_check::ResponseBuilder {
        let cutoff = cmp::min(check.cutoff_offset(), self.event_log.lifetime() / 2);

        let unwinder = self
            .event_log
            .unwind_to(utils::dur_now().saturating_sub(cutoff), self);
        hash_check::ResponseBuilder::new(sender, check, cutoff, unwinder)
    }
    /// If the returned [`sync::RequestBuilder`] is [`Some`], loop over each resource
    /// in the [`Vec`] next to the `RequestBuilder`. Add all the [`dach::Signature`]s for the resources.
    /// Before creating the signatures, please unwind them using the [`hash_check::Response::unwinder`].
    /// Run [`sync::RequestBuilder::finish`] once every [`dach::Signature`] has been added.
    /// Then, execute [`Self::process_sync`] with the [`sync::RequestBuilder`].
    /// If it's [`None`], the data matches.
    ///
    /// Delete all the resources in the returned [`Vec`] of [`String`].
    /// Even if this doesn't return a [`sync::RequestBuilder`].
    ///
    /// # Errors
    ///
    /// Returns [`fast_forward::Error::ExpectedNotRunning`] if [`Manager::is_fast_forwarding`] is
    /// true.
    /// If `sender` isn't the pier we decided to talk to, or if we haven't initiated a conversation
    /// ([`Self::process_hash_check`]), [`fast_forward::Error::UnexpectedPier`] is returned.
    #[allow(clippy::type_complexity, clippy::unused_self)] // method consistency
    pub fn apply_hash_check_reply(
        &mut self,
        response: &hash_check::Response,
        sender: Uuid,
        our_hashes: &BTreeMap<String, hash_check::ResponseHash>,
    ) -> Result<(Option<(sync::RequestBuilder, Vec<String>)>, Vec<String>), fast_forward::Error>
    {
        /// this could be made faster (using sorted iterators as in elipdotter)
        fn btreemap_difference(
            matches: &mut Vec<String>,
            mut to_delete: Option<&mut Vec<String>>,
            source: &BTreeMap<String, hash_check::ResponseHash>,
            other: &BTreeMap<String, hash_check::ResponseHash>,
        ) {
            for (resource, hash) in source {
                if let Some(other_hash) = other.get(resource) {
                    if hash != other_hash && !matches.contains(resource) {
                        matches.push(resource.clone());
                    }
                    // else, we have the same hash. Good.
                } else {
                    // the other doesn't have the item
                    if let Some(delete) = &mut to_delete {
                        delete.push(resource.clone());
                    } else {
                        matches.push(resource.clone());
                    }
                }
            }
        }
        if let Some(pier) = self.hash_checking {
            if pier != sender {
                return Err(fast_forward::Error::UnexpectedPier);
            }
        } else {
            return Err(fast_forward::Error::UnexpectedPier);
        }
        if self.is_fast_forwarding() {
            return Err(fast_forward::Error::ExpectedNotRunning);
        }
        let mut differing_data = vec![];
        let mut delete = vec![];
        // See which items in response we don't have.
        btreemap_difference(&mut differing_data, None, response.hashes(), our_hashes);
        // See which items the response doesn't have.
        btreemap_difference(
            &mut differing_data,
            Some(&mut delete),
            our_hashes,
            response.hashes(),
        );

        let request = if differing_data.is_empty() {
            None
        } else {
            let revert_to = self
                .event_log
                .cutoff_from_time(response.cutoff_timestamp())
                .map_or(sync::RevertTo::Origin, |cutoff| {
                    sync::RevertTo::To(self.event_log.list[cutoff].message_uuid)
                });

            Some((
                sync::RequestBuilder::new(
                    sender,
                    // Get all events in the log, up to `self.event_log.limit()`
                    Duration::ZERO,
                    self.event_log.limit(),
                    revert_to,
                ),
                differing_data,
            ))
        };
        Ok((request, delete))
    }
    /// Creates a [builder](sync::ResponseBuilder) used to construct a sync response.
    pub fn apply_sync<'a>(
        &'a self,
        request: &'a sync::Request,
        sender: Uuid,
    ) -> sync::ResponseBuilder<'a> {
        sync::ResponseBuilder::new(request, sender, self)
    }
    /// Applies the event log of the sync reply.
    ///
    /// You **MUST** also call the methods on [`sync::Response`] to actually make changes to the
    /// resources returned. This only handles the event log.
    ///
    /// The [`event::Rewinder`] returned should be called for all [modified
    /// resources](sync::Response::diff).
    ///
    /// You can be sure when calling [`Manager::process_fast_forward`] and
    /// [`Manager::process_hash_check`] that they won't return state errors, as this guarantees the
    /// action returned can be ran, because it's currently running.
    ///
    /// # Errors
    ///
    /// Returns [`fast_forward::Error::ExpectedWaitingForDiffs`] if we're syncing non-fast forward
    /// data in a fast forward. Since [`Self::apply_event_uuid_log_check`] returns
    /// [`LogCheckAction::Nothing`] when trying to execute it when fast forwarding,
    /// this is an issue with you accepting a bad message.
    pub fn apply_sync_reply<'a>(
        &'a mut self,
        response: &mut sync::Response,
        sender: Uuid,
    ) -> Result<SyncReplyAction<'a>, fast_forward::Error> {
        match self.fast_forward {
            fast_forward::State::NotRunning => {}
            fast_forward::State::WaitingForMeta { pier }
            | fast_forward::State::WaitingForDiffs {
                pier,
                latest_event: _,
                fast_forward_metadata: _,
                changes: _,
            } => {
                if pier != sender {
                    return Ok(SyncReplyAction::UnexpectedPier);
                }
            }
        }
        if let Some(pier) = self.hash_checking() {
            if pier != sender {
                return Ok(SyncReplyAction::UnexpectedPier);
            }
        }

        self.event_log.merge(response.take_event_log());
        self.event_log.required_event_timestamp = Some(utils::dur_now());

        let cutoff = match response.revert() {
            sync::RevertTo::Latest => self.event_log.list.len(),
            sync::RevertTo::Origin => 0,
            sync::RevertTo::To(uuid) => self
                .event_log
                .cutoff_from_uuid(uuid)
                .map_or(0, |idx| (idx + 1).min(self.event_log.list.len())),
        };
        let slice = &self.event_log.list[cutoff..];

        match core::mem::replace(&mut self.fast_forward, fast_forward::State::NotRunning) {
            fast_forward::State::NotRunning => {
                self.hash_checking = None;
                Ok(SyncReplyAction::HashCheck {
                    unwinder: event::Unwinder::new(slice, Some(self)),
                })
            }
            fast_forward::State::WaitingForMeta { pier } => {
                self.fast_forward = fast_forward::State::WaitingForMeta { pier };
                Err(fast_forward::Error::ExpectedWaitingForDiffs)
            }
            fast_forward::State::WaitingForDiffs {
                pier: _,
                latest_event,
                changes,
                fast_forward_metadata,
            } => {
                let idx = latest_event
                    .and_then(|latest_event| self.event_log.cutoff_from_uuid(latest_event))
                    .unwrap_or(0);
                let timestamp = self
                    .event_log
                    .list
                    .get(idx)
                    .map_or(Duration::ZERO, |ev| ev.event.timestamp_dur());
                self.event_log.required_event_timestamp = Some(timestamp);

                self.fast_forward = fast_forward::State::NotRunning;

                let applier = fast_forward::MetadataApplier::new(fast_forward_metadata, changes);
                Ok(SyncReplyAction::FastForward {
                    rewinder: event::Rewinder::new(slice, self),
                    metadata_applier: applier,
                })
            }
        }
    }
    /// Applies the fast forward reply by modifying the inner state.
    ///
    /// You need to call [`fast_forward::Metadata::changes`] on `reply`
    /// and remove the removed files.
    /// You also need to pass the new and modified files to [`sync::RequestBuilder::insert`].
    ///
    /// # Errors
    ///
    /// `sender` should match with the one provided by [`Manager::process_fast_forward`].
    /// If the internal state doesn't expect this to happen, it will also throw an error.
    pub fn apply_fast_forward_reply(
        &mut self,
        reply: fast_forward::Response,
        sender: Uuid,
    ) -> Result<sync::RequestBuilder, fast_forward::Error> {
        if let fast_forward::State::WaitingForMeta { pier } = self.fast_forward {
            if pier != sender {
                return Err(fast_forward::Error::UnexpectedPier);
            }
        } else {
            return Err(fast_forward::Error::ExpectedWaitingForMeta);
        };
        self.fast_forward = fast_forward::State::WaitingForDiffs {
            pier: sender,
            latest_event: reply.current_event_uuid,
            fast_forward_metadata: reply.metadata,
            // this will be populated when using [`Self::process_sync`]
            changes: vec![],
        };
        Ok(sync::RequestBuilder::new(
            sender,
            self.event_log.lifetime(),
            self.event_log.limit(),
            reply
                .current_event_uuid
                .map_or(sync::RevertTo::Origin, sync::RevertTo::To),
        ))
    }
    /// Handle a disconnect message.
    ///
    /// You should make sure nobody can forge this message, as that could lead to blackmailing of
    /// piers.
    pub fn apply_disconnect(&mut self, sender: Uuid) {
        self.piers.remove(&sender);
        self.event_uuid_conversation_piers.remove_pier(sender);
    }
    /// Accept a cancellation event from `sender`.
    ///
    /// You can be sure when calling [`Manager::process_fast_forward`] and
    /// [`Manager::process_hash_check`] that they won't return state errors, as this guarantees the
    /// action returned can be ran, because it's currently running.
    pub fn apply_cancelled(&mut self, sender: Uuid) -> CancelAction {
        self.cancelled_piers.insert(sender);
        self.cancelled_piers_last_changed = utils::dur_now();
        if let Some(pier) = self.hash_checking {
            if sender == pier {
                let pier = self.hash_check_persistent();
                return pier.map_or(CancelAction::Nothing, CancelAction::HashCheck);
            }
        }
        match self.fast_forward {
            fast_forward::State::NotRunning => {}
            fast_forward::State::WaitingForMeta { pier }
            | fast_forward::State::WaitingForDiffs {
                pier,
                latest_event: _,
                fast_forward_metadata: _,
                changes: _,
            } => {
                if pier == sender {
                    return CancelAction::FastForward;
                }
            }
        }
        CancelAction::Nothing
    }
}
impl Manager {
    /// Get the UUID of this client.
    #[inline]
    pub fn uuid(&self) -> Uuid {
        self.capabilities.uuid()
    }
    /// Get the random number generator of this manager.
    #[inline]
    #[allow(clippy::missing_panics_doc)] // it's a mutex, it shouldn't be poisoned
    pub fn rng(&self) -> impl DerefMut<Target = impl rand::Rng> + '_ {
        self.rng.lock().unwrap()
    }
    /// Generates a UUID using the internal [`rand::Rng`].
    #[inline]
    pub fn generate_uuid(&mut self) -> Uuid {
        let mut rng = self.rng();
        Uuid::with_rng(&mut *rng)
    }
    /// Get a reference to this manager's capabilities.
    #[inline]
    pub fn capabilities(&self) -> &Capabilities {
        &self.capabilities
    }
    /// The max capacity of the internal event log.
    ///
    /// Useful when calling [`Manager::process_event_log_check`].
    #[inline]
    pub fn event_log_limit(&self) -> u32 {
        self.event_log.limit()
    }

    /// The number of piers we know of.
    #[must_use]
    #[inline]
    pub fn pier_count(&self) -> usize {
        self.piers.len()
    }
    /// The number of clients we know of.
    ///
    /// [`Self::pier_count`] + 1 (ourself)
    #[must_use]
    #[inline]
    pub fn client_count(&self) -> usize {
        self.pier_count() + 1
    }

    /// Attempts to get the modern name of the resource named `old_name` at `timestamp`.
    ///
    /// If `old_name` is valid, it's returned.
    #[inline]
    pub fn modern_resource_name<'a>(
        &self,
        old_name: &'a str,
        timestamp: SystemTime,
    ) -> Option<&'a str> {
        self.event_log.modern_resource_name(
            old_name,
            timestamp
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap_or(Duration::ZERO),
        )
    }

    /// Get an [`event::Unwinder`] to unwind a resource to `timestamp`.
    #[inline]
    pub fn unwinder_to(&self, timestamp: SystemTime) -> event::Unwinder {
        let events_start = self
            .event_log
            .cutoff_from_time(utils::systime_to_dur(timestamp))
            .unwrap_or(0);
        let events = &self.event_log.list[events_start..];

        event::Unwinder::new(events, Some(self))
    }
    /// Get an [`event::Rewinder`] which can apply all the stored diffs received since the last
    /// data commit to a resource.
    ///
    /// You have to call [`Self::update_last_commit`] after each "merge" - after each occurrence of
    /// diffing.
    ///
    /// This is critical when rewinding the `current` storage before diffing it to the `public`
    /// storage to send an event.
    /// Use [`event::Rewinder::rewind`] on all the modified resources.
    ///
    /// The [`event::Rewinder`] can be reused for several resources.
    // `TODO`: use a list of the events that we haven't yet integrated with the public storage.
    // This would mean only the non committed events would be applied, instead of all after the
    // last commit. Say we got an event with a timestamp before the previous commit. With the
    // current system, it wouldn't be applied.
    #[inline]
    pub fn rewind_from_last_commit(&self) -> event::Rewinder {
        let cutoff = self
            .event_log
            .cutoff_from_time(
                self.event_log
                    .required_event_timestamp
                    .unwrap_or(Duration::ZERO),
            )
            .unwrap_or(0);
        let slice = &self.event_log.list[cutoff..];
        event::Rewinder::new(slice, self)
    }
    /// Get the time of the last call to [`Self::update_last_commit`].
    #[inline]
    pub fn last_commit(&self) -> Option<SystemTime> {
        self.event_log
            .required_event_timestamp
            .map(utils::dur_to_systime)
    }
    /// The [last commit](Self::last_commit) or [`SystemTime::UNIX_EPOCH`].
    #[inline]
    pub fn last_commit_or_epoch(&self) -> SystemTime {
        self.last_commit().unwrap_or(SystemTime::UNIX_EPOCH)
    }
    /// Update the inner timestamp of the last commit.
    ///
    /// See [`Self::rewind_from_last_commit`] for more details.
    #[inline]
    pub fn update_last_commit(&mut self) {
        self.event_log.required_event_timestamp = Some(utils::dur_now());
    }
    /// Set the time of the last commit. Should be used with care in rare circumstances
    /// (e.g. when fast forwarding).
    #[inline]
    pub fn set_last_commit(&mut self, timestamp: SystemTime) {
        self.event_log.required_event_timestamp = Some(utils::systime_to_dur(timestamp));
    }

    /// Returns true if we are in a fast forward. You shouldn't commit under these circumstances.
    #[must_use]
    #[inline]
    pub fn is_fast_forwarding(&self) -> bool {
        self.fast_forward != fast_forward::State::NotRunning
    }
    /// The timestamp of the latest change to `resource`.
    ///
    /// Returns [`SystemTime::UNIX_EPOCH`] if no changes to `resource` are in the log, or if the resource
    /// doesn't exist.
    #[inline]
    pub fn last_change_to_resource(&self, resource: &str) -> SystemTime {
        self.event_log
            .list
            .iter()
            .rev()
            .find(|ev| ev.event.resource() == resource)
            .map_or(SystemTime::UNIX_EPOCH, |ev| ev.event.timestamp())
    }

    /// Get the pier to use when starting a hash check.
    /// This should only be called if our [`Capabilities::persistent`] is set.
    /// If we're a temporary client (read: not server), this isn't needed.
    ///
    /// Call [`Manager::process_hash_check`] with the returned [`SelectedPier`].
    /// It is [`None`] if no other piers (which have not returned [`MessageKind::Cancelled`])
    /// are in the network. If that's the case, do nothing.
    #[inline]
    pub fn hash_check_persistent(&mut self) -> Option<SelectedPier> {
        self.clean_cancelled_piers();
        self.choose_pier(|uuid, capabilities| {
            capabilities.persistent() && !self.cancelled_piers.contains(&uuid)
        })
    }
    #[inline]
    fn clean_cancelled_piers(&mut self) {
        if utils::dur_now().saturating_sub(self.cancelled_piers_last_changed)
            > Duration::from_secs(60).saturating_sub(
                // if someone spams us with cancelled piers, decrease this to avoid
                // `cancelled_piers_last_changed` being pegged and us not getting to sync
                Duration::from_millis(250)
                    * u32::try_from(self.cancelled_piers.len()).unwrap_or(u32::MAX),
            )
        {
            self.cancelled_piers.clear();
        }
    }
    /// Returns the pier we're currently asking to check our resource's hashes against.
    #[must_use]
    #[inline]
    pub fn hash_checking(&self) -> Option<Uuid> {
        self.hash_checking
    }

    /// Get an iterator of the piers filtered by `filter`.
    ///
    /// If you need the number of piers, use [`Self::pier_count`] or [`Self::client_count`].
    pub fn filter_piers<'a>(
        &'a self,
        mut filter: impl FnMut(Uuid, &Capabilities) -> bool + 'a,
    ) -> impl Iterator<Item = (Uuid, &'a Capabilities)> {
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
        mut filter: impl FnMut(Uuid, &Capabilities) -> bool + Clone,
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
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[must_use = "you should probably send some message to this pier"]
pub struct SelectedPier {
    uuid: Uuid,
}
impl SelectedPier {
    pub(crate) fn new(uuid: Uuid) -> Self {
        Self { uuid }
    }
    /// Get the UUID of the pier.
    #[inline]
    pub fn uuid(&self) -> Uuid {
        self.uuid
    }
}

/// The action to perform after calling [`Manager::apply_sync_reply`].
#[derive(Debug)]
#[must_use]
pub enum SyncReplyAction<'a> {
    /// Rewind the public and current storage with `rewinder`.
    ///
    /// You only have to do this to the resources returned by the functions on
    /// [`sync::Response`].
    ///
    /// Call the methods on the [`sync::Response`] to actually make changes to the
    /// resources returned before using the `rewinder`.
    FastForward {
        /// Inner state
        rewinder: event::Rewinder<'a>,
        /// Apply this to your public metadata.
        metadata_applier: fast_forward::MetadataApplier,
    },
    /// Unwind the public and current storage with `unwinder`, then
    /// apply what's returned the methods on the [`sync::Response`]
    /// and then [`event::Unwinder::rewind`].
    ///
    /// You only have to do this to the resources returned by the functions on
    /// [`sync::Response`].
    HashCheck {
        /// Inner state
        unwinder: event::Unwinder<'a>,
    },
    /// An unexpected pier sent us a [`Self::HashCheck`] sync message while we are fast forwarding.
    /// Do nothing and ignore this message. Something's wrong with someone else's code.
    UnexpectedPier,
}
/// The action to follow after calling [`Manager::apply_cancelled`].
#[derive(Debug, Clone, Copy)]
#[must_use]
pub enum CancelAction {
    /// Do nothing.
    Nothing,
    /// Initiate a hash check with the selected pier.
    HashCheck(SelectedPier),
    /// [Trigger](Manager::process_fast_forward) a fast forward.
    FastForward,
}
