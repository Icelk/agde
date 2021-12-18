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
    /* missing_docs */
)]

pub mod diff;
pub mod event;
pub mod hash_check;
pub mod log;
pub mod resource;
pub mod section;
pub mod sync;

use std::borrow::Cow;
use std::cmp;
use std::collections::{BTreeMap, HashMap};
use std::ops::DerefMut;
use std::sync::Mutex;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use rand::Rng;
use serde::{Deserialize, Serialize};

pub use event::{Dataful as DatafulEvent, Event, Into as IntoEvent, Kind as EventKind};
pub use log::{UuidCheck, UuidCheckAction};
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
        check: UuidCheck,
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
        check: UuidCheck,
    },
    /// The target client cancelled the request.
    ///
    /// This may be the result of too many requests.
    /// Should not be sent as a signal of not supporting the feature.
    ///
    /// If a pier requests a check from all and we've reached our limit, don't send this.
    Canceled(Uuid),
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

    /// Get the specific recipient if the [`MessageKind`] is targeted.
    ///
    /// Can be used to determine to send this message to only one pier or all.
    #[inline]
    #[must_use]
    pub fn recipient(&self) -> Option<Uuid> {
        Some(match self.inner() {
            MessageKind::Sync(sync) => sync.recipient(),
            MessageKind::SyncReply(sync) => sync.recipient(),
            MessageKind::HashCheck(request) => request.recipient(),
            MessageKind::HashCheckReply(response) => response.recipient(),
            MessageKind::Canceled(uuid) => *uuid,
            _ => return None,
        })
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
// `TODO`: Merge logs
#[derive(Debug)]
#[must_use]
pub struct Manager {
    capabilities: Capabilities,
    rng: Mutex<rand::rngs::ThreadRng>,
    piers: HashMap<Uuid, Capabilities>,

    event_log: log::Log,
    event_uuid_conversation_piers: log::UuidReplies,
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
        let mut rng = rand::thread_rng();
        let uuid = Uuid::with_rng(&mut rng);
        let capabilities = Capabilities::new(uuid, persistent, help_desire);

        Self {
            capabilities,
            rng: Mutex::new(rng),
            piers: HashMap::new(),

            event_log: log::Log::new(log_lifetime, event_log_limit),
            event_uuid_conversation_piers: log::UuidReplies::new(),
        }
    }
    /// Gets the UUID of this client.
    pub fn uuid(&self) -> Uuid {
        self.capabilities.uuid()
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
        let event = event.into_ev(self);

        let uuid = self.generate_uuid();

        self.event_log.insert(&event, uuid);

        Message::new(MessageKind::Event(event), self.uuid(), uuid)
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
        let (pos, cutoff_timestamp) = self.event_log.appropriate_cutoff()?;
        // this should NEVER not fit inside an u32 as the limit is an u32.
        #[allow(clippy::cast_possible_truncation)]
        let possible_count = (self.event_log.len() - 1 - pos) as u32;
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

        Some(self.process(MessageKind::EventUuidLogCheck { uuid, check }))
    }
    pub fn process_hash_check(&mut self, pier: SelectedPier) -> Message {
        self.process(MessageKind::HashCheck(hash_check::Request::new(
            pier,
            resource::Matcher::all(),
            cmp::min(
                event::dur_now().saturating_sub(Duration::from_secs(15)),
                self.event_log.lifetime() / 2,
            ),
        )))
    }
    pub fn process_hash_check_reply(&mut self, response: hash_check::Response) -> Message {
        self.process(MessageKind::HashCheckReply(response))
    }
    /// You MUST pause the [`Self::apply_event`] between when the `signature` is created for
    /// [`sync::RequestBuilder::finish`] and when the [`sync::Response`] is applied.
    ///
    /// When you receive the [`sync::Response`], call the appropriate functions on it to apply data
    /// and [`Self::apply_sync_reply`].
    pub fn process_sync(&mut self, request: sync::Request) -> Message {
        self.process(MessageKind::Sync(request))
    }
    pub fn process_sync_reply(&mut self, response: sync::ResponseBuilder) -> Message {
        self.process(MessageKind::SyncReply(response.finish(&self.event_log)))
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
        let now = event::dur_now();
        // The timestamp is after now!
        if event.timestamp().saturating_sub(Duration::new(10, 0)) >= now {
            return Err(log::Error::EventInFuture);
        }

        self.event_log.insert(event, message_uuid);
        Ok(self.event_log.event_applier(event, message_uuid))
    }
    /// Handles a [`MessageKind::EventUuidLogCheck`].
    /// This will return an [`UuidCheckAction`] which tells you what to do.
    ///
    /// # Memory leaks
    ///
    /// You must call [`Self::assure_event_uuid_log`] after calling this.
    pub fn apply_event_uuid_log_check(
        &mut self,
        check: UuidCheck,
        conversation_uuid: Uuid,
        remote_uuid: Uuid,
    ) -> UuidCheckAction {
        fn new_cutoff(log: &log::Log, cutoff: Duration, count: u32) -> UuidCheckAction {
            let pos = if let Some(pos) = log.cutoff_from_time(cutoff) {
                pos
            } else {
                return UuidCheckAction::Nothing;
            };
            match log.get_uuid_hash(count, pos, cutoff) {
                Ok(check) => UuidCheckAction::SendAndFurtherCheck(check),
                Err(log::UuidError::CountTooBig) => UuidCheckAction::Nothing,
                Err(log::UuidError::CutoffMissing) => {
                    unreachable!("we got the cutoff above, this must exist in the log.")
                }
            }
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
                Ok(check) => UuidCheckAction::Send(check),
                Err(err) => match err {
                    // We don't have a large enough log. Ignore.
                    // See comment in [`Self::process_event_uuid_log_check`].
                    log::UuidError::CountTooBig => UuidCheckAction::Nothing,
                    // We don't have the UUID of the cutoff!
                    log::UuidError::CutoffMissing => {
                        new_cutoff(&self.event_log, check.cutoff_timestamp(), check.count())
                    }
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
    /// Use the [`event::Unwinder`] before inserting any hashes.
    ///
    /// For each resource which [`hash_check::Request::matches`], execute
    /// [`hash_check::ResponseBuilder::insert`]. When all are inserted, run
    /// [`hash_check::ResponseBuilder::finish`].
    ///
    /// `sender` is the UUID of the pier who sent this, [`Message::sender`].
    #[allow(clippy::unused_self)] // Consistency between functions.
    pub fn apply_hash_check(
        &mut self,
        check: &hash_check::Request,
        sender: Uuid,
    ) -> (hash_check::ResponseBuilder, event::Unwinder) {
        let cutoff = cmp::min(check.cutoff_offset(), self.event_log.lifetime() / 2);

        let unwinder = self
            .event_log
            .unwind_to(event::dur_now().saturating_sub(cutoff));
        (
            hash_check::ResponseBuilder::new(sender, check, cutoff),
            unwinder,
        )
    }
    /// If the returned [`sync::RequestBuilder`] is [`Some`], loop over each resource,
    /// call [`sync::RequestBuilder::matches`] to check if the resource should be included.
    /// Run [`sync::RequestBuilder::finish`] once every [`den::Signature`] has been added.
    /// Then, execute [`Self::process_sync`] with the [`sync::RequestBuilder`].
    /// If it's [`None`], the data matches.
    ///
    /// Delete all the resources in the returned [`Vec`] of [`String`].
    /// Even if this doesn't return a [`sync::RequestBuilder`].
    #[allow(clippy::unused_self)] // method consistency
    pub fn apply_hash_check_reply(
        &mut self,
        response: &hash_check::Response,
        sender: Uuid,
        our_hashes: &hash_check::Response,
    ) -> (Option<sync::RequestBuilder>, Vec<String>) {
        fn btreemap_difference(
            matches: &mut Vec<resource::Matches>,
            mut to_delete: Option<&mut Vec<String>>,
            source: &BTreeMap<String, hash_check::ResponseHash>,
            other: &BTreeMap<String, hash_check::ResponseHash>,
        ) {
            fn has_exact(matches: &[resource::Matches], resource: &str) -> bool {
                for item in matches {
                    if let resource::Matches::Exact(item_exact) = item {
                        if item_exact == resource {
                            return true;
                        }
                    }
                }
                false
            }
            for (resource, hash) in source.iter() {
                if let Some(other_hash) = other.get(resource) {
                    if hash != other_hash && !has_exact(matches, resource) {
                        matches.push(resource::Matches::Exact(resource.clone()));
                    }
                    // else, we have the same hash. Good.
                } else {
                    // the other doesn't have the item
                    if let Some(delete) = &mut to_delete {
                        delete.push(resource.clone());
                    } else {
                        matches.push(resource::Matches::Exact(resource.clone()));
                    }
                }
            }
        }
        let mut differing_data = vec![];
        let mut delete = vec![];
        // See which items in response we don't have.
        btreemap_difference(
            &mut differing_data,
            None,
            response.hashes(),
            our_hashes.hashes(),
        );
        // See which items the response doesn't have.
        btreemap_difference(
            &mut differing_data,
            Some(&mut delete),
            our_hashes.hashes(),
            response.hashes(),
        );

        let request = if differing_data.is_empty() {
            None
        } else {
            Some(sync::RequestBuilder::new(
                sender,
                resource::Matcher::new().set_include(resource::Matches::List(differing_data)),
                // Get all events in the log, up to `self.event_log.limit()`
                Duration::ZERO,
                self.event_log.limit(),
            ))
        };
        (request, delete)
    }
    #[allow(clippy::unused_self)] // method consistency
    pub fn apply_sync<'a>(
        &mut self,
        request: &'a sync::Request,
        sender: Uuid,
    ) -> sync::ResponseBuilder<'a> {
        sync::ResponseBuilder::new(request, sender)
    }
    pub fn apply_sync_reply(&mut self, response: &mut sync::Response) {
        self.event_log.merge(response.take_event_log().into_iter());
    }
}
impl Manager {
    /// Get the random number generator of this manager.
    fn rng(&self) -> impl DerefMut<Target = impl rand::Rng> + '_ {
        self.rng.lock().unwrap()
    }
    /// Generates a UUID using the internal [`rand::Rng`].
    #[inline]
    pub(crate) fn generate_uuid(&mut self) -> Uuid {
        let mut rng = self.rng();
        Uuid::with_rng(&mut *rng)
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
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SelectedPier {
    uuid: Uuid,
}
impl SelectedPier {
    pub(crate) fn new(uuid: Uuid) -> Self {
        Self { uuid }
    }
    /// Get the UUID of the pier.
    pub fn uuid(&self) -> Uuid {
        self.uuid
    }
}
