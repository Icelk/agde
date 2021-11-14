//! A general decentralized sync library supporting text and binary.

#![deny(
    clippy::pedantic,
    unreachable_pub,
    missing_debug_implementations,
    // missing_docs
)]

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
    /// Don't forget to [`Self::advance`] to set the filled region of `slice`.
    /// If you forget to do this, new data will be overridden at the start.
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
        assert!(
            self.len <= self.slice.len(),
            "Tried to set filled to {} while length is {}",
            n,
            self.slice.len()
        );
    }
    /// Advances the size of the filled region of the buffer.
    ///
    /// # Panics
    ///
    /// Panics if `self.len() + n > self.capacity()`.
    pub fn advance(&mut self, n: usize) {
        self.set_filled(self.len + n);
    }
    /// Size of the filled region of the buffer.
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
    /// The function called must not be called on the current event.
    InvalidEvent,
}

/// [`Section::end`] must always be after [`Section::start`].
pub trait Section {
    /// The start of the section to replace in the resource.
    fn start(&self) -> usize;
    /// The end of the section to replace in the resource.
    fn end(&self) -> usize;
    /// Length of new data to fill between [`Section::start`] and [`Section::end`].
    fn new_len(&self) -> usize;
    /// Length of the data between [`Section::start`] and [`Section::end`].
    ///
    /// # Panics
    ///
    /// Panics if the end is before the start.
    /// This guarantee should be upheld by the implementer of [`Section`].
    fn old_len(&self) -> usize {
        self.end() - self.start()
    }
    /// Difference between old and new length of resource.
    ///
    /// # Panics
    ///
    /// Panics if the end is before the start.
    /// This guarantee should be upheld by the implementer of [`Section`].
    ///
    /// Will also panic if any of the lengths don't fit in a [`isize`].
    fn len_difference(&self) -> isize {
        isize::try_from(self.new_len()).expect("length too large for isize.")
            - isize::try_from(self.old_len()).expect("length too large for isize.")
    }
    fn is_empty(&self) -> bool {
        self.new_len() == 0
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
        fn add_iusize(a: usize, b: isize) -> Result<usize, ()> {
            // We've checked that with the if else.
            #[allow(clippy::cast_sign_loss)]
            let result = if b < 0 {
                let b = (-b) as usize;
                if b > a {
                    return Err(());
                }
                a - b
            } else {
                a + (b as usize)
            };
            Ok(result)
        }

        let new_size = add_iusize(resource.filled(), self.len_difference())
            .map_err(|()| ApplyError::BufTooSmall)?;
        let new_data_pos = add_iusize(self.start(), self.len_difference())
            .map_err(|()| ApplyError::BufTooSmall)?;
        if new_size > resource.capacity() || self.end() > resource.capacity() {
            return Err(ApplyError::BufTooSmall);
        }
        // Copy the old data that's in the way of the new.
        // SAFETY: We guarantee above that we can offset the bytes by `self.len_difference()`.
        unsafe {
            std::ptr::copy(
                &resource.slice[self.start()],
                &mut resource.slice[new_data_pos],
                resource.filled().saturating_sub(self.start()),
            );
        }

        // Copy data from `Section` to `resource`.
        // SAFETY: The write is guaranteed to have the space left, see `unsafe` block above.
        // They will never overlap, as the [`SliceBuf`] contains a mutable reference to the bytes,
        // they are exclusive.
        unsafe {
            std::ptr::copy_nonoverlapping(
                &self.data()[0],
                &mut resource.slice[self.start()],
                self.new_len(),
            );
        }
        resource.set_filled(new_size);
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
    fn new_len(&self) -> usize {
        (*self).new_len()
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
            len: section.new_len(),
        }
    }
    /// The reverse of [`DataSection::apply`].
    /// Puts the data gathered from undoing the `apply` in a [`VecSection`].
    ///
    /// # Errors
    ///
    /// Returns an error if the new data cannot fit in `resource`.
    pub(crate) fn revert(&self, resource: &mut SliceBuf) -> Result<VecSection, ApplyError> {
        fn add_iusize(a: usize, b: isize) -> Result<usize, ()> {
            // We've checked that with the if else.
            #[allow(clippy::cast_sign_loss)]
            let result = if b < 0 {
                let b = (-b) as usize;
                if b > a {
                    return Err(());
                }
                a - b
            } else {
                a + (b as usize)
            };
            Ok(result)
        }
        let new_size = add_iusize(resource.filled(), -self.len_difference())
            .map_err(|()| ApplyError::BufTooSmall)?;
        let new_data_pos = add_iusize(self.start(), self.len_difference())
            .map_err(|()| ApplyError::BufTooSmall)?;
        if new_size > resource.capacity() {
            return Err(ApplyError::BufTooSmall);
        }

        let mut section = VecSection::new(self.start(), self.end(), vec![0; self.new_len()]);

        // `TODO`: safety notes

        // Copy data from `resource` to `section`.
        unsafe {
            std::ptr::copy_nonoverlapping(
                &resource.slice[self.start()],
                &mut section.data[0],
                self.new_len(),
            );
            section.data.set_len(section.data().len());
        }
        // Copy the "old" data back in place of the new.
        unsafe {
            std::ptr::copy(
                &resource.slice[new_data_pos],
                &mut resource.slice[self.start()],
                resource.filled().saturating_sub(self.start()),
            );
        }

        // If we added bytes here,
        if self.len_difference() < 0 {
            // We've checked that with the if statement above.
            #[allow(clippy::cast_sign_loss)]
            let to_fill = (0 - self.len_difference()) as usize;
            unsafe {
                std::ptr::write_bytes(&mut resource.slice[new_data_pos], 0, to_fill);
            }
        }

        resource.set_filled(new_size);
        Ok(section)
    }
}
impl Section for EmptySection {
    fn start(&self) -> usize {
        self.start
    }
    fn end(&self) -> usize {
        self.end
    }
    fn new_len(&self) -> usize {
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
    fn new_len(&self) -> usize {
        self.data.len()
    }
}
impl DataSection for VecSection {
    fn data(&self) -> &[u8] {
        &self.data
    }
}

/// A modification to a resource.
///
/// The resource must be initialised using [`CreateEvent`].
#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
#[must_use]
pub struct ModifyEvent<S> {
    resource: String,
    section: S,
}
impl<S: DataSection> ModifyEvent<S> {
    /// Calculates the diff if `source` is [`Some`].
    /// 
    /// # Panics
    ///
    /// `TODO`: implement diff
    pub fn new(resource: String, section: S, source: Option<&[u8]>) -> Self {
        if source.is_some() {
            unimplemented!("Implement diff");
        }
        Self { resource, section }
    }
}
impl<S: Section> ModifyEvent<S> {
    pub fn section(&self) -> &S {
        &self.section
    }
}
impl<S> ModifyEvent<S> {
    pub fn resource(&self) -> &str {
        &self.resource
    }
}
/// A deletion of a resource.
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
    pub fn new(resource: String, successor: Option<String>) -> Self {
        Self {
            resource,
            successor,
        }
    }

    #[must_use]
    pub fn resource(&self) -> &str {
        &self.resource
    }
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
    pub fn new(resource: String) -> Self {
        Self { resource }
    }

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
        impl<'a, T> From<$type> for Event<T> {
            fn from(event: $type) -> Self {
                Self::new(event.into())
            }
        }
    };
}

event_kind_impl!(ModifyEvent<T>, Modify);
event_kind_impl!(CreateEvent, Create);
event_kind_impl!(DeleteEvent, Delete);
impl<T: Into<Event<VecSection>>> From<T> for EventMessage {
    fn from(ev: T) -> Self {
        EventMessage::new(vec![ev.into()])
    }
}

/// The kind of change of data.
#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
#[must_use]
pub enum EventKind<S> {
    /// Modification.
    ///
    /// If the [`ModifyEvent::section()`] start **and** end is `0`, this is considered a file
    /// creation. See [`Self::Move`] on how this affects the move operation.
    Modify(ModifyEvent<S>),
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
}
impl<S> Event<S> {
    pub fn new(kind: EventKind<S>) -> Self {
        Self { kind }
    }
    pub fn resource(&self) -> &str {
        match &self.kind {
            EventKind::Modify(ev) => ev.resource(),
            EventKind::Create(ev) => ev.resource(),
            EventKind::Delete(ev) => ev.resource(),
        }
    }
    pub fn inner(&self) -> &EventKind<S> {
        &self.kind
    }
    pub(crate) fn inner_mut(&mut self) -> &mut EventKind<S> {
        &mut self.kind
    }
}
/// Clones the `resource` [`String`].
impl<S: Section> From<&Event<S>> for Event<EmptySection> {
    fn from(ev: &Event<S>) -> Self {
        let kind = match ev.inner() {
            EventKind::Modify(ev) => EventKind::Modify(ModifyEvent {
                resource: ev.resource().into(),
                section: EmptySection::new(&ev.section()),
            }),
            EventKind::Create(ev) => EventKind::Create(ev.clone()),
            EventKind::Delete(ev) => EventKind::Delete(ev.clone()),
        };
        Event { kind }
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
    ///
    /// **NOTE**: Be very careful with this. `timestamp` MUST be within a second of real time,
    /// else the sync will risk wrong results, forcing [`Message::HashCheck`].
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
// `TODO`: implement the rest of these.
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
    #[allow(clippy::missing_panics_doc)]
    #[must_use]
    pub fn bin(self) -> Vec<u8> {
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
    pub fn base64(self) -> String {
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
    ///
    /// Be careful to have a [`EventKind::Create`] before the resource receives a [`EventKind::Modify`].
    #[inline]
    pub fn process_event(&mut self, event: EventMessage) -> Message {
        Message::new(MessageKind::Event(event), self.uuid, self.generate_uuid())
    }
    /// `pos_in_event_message` MUST be the true value. Else, the ordering will fail
    ///
    /// # Errors
    ///
    /// Fails if the `event` is more than 10s from the future.
    ///
    /// This prevents other clients from hogging our memory with items which never expire.
    /// If their clock is more than 10s off relative to our, we have a serious problem!
    // `TODO`: Make this not take `pos_in_event_message` as that's annoying and a source of
    // troubles for the implementers.
    pub fn apply_event<'a>(
        &'a mut self,
        event: &'a DatafulEvent,
        timestamp: Duration,
        message_uuid: Uuid,
        pos_in_event_message: usize,
    ) -> Result<log::EventApplier<'a, VecSection>, log::Error> {
        self.event_log
            .insert(event, timestamp, message_uuid, pos_in_event_message)?;
        Ok(self
            .event_log
            .event_applier(event, message_uuid, pos_in_event_message))
    }
}
impl Default for Manager {
    fn default() -> Self {
        Self::new(Duration::new(60, 0), 200)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn send_diff() {
        let (message_bin, message_base64, sender_uuid): (Vec<u8>, String, Uuid) = {
            let mut manager = Manager::default();

            let event: Event<_> = ModifyEvent::new(
                "test.txt".into(),
                VecSection::new(0, 0, b"Some test data.".to_vec()),
                None,
            )
            .into();

            let message = manager.process_event(event.into());

            (message.clone().bin(), message.base64(), manager.uuid())
        };

        // The message are sent to a different client.

        // receive message...
        let message = Message::from_bin(&message_bin).unwrap();
        let message_base64 = Message::from_base64(&message_base64).unwrap();
        assert_eq!(message, message_base64);
        assert_eq!(message.sender(), sender_uuid);

        let mut receiver = Manager::default();

        match message.inner() {
            MessageKind::Event(ev_msg) => {
                let mut events = ev_msg.event_iter();

                // assert the only event is to modify `test.txt` with the same section as before.
                assert_eq!(
                    events.next().map(Event::inner),
                    Some(&EventKind::Modify(ModifyEvent::new(
                        "test.txt".into(),
                        VecSection::new(0, 0, b"Some test data.".to_vec()),
                        None
                    )))
                );
                assert_eq!(events.next(), None);

                for (pos, event) in ev_msg.event_iter().enumerate() {
                    let event_applier = receiver
                        .apply_event(event, ev_msg.timestamp, message.uuid(), pos)
                        .expect("Got event from future.");
                    match event_applier.event().inner() {
                        EventKind::Modify(ev) => {
                            assert_eq!(event_applier.resource(), Some("test.txt"));

                            let mut test = Vec::new();
                            let additional = ev.section().len_difference() + 1;
                            // `as usize` should not normally be used!
                            test.resize(additional as usize, 0);

                            let mut resource = SliceBuf::new(&mut test);
                            resource.advance(0);

                            event_applier
                                .apply(&mut resource)
                                .expect("Buffer too small!");
                        }
                        _ => panic!("Wrong EventKind"),
                    }
                }
            }
            kind => {
                panic!("Got {:?}, but expected a Event!", kind);
            }
        }
    }

    #[test]
    fn rework_history() {
        fn process_message(
            manager: &mut Manager,
            message: &Message,
            resource: &mut Vec<u8>,
            resource_len: &mut usize,
            check_too_small: bool,
        ) {
            match message.inner() {
                MessageKind::Event(ev_msg) => {
                    for (pos, event) in ev_msg.event_iter().enumerate() {
                        let event_applier = manager
                            .apply_event(event, ev_msg.timestamp, message.uuid(), pos)
                            .expect("Got event from future.");
                        match event_applier.event().inner() {
                            EventKind::Modify(ev) => {
                                assert_eq!(ev.resource(), "private/secret.txt");

                                let additional = std::cmp::max(
                                    ev.section().len_difference() + 1,
                                    ev.section.new_len() as isize,
                                );
                                // `as usize` should not normally be used!
                                // `TODO`: implement a `section.additional()` method to get how
                                // many bytes to extend the buffer with.
                                resource.resize(resource.len() + additional as usize, 32);

                                if check_too_small {
                                    let mut buf = SliceBuf::new(resource);
                                    buf.set_filled(*resource_len);

                                    event_applier
                                        .apply(&mut buf)
                                        .expect_err("Buffer should be too small!");
                                }

                                // Redo with larger buffer.

                                resource.resize(1024, 32);

                                let mut buf = SliceBuf::new(resource);
                                buf.set_filled(*resource_len);

                                event_applier.apply(&mut buf).expect("Buffer too small!");

                                *resource_len = buf.filled();
                            }
                            _ => panic!("Wrong EventKind"),
                        }
                    }
                }
                kind => {
                    panic!("Got {:?}, but expected a Event!", kind);
                }
            }
        }
        let (first_message, second_message) = {
            let mut sender = Manager::default();

            let first_event = ModifyEvent::new(
                "private/secret.txt".into(),
                VecSection::new(0, 0, "Hello world!".into()),
                None,
            )
            .into();
            let second_event = ModifyEvent::new(
                "private/secret.txt".into(),
                VecSection::new(6, 11, "friend".into()),
                None,
            )
            .into();
            (
                sender.process_event(EventMessage::with_timestamp(
                    vec![first_event],
                    SystemTime::now() - Duration::from_secs(10),
                )),
                sender.process_event(EventMessage::with_timestamp(
                    vec![second_event],
                    SystemTime::now(),
                )),
            )
        };

        let mut receiver = Manager::default();

        let mut resource = Vec::new();
        let mut resource_len = 0;

        // Process second message first.
        process_message(
            &mut receiver,
            &second_message,
            &mut resource,
            &mut resource_len,
            true,
        );

        // We've inserted after the limit. Extend the len.
        resource_len = 12;

        assert_eq!(&resource[..resource_len], b"      friend");

        resource_len = 1;

        // Now, process first message.
        process_message(
            &mut receiver,
            &first_message,
            &mut resource,
            &mut resource_len,
            false,
        );

        assert_eq!(&resource[..resource_len], b"Hello friend!");
        // blocking on `TODO` about ev.section().additional()
        // assert_eq!(resource_len, resource.len());
    }

    // Test doing this ↑ but simplifying as it had previous data, stored how?

    // Test when the underlying data has changed without events; then this library is called again.
    // A special call to the library, which will request all the files, mtime & size to see which
    // have changed.
}
