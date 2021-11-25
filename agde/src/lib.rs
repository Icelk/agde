//! A general decentralized sync library supporting text and binary.

#![deny(
    clippy::pedantic,
    unreachable_pub,
    missing_debug_implementations,
    missing_docs
)]

pub mod diff;
pub mod log;

use std::borrow::Cow;
use std::collections::HashMap;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

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
    /// The client is striving to be persistent. These will regularly do [`Message::HashCheck`].
    #[must_use]
    pub fn persistent(&self) -> bool {
        self.persistent
    }
    /// The desire to communicate with others in the system.
    ///
    /// This value will go from [`i16::MIN`], which means "I want none o' this" to
    /// [`i16::MAX`] which means the client is happy to help others.
    /// This will usually be set in accordance to the free processing power a client has to offer.
    ///
    /// This will play part in deciding which client to send check requests to.
    /// Normally, the always-on clients (those started alongside the server, if there is any),
    /// will have a high value.
    #[must_use]
    pub fn help_desire(&self) -> i16 {
        self.help_desire
    }
}

/// A buffer containing a byte slice and a length of the filled data.
#[derive(Debug)]
#[must_use]
pub struct SliceBuf<'a> {
    slice: &'a mut [u8],
    len: usize,
}
impl<'a> SliceBuf<'a> {
    /// Creates a new buffer with length set to `0`.
    /// Don't forget to [`Self::advance`] to set the filled region of `slice`.
    /// If you forget to do this, new data will be overridden at the start.
    #[allow(clippy::inline_always)]
    #[inline(always)]
    pub fn new(slice: &'a mut [u8]) -> Self {
        Self { slice, len: 0 }
    }
    /// Sets the size of the filled region of the buffer.
    ///
    /// # Panics
    ///
    /// Panics if `n > self.capacity()`.
    #[allow(clippy::inline_always)]
    #[inline(always)]
    pub fn set_filled(&mut self, n: usize) {
        assert!(
            n <= self.slice.len(),
            "Tried to set filled to {} while length is {}",
            n,
            self.slice.len()
        );
        self.len = n;
    }
    /// Advances the size of the filled region of the buffer.
    ///
    /// # Panics
    ///
    /// Panics if `self.len() + n > self.capacity()`.
    #[allow(clippy::inline_always)]
    #[inline(always)]
    pub fn advance(&mut self, n: usize) {
        self.set_filled(self.len + n);
    }
    /// Size of the filled region of the buffer.
    #[must_use]
    #[allow(clippy::inline_always)]
    #[inline(always)]
    pub fn filled(&self) -> usize {
        self.len
    }
    /// Size of the capacity of the buffer. This cannot be increased.
    #[must_use]
    #[allow(clippy::inline_always)]
    #[inline(always)]
    pub fn capacity(&self) -> usize {
        self.slice.len()
    }
}

/// An error during [`DataSection::apply`] and [`EventApplier::apply`].
#[derive(Debug)]
pub enum ApplyError {
    /// [`SliceBuf::capacity`] is too small.
    BufTooSmall,
    /// The function called must not be called on the current event.
    InvalidEvent,
}

/// Adds `b` to `a`. There will be no loss in range.
///
/// # Errors
///
/// Returns an error if `a + b` is negative.
#[allow(clippy::inline_always)]
#[inline(always)]
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
    #[allow(clippy::inline_always)]
    #[inline(always)]
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
    #[allow(clippy::inline_always)]
    #[inline(always)]
    fn len_difference(&self) -> isize {
        isize::try_from(self.new_len()).expect("length too large for isize.")
            - isize::try_from(self.old_len()).expect("length too large for isize.")
    }
    /// The needed length of the [`SliceBuf`].
    /// Should be set to this before calling [`EventApplier::Apply`].
    ///
    /// May return a value lower than `resource_len`. This should not be applied until after the
    /// application.
    fn needed_len(&self, resource_len: usize) -> usize {
        let diff = add_iusize(resource_len, self.len_difference()).unwrap_or(0) + 1;
        let end = self.end() + 1;
        std::cmp::max(diff, end)
    }
    /// Extends the `buffer` with `fill` to fit this section.
    /// `len` is the length of the [`SliceBuf::filled`] part.
    fn apply_len(&self, buffer: &mut Vec<u8>, len: usize, fill: u8) {
        let needed = std::cmp::max(self.needed_len(len), len);
        buffer.resize(needed, fill);
    }
}
/// [`DataSection::end`] must always be after [`DataSection::start`].
pub trait DataSection {
    /// The start of the section to replace in the resource.
    fn start(&self) -> usize;
    /// The end of the section to replace in the resource.
    fn end(&self) -> usize;
    /// Returns a reference to the entirety of the data.
    fn data(&self) -> &[u8];
    /// Applies `self` to `resource`.
    ///
    /// Returns [`SliceBuf::filled`] if successful.
    ///
    /// # Errors
    ///
    /// Returns [`ApplyError::BufTooSmall`] if [`DataSection::data`] cannot fit in `resource`.
    #[allow(clippy::inline_always)]
    #[inline(always)]
    fn apply(&self, resource: &mut SliceBuf) -> Result<usize, ApplyError> {
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
impl<S: DataSection + ?Sized> Section for S {
    #[allow(clippy::inline_always)]
    #[inline(always)]
    fn start(&self) -> usize {
        self.start()
    }
    #[allow(clippy::inline_always)]
    #[inline(always)]
    fn end(&self) -> usize {
        self.end()
    }
    #[allow(clippy::inline_always)]
    #[inline(always)]
    fn new_len(&self) -> usize {
        self.data().len()
    }
}

#[derive(Debug)]
pub(crate) struct EmptySection {
    start: usize,
    end: usize,
    len: usize,
}
impl EmptySection {
    pub(crate) fn new<S: Section>(section: &S) -> Self {
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
    #[allow(clippy::inline_always)]
    #[inline(always)]
    pub(crate) fn revert(&self, resource: &mut SliceBuf) -> Result<VecSection, ApplyError> {
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
    #[allow(clippy::inline_always)]
    #[inline(always)]
    fn start(&self) -> usize {
        self.start
    }
    #[allow(clippy::inline_always)]
    #[inline(always)]
    fn end(&self) -> usize {
        self.end
    }
    #[allow(clippy::inline_always)]
    #[inline(always)]
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
    /// Assembles a new section using `start` as the start of the original data, `end` as the
    /// terminator, and `data` to fill the space between `start` and `end`.
    ///
    /// If `data` is longer than `end-start`, the buffer (that this section will be applied to) is grown. If it's shorter, the buffer will
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
    /// Creates a new section representing the entire resource.
    ///
    /// Useful to use when passing data for [`ModifyEvent::new`] to diff.
    pub fn whole_resource(resource_len: usize, data: Vec<u8>) -> Self {
        Self::new(0, resource_len, data)
    }
}
impl DataSection for VecSection {
    #[allow(clippy::inline_always)]
    #[inline(always)]
    fn start(&self) -> usize {
        self.start
    }
    #[allow(clippy::inline_always)]
    #[inline(always)]
    fn end(&self) -> usize {
        self.end
    }
    #[allow(clippy::inline_always)]
    #[inline(always)]
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
                        if section.old_len() > base.len() {
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
                    section.apply_len(&mut target, filled, 0);
                    let mut buf = SliceBuf::new(&mut target);
                    section
                        .apply(&mut buf)
                        .expect("Buffer is guaranteed to not be too small.");
                    filled = buf.filled();
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
// impl<T: Into<Event<VecSection>>> From<T> for EventMessage {
// fn from(ev: T) -> Self {
// EventMessage::new(vec![ev.into()])
// }
// }

/// The kind of change of data.
#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
#[must_use]
pub enum EventKind<S> {
    /// Modification.
    ///
    /// If the [`ModifyEvent::section()`] start **and** end is `0`, this is considered a file
    /// creation. See [`Self::Move`] on how this affects the move operation.
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
}
impl<S> Event<S> {
    /// Creates a new event from `kind`.
    pub fn new(kind: EventKind<S>) -> Self {
        Self::with_timestamp(kind, SystemTime::now())
    }
    /// Creates a new event from `kind` with the `timestamp`.
    ///
    /// **NOTE**: Be very careful with this. `timestamp` MUST be within a second of real time,
    /// else the sync will risk wrong results, forcing [`Message::HashCheck`].
    pub fn with_timestamp(kind: EventKind<S>, timestamp: SystemTime) -> Self {
        Self {
            kind,
            timestamp: timestamp
                .duration_since(UNIX_EPOCH)
                .unwrap_or(Duration::ZERO),
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
}
/// Clones the `resource` [`String`].
impl<S: Section> From<&Event<S>> for Event<EmptySection> {
    fn from(ev: &Event<S>) -> Self {
        let kind = match ev.inner() {
            EventKind::Modify(ev) => EventKind::Modify(ModifyEvent {
                resource: ev.resource().into(),
                sections: ev.sections().iter().map(EmptySection::new).collect(),
            }),
            EventKind::Create(ev) => EventKind::Create(ev.clone()),
            EventKind::Delete(ev) => EventKind::Delete(ev.clone()),
        };
        Event {
            kind,
            timestamp: ev.timestamp,
        }
    }
}
/// A [`Event`] with internal data.
///
/// This is the type that is sent between clients.
pub type DatafulEvent = Event<VecSection>;

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
    Event(DatafulEvent),
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
/// The `process_*` methods are for creating [`Message`]s.
/// The `apply_*` methods are for accepting and evaluating [`Message`]s.
#[derive(Debug)]
#[must_use]
pub struct Manager {
    capabilities: Capabilities,
    rng: rand::rngs::ThreadRng,
    clients: HashMap<Uuid, Capabilities>,

    event_log: log::EventLog,
    message_uuid_log: log::MessageUuidLog,
}
impl Manager {
    /// Creates a empty manager.
    ///
    /// Call [`Self::process_hello()`] to get a hello message.
    ///
    /// The options are partially explained in [`Capabilities::new`].
    /// I recommend a default of `60s` for `log_lifetime` and `200` for `message_log_limit`.
    pub fn new(
        persistent: bool,
        help_desire: i16,
        log_lifetime: Duration,
        message_log_limit: u32,
    ) -> Self {
        let mut rng = rand::thread_rng();
        let uuid = Uuid::with_rng(&mut rng);
        let capabilities = Capabilities::new(uuid, persistent, help_desire);

        Self {
            capabilities,
            rng,
            clients: HashMap::new(),

            event_log: log::EventLog::new(log_lifetime),
            message_uuid_log: log::MessageUuidLog::new(message_log_limit),
        }
    }
    /// Gets the UUID of this client.
    pub fn uuid(&self) -> Uuid {
        self.capabilities.uuid()
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
    pub fn process_event(&mut self, event: DatafulEvent) -> Message {
        Message::new(MessageKind::Event(event), self.uuid(), self.generate_uuid())
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
        message_uuid: Uuid,
    ) -> Result<log::EventApplier<'a, VecSection>, log::Error> {
        self.event_log.insert(event, message_uuid)?;
        Ok(self.event_log.event_applier(event, message_uuid))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn manager() -> Manager {
        Manager::new(false, 0, Duration::from_secs(60), 200)
    }

    #[test]
    fn send_diff() {
        let (message_bin, message_base64, sender_uuid): (Vec<u8>, String, Uuid) = {
            let mut manager = manager();

            let event: Event<_> = ModifyEvent::new(
                "test.txt".into(),
                vec![VecSection::whole_resource(0, b"Some test data.".to_vec())],
                None,
            )
            .into();

            let message = manager.process_event(event);

            (message.bin(), message.base64(), manager.uuid())
        };

        // The message are sent to a different client.

        // receive message...
        let message = Message::from_bin(&message_bin).unwrap();
        let message_base64 = Message::from_base64(&message_base64).unwrap();
        assert_eq!(message, message_base64);
        assert_eq!(message.sender(), sender_uuid);

        let mut receiver = manager();

        match message.inner() {
            MessageKind::Event(event) => {
                // assert the event is to modify `test.txt` with the same section as before.
                assert_eq!(
                    event.inner(),
                    &EventKind::Modify(ModifyEvent::new(
                        "test.txt".into(),
                        vec![VecSection::whole_resource(0, b"Some test data.".to_vec())],
                        None
                    ))
                );

                let event_applier = receiver
                    .apply_event(event, message.uuid())
                    .expect("Got event from future.");
                match event_applier.event().inner() {
                    EventKind::Modify(ev) => {
                        assert_eq!(event_applier.resource(), Some("test.txt"));

                        let mut test = Vec::new();
                        for section in ev.sections() {
                            section.apply_len(&mut test, 0, b' ');
                        }

                        let mut resource = SliceBuf::new(&mut test);
                        resource.advance(0);

                        event_applier
                            .apply(&mut resource)
                            .expect("Buffer too small!");
                    }
                    _ => panic!("Wrong EventKind"),
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
        ) {
            match message.inner() {
                MessageKind::Event(event) => {
                    let event_applier = manager
                        .apply_event(event, message.uuid())
                        .expect("Got event from future.");
                    match event_applier.event().inner() {
                        EventKind::Modify(ev) => {
                            assert_eq!(ev.resource(), "private/secret.txt");

                            for section in ev.sections() {
                                section.apply_len(resource, *resource_len, b' ');
                            }

                            let mut buf = SliceBuf::new(resource);
                            buf.set_filled(*resource_len);

                            event_applier.apply(&mut buf).expect("Buffer too small!");

                            *resource_len = buf.filled();
                        }
                        _ => panic!("Wrong EventKind"),
                    }
                }
                kind => {
                    panic!("Got {:?}, but expected a Event!", kind);
                }
            }
        }
        let (first_message, second_message) = {
            let mut sender = manager();

            let first_event = ModifyEvent::new(
                "private/secret.txt".into(),
                vec![VecSection::new(0, 0, "Hello world!".into())],
                None,
            )
            .into();
            let second_event = ModifyEvent::new(
                "private/secret.txt".into(),
                vec![VecSection::new(6, 11, "friend".into())],
                None,
            )
            .into();
            (
                sender.process_event(Event::with_timestamp(
                    first_event,
                    SystemTime::now() - Duration::from_secs(10),
                )),
                sender.process_event(Event::with_timestamp(second_event, SystemTime::now())),
            )
        };

        let mut receiver = manager();

        let mut resource = Vec::new();
        let mut resource_len = 0;

        // Process second message first.
        process_message(
            &mut receiver,
            &second_message,
            &mut resource,
            &mut resource_len,
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
        );

        assert_eq!(&resource[..resource_len], b"Hello friend!");
        assert!((resource.len().saturating_sub(2)..resource_len + 2).contains(&resource_len));
    }

    // Test doing this ↑ but simplifying as it had previous data, stored how?

    #[test]
    fn basic_diff() {
        let mut mgr = manager();

        let event: Event<_> = ModifyEvent::new(
            "diff.bin".into(),
            vec![VecSection::whole_resource(0, b"Some test data. This test works, as the whole diff algorithm is written by me!".to_vec())],
            Some(b"Some test data. Hope this test workes, as the whole diff algorithm is written by me!"),
        )
        .into();

        let message = mgr.process_event(event);

        let mut receiver = manager();

        let mut test =
            b"Some test data. Hope this test workes, as the whole diff algorithm is written by me!"
                .to_vec();

        match message.inner() {
            MessageKind::Event(event) => {
                let event_applier = receiver
                    .apply_event(event, message.uuid())
                    .expect("Got event from future.");
                match event_applier.event().inner() {
                    EventKind::Modify(ev) => {
                        for section in ev.sections() {
                            section.apply_len(&mut test, 0, b' ');
                        }

                        let mut resource = SliceBuf::new(&mut test);
                        resource.advance(0);

                        event_applier
                            .apply(&mut resource)
                            .expect("Buffer too small!");
                    }
                    _ => panic!("Wrong EventKind"),
                }
            }
            kind => {
                panic!("Got {:?}, but expected a Event!", kind);
            }
        }
        assert_eq!(
            test,
            b"Some test data. This test works, as the whole diff algorithm is written by me!"
        );
    }

    // Test when the underlying data has changed without events; then this library is called again.
    // A special call to the library, which will request all the files, mtime & size to see which
    // have changed.
}
