//! Events are a type of message which manipulate a resource.

use crate::{
    diff, section, Cow, DataSection, Deserialize, Duration, IntoEvent, Manager, Section, Serialize,
    SliceBuf, SystemTime, Uuid, VecSection, UNIX_EPOCH,
};

/// A modification to a resource.
///
/// The resource must be initialised using [`Create`].
#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
#[must_use]
pub struct Modify<S> {
    resource: String,
    sections: Vec<S>,
}
impl Modify<VecSection> {
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
impl<S: Section> Modify<S> {
    /// Gets a reference to the sections of data this event modifies.
    #[must_use]
    pub fn sections(&self) -> &[S] {
        &self.sections
    }
}
impl<S> Modify<S> {
    /// Returns a reference to the target resource name.
    #[must_use]
    #[inline]
    pub fn resource(&self) -> &str {
        &self.resource
    }
    /// Sets the inner `resource`.
    #[inline]
    pub(crate) fn set_resource(&mut self, resource: String) {
        self.resource = resource;
    }
}
/// Deletion of a resource.
///
/// The resource must be initialised using [`Create`].
#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
#[must_use]
pub struct Delete {
    resource: String,
    /// An optional successor to the [`Self::resource()`]
    successor: Option<String>,
}
impl Delete {
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
    #[inline]
    pub fn resource(&self) -> &str {
        &self.resource
    }
    /// Returns a reference to the optional successor.
    #[must_use]
    #[inline]
    pub fn successor(&self) -> Option<&str> {
        self.successor.as_deref()
    }

    /// Sets the inner `resource`.
    #[inline]
    pub(crate) fn set_resource(&mut self, resource: String) {
        self.resource = resource;
    }
    /// Returns the successor, if any.
    #[inline]
    pub(crate) fn take_successor(&mut self) -> Option<String> {
        self.successor.take()
    }
}
/// The creation of a resource.
///
/// Creates an empty file. Overrides the file if it already exists.
#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
#[must_use]
pub struct Create {
    resource: String,
}
impl Create {
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
        impl<'a, T> From<$type> for Kind<T> {
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
pub trait Into<SectionKind> {
    /// Converts `self` into an [`Event`].
    fn into_ev(self, manager: &Manager) -> Event<SectionKind>;
}
impl<T> Into<T> for Event<T> {
    fn into_ev(self, _manager: &Manager) -> Event<T> {
        self
    }
}

event_kind_impl!(Modify<T>, Modify);
event_kind_impl!(Create, Create);
event_kind_impl!(Delete, Delete);

/// Returns the time since [`UNIX_EPOCH`].
pub(crate) fn dur_now() -> Duration {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::ZERO)
}

/// The kind of change of data.
#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
#[must_use]
pub enum Kind<S> {
    /// Modification.
    ///
    /// You need to make a [`Self::Create`] event before modifying the resource.
    /// If you don't do this, the modification MUST NOT be applied.
    Modify(Modify<S>),
    /// Creation.
    ///
    /// A new resource has been created. Before any other event can affect this resource,
    /// you'll have to initialise it with this event.
    Create(Create),
    /// Deletion.
    ///
    /// Can contain a [`Delete::successor`] to hint on where the file has been moved to. This
    /// enables subsequent [`Event`]s to be redirected to the successor.
    /// The redirections will stop when a new [`Self::Create`] event is triggered.
    Delete(Delete),
}
/// A change of data.
#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
#[must_use]
pub struct Event<S> {
    kind: Kind<S>,
    /// Duration since UNIX_EPOCH
    timestamp: Duration,
    sender: Uuid,
}
impl<S> Event<S> {
    /// Creates a new event from `kind`.
    pub fn new(kind: Kind<S>, sender: Uuid) -> Self {
        Self::with_timestamp(kind, sender, SystemTime::now())
    }
    /// Creates a new event from `kind` with the `timestamp`.
    ///
    /// **NOTE**: Be very careful with this. `timestamp` MUST be within a second of real time,
    /// else the sync will risk wrong results, forcing [`crate::MessageKind::HashCheck`].
    pub fn with_timestamp(kind: Kind<S>, sender: Uuid, timestamp: SystemTime) -> Self {
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
            Kind::Modify(ev) => ev.resource(),
            Kind::Create(ev) => ev.resource(),
            Kind::Delete(ev) => ev.resource(),
        }
    }
    /// Returns a reference to the inner [`Kind`] where all the event data is stored.
    #[allow(clippy::inline_always)]
    #[inline(always)]
    pub fn inner(&self) -> &Kind<S> {
        &self.kind
    }
    /// Returns a mutable reference to the inner [`Kind`].
    #[inline]
    pub(crate) fn inner_mut(&mut self) -> &mut Kind<S> {
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
            Kind::Modify(ev) => Kind::Modify(Modify {
                resource: ev.resource().into(),
                sections: ev.sections().iter().map(section::Empty::new).collect(),
            }),
            Kind::Create(ev) => Kind::Create(ev.clone()),
            Kind::Delete(ev) => Kind::Delete(ev.clone()),
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
pub type Dataful = Event<VecSection>;
