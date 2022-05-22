//! Events are a type of message which manipulate a resource.

use std::borrow::Cow;

use den::{Difference, ExtendVec, Signature};

use crate::{log, utils, Deserialize, Duration, EventKind, Manager, Serialize, SystemTime, Uuid};

/// Creates a granular [`Difference`] between `base` and `target`.
///
/// If you [`Difference::apply`] this on `base`, you **should** get `target`.
pub fn diff(base: &[u8], target: &[u8]) -> Difference {
    let mut sig = Signature::new(256);
    sig.write(base);
    let sig = sig.finish();
    let rough_diff = sig.diff(target);
    println!("rough {rough_diff:?}");
    #[allow(clippy::let_and_return)]
    let granular_diff = rough_diff
        .minify(8, base)
        .expect("The way we are using the function, this should never err.");
    println!("minified {granular_diff:?}");
    granular_diff
}

/// A modification to a resource.
///
/// The resource must be initialised using [`Create`].
#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
#[must_use]
pub struct Modify<S: ExtendVec + 'static = Vec<u8>> {
    pub(crate) resource: String,
    pub(crate) diff: Difference<S>,
}
impl Modify {
    /// Get the difference needed to get from `base` to `target`, as a modify event.
    pub fn new(resource: String, target: &[u8], base: &[u8]) -> Self {
        let diff = diff(base, target);

        Self { resource, diff }
    }
    /// Get the difference needed to get from `base` to `target`, as a modify event.
    ///
    /// This also verifies that the [`Modify::diff`] gives `target`.
    /// Using this over [`Self::new`] is only recommended when you have data that
    /// MUST be transmitted correctly.
    ///
    /// Most errors will however resolve once the client reconnects to the network or
    /// if a server, performs a hash check.
    ///
    /// This does not trigger an allocation.
    ///
    /// If the verification failed, we send the whole resource.
    pub fn new_with_verification(resource: String, target: &[u8], base: &[u8]) -> Self {
        let mut difference = diff(base, target);

        if !difference.verify(base, target) {
            difference = diff(b"", target);
            difference.set_original_data_len(base.len());
        }

        Self {
            resource,
            diff: difference,
        }
    }
}
impl<S: ExtendVec + 'static> Modify<S> {
    /// Get a reference to the sections of data this event modifies.
    pub fn diff(&self) -> &Difference<S> {
        &self.diff
    }
    /// Returns a reference to the target resource name.
    #[must_use]
    #[inline]
    pub fn resource(&self) -> &str {
        &self.resource
    }
    /// Set the inner `resource`.
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
        impl<'a> From<$type> for Kind {
            fn from(event: $type) -> Self {
                Self::$enum_name(event)
            }
        }
        impl<'a> IntoEvent for $type {
            fn into_ev(self, manager: &Manager) -> Event {
                Event::new(self.into(), manager)
            }
        }
    };
}

/// Helper trait to convert from `*Event` structs to [`Event`].
///
/// Should not be implemented but used with [`Manager::process_event`].
#[allow(clippy::module_name_repetitions)] // We can't call it `Into`!
pub trait IntoEvent<S: ExtendVec + 'static = Vec<u8>> {
    /// Converts `self` into an [`Event`].
    fn into_ev(self, manager: &Manager) -> Event<S>;
}
impl IntoEvent for Event {
    fn into_ev(self, _manager: &Manager) -> Event {
        self
    }
}

impl<'a, S: ExtendVec + 'static> From<Modify<S>> for Kind<S> {
    fn from(event: Modify<S>) -> Self {
        Self::Modify(event)
    }
}
impl<'a, S: ExtendVec + 'static> IntoEvent<S> for Modify<S> {
    fn into_ev(self, manager: &Manager) -> Event<S> {
        Event::new(self.into(), manager)
    }
}
event_kind_impl!(Create, Create);
event_kind_impl!(Delete, Delete);

/// The kind of change of data.
#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
#[must_use]
pub enum Kind<S: ExtendVec + 'static = Vec<u8>> {
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
impl<S: ExtendVec + 'static> Kind<S> {
    /// Returns a reference to the target resource name.
    #[allow(clippy::inline_always)]
    #[inline(always)]
    #[must_use]
    pub fn resource(&self) -> &str {
        match &self {
            Kind::Modify(ev) => ev.resource(),
            Kind::Create(ev) => ev.resource(),
            Kind::Delete(ev) => ev.resource(),
        }
    }
}
/// A change of data.
#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
#[must_use]
pub struct Event<S: ExtendVec + 'static = Vec<u8>> {
    kind: Kind<S>,
    /// A [`Duration`] of time after UNIX_EPOCH.
    timestamp: Duration,
    /// A [`Duration`] of time after UNIX_EPOCH.
    ///
    /// This gives the receiver information about when to modify later events when this event
    /// arrives. If event 2 (stored in the log) has this set to before event 1's `timestamp`, we
    /// use [`utils::Offsets`] to modify event 2. If this is set to after event 1's `timestamp`, we
    /// do nothing.
    ///
    /// This provides some resistance against simultaneous changes (where the issuer doesn't yet
    /// know about the pier's new diff).
    latest_event: Duration,
    sender: Uuid,
}
impl<S: ExtendVec + 'static> Event<S> {
    /// Creates a new event from `kind`.
    ///
    /// Create an `Event` with the [`Self::timestamp`] set to the current time.
    pub fn new(kind: Kind<S>, sender: &Manager) -> Self {
        let latest_event = sender.event_log.latest_event(kind.resource());
        let latest_event =
            latest_event.map_or_else(|| Duration::ZERO, |ev| ev.event.timestamp_dur());
        Self {
            kind,
            timestamp: utils::systime_to_dur(SystemTime::now()),
            latest_event,
            sender: sender.uuid(),
        }
    }
    /// Override [`Self::timestamp`] with `timestamp`.
    ///
    /// **NOTE**: Be very careful with this. `timestamp` MUST be within a second of real time,
    /// else you risk wrong results from the sync mechanism, forcing [`crate::MessageKind::HashCheck`].
    pub fn with_timestamp(mut self, timestamp: SystemTime) -> Self {
        self.timestamp = utils::systime_to_dur(timestamp);
        self
    }
    /// Returns a reference to the target resource name.
    #[allow(clippy::inline_always)]
    #[inline(always)]
    #[must_use]
    pub fn resource(&self) -> &str {
        self.inner().resource()
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
    pub(crate) fn timestamp_dur(&self) -> Duration {
        self.timestamp
    }
    /// Get the timestamp of this event.
    #[inline]
    #[must_use]
    pub fn timestamp(&self) -> SystemTime {
        utils::dur_to_systime(self.timestamp)
    }
    /// Returns the UUID of the sender of this event.
    #[inline]
    pub fn sender(&self) -> Uuid {
        self.sender
    }
    /// Get the [`Difference`], if [`Self::inner`] is [`Kind::Modify`].
    #[must_use]
    #[inline]
    pub fn diff(&self) -> Option<&Difference<S>> {
        match self.inner() {
            Kind::Modify(ev) => Some(ev.diff()),
            _ => None,
        }
    }
    /// Get the timestamp of the last event observed by the issuer of this event.
    #[inline]
    #[must_use]
    pub fn latest_event_timestamp(&self) -> SystemTime {
        utils::dur_to_systime(self.latest_event)
    }
}

/// Error during [`Unwinder`] operations.
#[derive(Debug)]
pub enum UnwindError {
    /// The resource has previously been destroyed.
    ResourceDestroyed,
    /// An error during an application of a section.
    ///
    /// The rewound resource is also returned, if you want to use it.
    /// The diffs which erred aren't applied.
    Apply(den::ApplyError, Vec<u8>),
}

/// Unwinds the stack of stored events to get the local data to a previous state.
///
/// Has nothing to do with unwinding of the program's stack in a [`panic!`].
#[derive(Debug)]
pub struct Unwinder<'a> {
    /// Ordered from last (temporally).
    events: &'a [log::ReceivedEvent],
    rewound_events: Vec<&'a Difference>,
    // these are allocated once to optimize allocations
    buffer: Vec<u8>,
}
impl<'a> Unwinder<'a> {
    pub(crate) fn new(events: &'a [log::ReceivedEvent]) -> Self {
        Self {
            events,
            rewound_events: vec![],
            buffer: vec![],
        }
    }
    /// # Errors
    ///
    /// Will never return [`UnwindError::Apply`].
    pub(crate) fn check_name(&self, modern_resource_name: &'a str) -> Result<(), UnwindError> {
        for log_event in self.events {
            if log_event.event.resource() == modern_resource_name {
                match &log_event.event.inner() {
                    EventKind::Delete(_) | EventKind::Create(_) => {
                        return Err(UnwindError::ResourceDestroyed)
                    }
                    // Do nothing; the file is just modified.
                    EventKind::Modify(_) => {}
                }
            }
        }
        Ok(())
    }
    /// Get an iterator over the [`Difference`]s to `modern_resource_name`.
    ///
    /// # Errors
    ///
    /// Returns [`UnwindError::ResourceDestroyed`] if `modern_resource_name` was destroyed/created
    /// again.
    pub fn sections<'b>(
        &'b self,
        modern_resource_name: &'b str,
    ) -> Result<impl Iterator<Item = &Difference> + 'b, UnwindError> {
        self.check_name(modern_resource_name)?;

        let iter = self.events.iter().rev().filter_map(move |received_ev| {
            if received_ev.event.resource() != modern_resource_name {
                return None;
            }
            match received_ev.event.inner() {
                EventKind::Modify(ev) => Some(ev.diff()),
                EventKind::Delete(_) | EventKind::Create(_) => unreachable!(
                    "Unexpected delete or create event in unwinding of event log.\
                    Please report this bug."
                ),
            }
        });
        Ok(iter)
    }
    /// Get an iterator over the events stored in this unwinder.
    /// The first item is the oldest one. The last is the most recent.
    ///
    /// Useful it you want to get resources affected since a timestamp:
    ///
    /// ```
    /// # use agde::*;
    /// use std::time::{Duration, SystemTime};
    /// let manager = Manager::new(false, 0, Duration::from_secs(60), 512);
    ///
    /// let unwinder = manager.unwinder_to(SystemTime::now() - Duration::from_secs(2));
    /// for event in unwinder.events() {
    ///     println!("Resource {} changed in some way.", event.resource());
    /// }
    /// ```
    pub fn events(&self) -> impl Iterator<Item = &Event> + DoubleEndedIterator + '_ {
        self.events.iter().map(|received_ev| &received_ev.event)
    }
    /// Reverts the `resource` with `modern_resource_name` to the bottom of the internal list.
    ///
    /// # Panics
    ///
    /// If you called this before and didn't call [`Self::unwind`], this panics.
    ///
    /// # Errors
    ///
    /// Returns [`UnwindError::ResourceDestroyed`] if `modern_resource_name` has been re-created or
    /// destroyed during the timeline of this unwinder.
    pub fn unwind(
        &mut self,
        resource: impl Into<Vec<u8>>,
        modern_resource_name: &'a str,
    ) -> Result<Vec<u8>, UnwindError> {
        assert_eq!(
            self.rewound_events.len(),
            0,
            "The rewinding stack must be empty!"
        );

        self.check_name(modern_resource_name)?;

        let mut error = None;

        let mut resource = resource.into();
        // these are allocated once to optimize allocations
        let mut other = std::mem::take(&mut self.buffer);

        // On move events, only change resource.
        // On delete messages, panic. A bug.
        // ↑ should be contracted by the creator.
        for received_ev in self.events.iter().rev() {
            if received_ev.event.resource() != modern_resource_name {
                continue;
            }
            match received_ev.event.inner() {
                EventKind::Modify(ev) => {
                    let diff = ev.diff();

                    // `TODO`: don't hardcode fill_byte.
                    let ok = ev.diff().in_bounds(&resource);

                    if ok {
                        ev.diff()
                            .revert(&resource, &mut other, b' ')
                            .expect("we made sure the references were in bounds");

                        std::mem::swap(&mut resource, &mut other);
                        other.clear();
                        self.rewound_events.push(diff);
                    } else {
                        error = Some(den::ApplyError::RefOutOfBounds);
                    }
                }
                EventKind::Delete(_) | EventKind::Create(_) => unreachable!(
                    "Unexpected delete or create event in unwinding of event log.\
                    Please report this bug."
                ),
            }
        }
        self.buffer = other;

        if let Some(err) = error {
            Err(UnwindError::Apply(err, resource))
        } else {
            Ok(resource)
        }
    }
    /// Rewinds the `resource` back up.
    ///
    /// # Errors
    ///
    /// Passes errors from [`Difference::apply`].
    pub fn rewind(&mut self, resource: &[u8]) -> Result<Vec<u8>, (den::ApplyError, Vec<u8>)> {
        let mut vec = resource.to_vec();
        let mut other = vec![];
        let mut error = None;
        // Unwind the stack, redoing all the events.
        while let Some(diff) = self.rewound_events.pop() {
            if diff.in_bounds(&vec) {
                if diff.apply_overlaps(vec.len()) {
                    diff.apply(&vec, &mut other)
                        .expect("we made sure the references were in bounds");
                    std::mem::swap(&mut vec, &mut other);
                    other.clear();
                } else {
                    diff.apply_in_place(&mut vec)
                        .expect("we made sure the references were in bounds");
                }
            } else {
                error = Some(den::ApplyError::RefOutOfBounds);
            }
        }
        match error {
            Some(e) => Err((e, vec)),
            None => Ok(vec),
        }
    }
}

#[derive(Debug)]
/// An error when [rewinding](Rewinder::rewind).
pub enum RewindError {
    /// An error occurred when applying a diff.
    ///
    /// The rewound resource is also returned, if you want to use it.
    /// The diffs which erred aren't applied.
    Apply(den::ApplyError, Vec<u8>),
    /// The resource has been destroyed since last commit. Throw away your changes.
    ///
    /// The inner byte array is the `data` supposed to be rewound.
    ResourceDestroyed(Vec<u8>),
}
impl RewindError {
    /// Returns the recovered data.
    #[must_use]
    pub fn into_data(self) -> Vec<u8> {
        match self {
            RewindError::Apply(_e, vec) => vec,
            RewindError::ResourceDestroyed(vec) => vec,
        }
    }
}

/// Struct to apply all the diffs from a specified timestamp to a resource.
///
/// See [`Manager::rewind_from_last_commit`] for more info.
#[derive(Debug)]
pub struct Rewinder<'a> {
    /// Ordered from last (temporally).
    events: &'a [log::ReceivedEvent],
    buf: Vec<u8>,
    manager: &'a Manager,
}
impl<'a> Rewinder<'a> {
    pub(crate) fn new(slice: &'a [log::ReceivedEvent], manager: &'a Manager) -> Self {
        Self {
            events: slice,
            buf: Vec::new(),
            manager,
        }
    }
    /// Rewinds the `resource` back up to the most recent version.
    /// `diff_modification` can be used to change the diff just before it's applied.
    ///
    /// # Errors
    ///
    /// Passes errors from [`Difference::apply`].
    pub fn rewind_with_modify_diff(
        &mut self,
        resource: &str,
        data: impl Into<Vec<u8>>,
        mut diff_modification: impl FnMut(&Difference) -> Cow<'_, Difference>,
    ) -> Result<Vec<u8>, RewindError> {
        let mut vec = data.into();
        let mut other = std::mem::take(&mut self.buf);

        let mut error = None;

        // if event started ev from beginning, don't apply the
        let breaking_idx = self
            .events
            .iter()
            .rposition(|ev| ev.event.resource() == resource && ev.event.diff().is_none());

        if breaking_idx.is_some() {
            return Err(RewindError::ResourceDestroyed(vec));
        }

        // Apply all events.
        for received_event in self.events {
            if received_event.event.resource() != resource {
                continue;
            }
            let diff = if let Some(diff) = received_event.event.diff() {
                diff
            } else {
                continue;
            };
            let diff = diff_modification(diff);
            if diff.in_bounds(&vec) {
                if diff.apply_overlaps_adaptive_end(vec.len()) {
                    diff.apply_adaptive_end(&vec, &mut other)
                        .expect("we made sure the references were in bounds");
                    std::mem::swap(&mut vec, &mut other);

                    other.clear();
                } else {
                    diff.apply_in_place_adaptive_end(&mut vec)
                        .expect("we made sure the references were in bounds");
                }
            } else {
                error = Some(den::ApplyError::RefOutOfBounds);
            }
        }
        self.buf = other;

        if let Some(err) = error {
            Err(RewindError::Apply(err, vec))
        } else {
            Ok(vec)
        }
    }
    /// Rewinds the `resource` back up to the most recent version.
    ///
    /// # Errors
    ///
    /// Passes errors from [`Difference::apply`].
    pub fn rewind(
        &mut self,
        resource: &str,
        data: impl Into<Vec<u8>>,
    ) -> Result<Vec<u8>, RewindError> {
        self.rewind_with_modify_diff(resource, data, |d| Cow::Borrowed(d))
    }

    /// Get an iterator over the events stored in this rewinder.
    /// The first item is the oldest one. The last is the most recent.
    pub fn events(&self) -> impl Iterator<Item = &Event> + DoubleEndedIterator + '_ {
        self.events.iter().map(|received_ev| &received_ev.event)
    }

    /// See [`Manager::last_change_to_resource`].
    #[must_use]
    pub fn last_change_to_resource(&self, resource: &str) -> SystemTime {
        self.manager.last_change_to_resource(resource)
    }
}
