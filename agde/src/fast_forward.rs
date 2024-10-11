//! Mechanics for reading the current data from other piers after being offline for some time.
//!
//! # Procedure
//!
//! At agde's startup, call [`crate::Manager::process_fast_forward`] and send it.
//! Agde will not apply any events that are received and given to [`crate::Manager::apply_event`].
//! If the answer is [`crate::MessageKind::Cancelled`], try again.
//!
//! Then, call [`crate::Manager::apply_fast_forward_reply`]. This will create a message which
//! fetches the diffs. Send that and wait for the response. Then, call the methods of
//! [`crate::sync::Response`]. Also call [`crate::Manager::apply_sync_reply`].
//!
//! Now, you get back a list of [`crate::log::EventApplier`]s, since we need to apply the events
//! received during the fast forward. Handle all of them.
use crate::{utils, Deserialize, Duration, HashMap, Serialize, SystemTime, Uuid};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum State {
    NotRunning,
    WaitingForMeta {
        pier: Uuid,
    },
    WaitingForDiffs {
        pier: Uuid,
        latest_event: Option<Uuid>,
        fast_forward_metadata: Metadata,
        changes: Vec<MetadataChange>,
    },
}
/// An error while handling fast forwarding.
#[derive(Debug)]
pub enum Error {
    /// The state was unexpected.
    /// There's something wrong with your calls to agde.
    ExpectedNotRunning,
    /// The state was unexpected.
    /// There's something wrong with your calls to agde.
    ExpectedWaitingForMeta,
    /// The state was unexpected.
    /// There's something wrong with your calls to agde.
    ExpectedWaitingForDiffs,
    /// A pier we hadn't asked for help from responded.
    /// Ignore the message.
    UnexpectedPier,
}

/// The metadata of a resource.
#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq, Eq)]
pub struct ResourceMeta {
    /// mtime of the current storage.
    ///
    /// Is [`None`] if this resource has not yet been written to the current storage.
    /// In that case, there's always a change. This can occur when data has been written to the
    /// public storage, and the current storage hasn't gotten that data yet.
    mtime_in_current: Option<Duration>,
    /// The timestamp of the event that last modified this.
    event_mtime: Duration,
    size: u64,
}

impl ResourceMeta {
    /// `size` is the length of the data.
    ///
    /// # `mtime_in_current`
    ///
    /// `mtime_in_current` is the modify time of the resource in the current storage.
    ///
    /// Is [`None`] if this resource has not yet been written to the current storage.
    /// In that case, there's always a change. This can occur when data has been written to the
    /// public storage, and the current storage hasn't gotten that data yet.
    #[must_use]
    pub fn new(mtime_in_current: Option<SystemTime>, size: u64) -> Self {
        Self::new_from_event(mtime_in_current, SystemTime::UNIX_EPOCH, size)
    }
    /// Same as [`Self::new`] but with an additional argument - `event_mtime`.
    ///
    /// It represents the time of the latest event which acted upon the resource.
    /// Only use this function for the public storage, as it's useless elsewhere.
    #[must_use]
    pub fn new_from_event(
        mtime_in_current: Option<SystemTime>,
        event_mtime: SystemTime,
        size: u64,
    ) -> Self {
        Self {
            mtime_in_current: mtime_in_current.map(utils::systime_to_dur),
            event_mtime: utils::systime_to_dur(event_mtime),
            size,
        }
    }
    /// The size of the resource.
    #[must_use]
    pub fn size(&self) -> u64 {
        self.size
    }
    /// Mtime of this resource in the current storage.
    /// [`None`] if this hasn't been written to the curren storage.
    #[must_use]
    pub fn mtime_in_current(&self) -> Option<SystemTime> {
        self.mtime_in_current.map(utils::dur_to_systime)
    }
    /// The timestamp of the last modification to this resource by an event.
    /// Only applicable to the public storage.
    /// [`None`] if this hasn't been written to the curren storage.
    #[must_use]
    pub fn mtime_of_last_event(&self) -> SystemTime {
        utils::dur_to_systime(self.event_mtime)
    }

    /// Update the metadata.
    pub fn set_size(&mut self, size: u64) {
        self.size = size;
    }
    /// Update the metadata.
    pub fn set_mtime_in_current(&mut self, mtime_in_current: Option<SystemTime>) {
        self.mtime_in_current = mtime_in_current.map(utils::systime_to_dur);
    }
}

/// The metadata for all files we're tracking.
///
/// The implementations of agde need to provide methods to create and save this.
///
/// You can call [`Clone::clone_from`] for an optimized cloning method.
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
#[must_use]
pub struct Metadata {
    /// Associate resource to metadata
    map: HashMap<String, ResourceMeta>,
}
impl Metadata {
    /// Create a new metadata map.
    pub fn new(map: HashMap<String, ResourceMeta>) -> Self {
        Self { map }
    }
    /// Get an iterator over all the resources and their respective metadata.
    #[inline]
    pub fn iter(&self) -> impl Iterator<Item = (&str, ResourceMeta)> {
        self.map.iter().map(|(k, v)| (&**k, *v))
    }
    /// Get the metadata for `resource`.
    #[must_use]
    #[inline]
    pub fn get(&self, resource: &str) -> Option<ResourceMeta> {
        self.map.get(resource).copied()
    }
    /// If the metadata contains `resource`.
    #[must_use]
    #[inline]
    pub fn contains(&self, resource: &str) -> bool {
        self.map.contains_key(resource)
    }
    /// Remove the metadata for `resource`.
    #[inline]
    pub fn remove(&mut self, resource: &str) {
        self.map.remove(resource);
    }
    /// Insert the `meta` for `resource`.
    #[inline]
    pub fn insert(&mut self, resource: String, meta: ResourceMeta) {
        self.map.insert(resource, meta);
    }

    /// Calculate the changes to get the metadata from `self` to `target`.
    ///
    /// `ignore_mtime_of_last_event` is used when diffing locally, as we don't care about the
    /// unrelated `mtime_of_last_event` in the current storage.
    #[must_use]
    pub fn changes(&self, target: &Self, ignore_mtime_of_last_event: bool) -> Vec<MetadataChange> {
        let mut changed = Vec::new();
        for (resource, meta) in self.iter() {
            match target.get(resource) {
                Some(target_meta) => {
                    if target_meta.size() != meta.size()
                        || ((meta.mtime_of_last_event() != target_meta.mtime_of_last_event()
                            || ignore_mtime_of_last_event)
                            && meta.mtime_in_current() != target_meta.mtime_in_current())
                    {
                        changed.push(MetadataChange::Modify(
                            resource.to_owned(),
                            false,
                            target_meta.mtime_in_current(),
                        ));
                    }
                }
                None => changed.push(MetadataChange::Delete(resource.to_owned())),
            }
        }
        for (resource, meta) in target.iter() {
            if !self.contains(resource) {
                changed.push(MetadataChange::Modify(
                    resource.to_owned(),
                    true,
                    meta.mtime_in_current(),
                ));
            }
        }
        changed
    }
    /// Apply the changes gathered from [`Self::changes`] to `self`, while fetching data from
    /// `target`.
    ///
    /// This means you can apply the changes between two metadata sets on a third set.
    ///
    /// Also see [`Self::apply_current_mtime_changes`]
    pub fn apply_changes(&mut self, changes: &[MetadataChange], target: &Self) {
        for change in changes {
            match change {
                MetadataChange::Modify(res, _, _) => {
                    if let Some(meta) = target.get(res) {
                        if let Some(mut old) = self.get(res) {
                            // don't reset mtime_in_current.
                            old.size = meta.size;
                            if meta.event_mtime != Duration::ZERO {
                                old.event_mtime = meta.event_mtime;
                            }
                            self.insert(res.clone(), old);
                        } else {
                            self.insert(res.clone(), meta);
                        }
                    } else {
                        self.remove(res);
                    }
                }
                MetadataChange::Delete(res) => {
                    self.remove(res);
                }
            }
        }
    }
    /// Same as [`Self::apply_changes`] but also
    /// apply the [`ResourceMeta::mtime_in_current`] to get `self` to `target`.
    ///
    /// This is here to update the mtime of resources which did not get their mtime updated due to
    /// the diff being [empty](crate::dach::Difference::is_empty).
    pub fn apply_current_mtime_changes(&mut self, changes: &[MetadataChange]) {
        for change in changes {
            match change {
                MetadataChange::Modify(res, _, mtime) => {
                    if let Some(meta) = self.map.get_mut(res) {
                        meta.mtime_in_current = mtime.map(utils::systime_to_dur);
                    } else {
                        self.remove(res);
                    }
                }
                MetadataChange::Delete(res) => {
                    self.remove(res);
                }
            }
        }
    }
}
impl Clone for Metadata {
    #[inline]
    fn clone(&self) -> Self {
        Self {
            map: self.map.clone(),
        }
    }
    #[inline]
    fn clone_from(&mut self, source: &Self) {
        self.map.clear();
        self.map
            .extend(source.map.iter().map(|(k, v)| (k.clone(), *v)));
    }
}

/// A single change between sets of metadata.
#[derive(Debug, PartialEq, Eq, Clone)]
pub enum MetadataChange {
    /// True if the resource was just created.
    ///
    /// The [`SystemTime`] is the modify time of the target of the difference.
    /// It represents the [`ResourceMeta::mtime_in_current`].
    ///
    /// The [`SystemTime`] is here to update the mtime of resources which did not get their mtime updated due to
    /// the diff being [empty](crate::dach::Difference::is_empty).
    Modify(String, bool, Option<SystemTime>),
    /// The resource was destroyed.
    Delete(String),
}
impl MetadataChange {
    /// The resource of the change.
    #[must_use]
    #[inline]
    pub fn resource(&self) -> &str {
        match self {
            Self::Modify(r, _, _) | Self::Delete(r) => r,
        }
    }
}

/// A request to fast forward the local data.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Request {
    pub(crate) pier: Uuid,
}
impl Request {
    pub(crate) fn new(pier: Uuid) -> Self {
        Self { pier }
    }

    /// Get the pier this is targeted to.
    pub fn recipient(&self) -> Uuid {
        self.pier
    }
}
/// A fast forward response. This contains the data to bring us up and running again.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Response {
    pub(crate) pier: Uuid,
    pub(crate) metadata: Metadata,
    /// [`None`] if our event log is empty.
    /// The UUID of the latest event message stored in the log.
    pub(crate) current_event_uuid: Option<Uuid>,
}
impl Response {
    pub(crate) fn new(pier: Uuid, metadata: Metadata, current_event_uuid: Option<Uuid>) -> Self {
        Self {
            pier,
            metadata,
            current_event_uuid,
        }
    }

    /// Get the metadata of the remote. Use [`Metadata::changes`] to list which you need to add to
    /// the [`crate::sync::RequestBuilder`].
    ///
    /// Use like this: `local_public_metadata.changes(ff_response.metadata())`.
    /// Remove the files the changes says needs to be removed and add the rest to the
    /// [`crate::sync::RequestBuilder::insert`].
    pub fn metadata(&self) -> &Metadata {
        &self.metadata
    }

    /// Get the recipient of this response.
    pub fn recipient(&self) -> Uuid {
        self.pier
    }
}

/// Returned from [`crate::Manager::apply_sync_reply`].
#[derive(Debug)]
#[must_use]
pub struct MetadataApplier {
    metadata: Metadata,
    changes: Vec<MetadataChange>,
}
impl MetadataApplier {
    pub(crate) fn new(metadata: Metadata, changes: Vec<MetadataChange>) -> Self {
        Self { metadata, changes }
    }

    /// Apply the fast forward metadata changes to your public metadata `target`.
    #[inline]
    pub fn apply(&self, target: &mut Metadata) {
        target.apply_changes(&self.changes, &self.metadata);
    }
    /// Get a reference to the remote's metadata acquired during the fast forward.
    pub fn metadata(&self) -> &Metadata {
        &self.metadata
    }
}
