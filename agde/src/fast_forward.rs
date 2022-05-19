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
        Self {
            mtime_in_current: mtime_in_current.map(utils::systime_to_dur),
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
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
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
    pub fn iter(&self) -> impl Iterator<Item = (&str, ResourceMeta)> {
        self.map.iter().map(|(k, v)| (&**k, *v))
    }
    /// Get the metadata for `resource`.
    #[must_use]
    pub fn get(&self, resource: &str) -> Option<ResourceMeta> {
        self.map.get(resource).copied()
    }
    /// If the metadata contains `resource`.
    #[must_use]
    pub fn contains(&self, resource: &str) -> bool {
        self.map.contains_key(resource)
    }
    /// Remove the metadata for `resource`.
    pub fn remove(&mut self, resource: &str) {
        self.map.remove(resource);
    }
    /// Insert the `meta` for `resource`.
    pub fn insert(&mut self, resource: String, meta: ResourceMeta) {
        self.map.insert(resource, meta);
    }

    /// Calculate the changes to get the metadata from `self` to `other`.
    #[must_use]
    pub fn changes(&self, other: &Self) -> Vec<MetadataChange> {
        let mut changed = Vec::new();
        for (resource, meta) in self.iter() {
            match other.get(resource) {
                Some(current_data) => {
                    if meta.mtime_in_current().map_or(true, |mtime| {
                        mtime
                            != current_data
                                .mtime_in_current()
                                .expect("we just created this from local metadata")
                    }) || current_data.size() != meta.size()
                    {
                        changed.push(MetadataChange::Modify(resource.to_owned(), false));
                    }
                }
                None => changed.push(MetadataChange::Delete(resource.to_owned())),
            }
        }
        for resource in other.iter().map(|(k, _v)| k) {
            if !self.contains(resource) {
                changed.push(MetadataChange::Modify(resource.to_owned(), true));
            }
        }
        changed
    }
}

/// A single change between sets of metadata.
#[derive(Debug, PartialEq, Eq, Clone)]
pub enum MetadataChange {
    /// True if the resource was just created.
    Modify(String, bool),
    /// The resource was destroyed.
    Delete(String),
}
impl MetadataChange {
    /// The resource of the change.
    #[must_use]
    pub fn resource(&self) -> &str {
        match self {
            Self::Modify(r, _) | Self::Delete(r) => r,
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
}
/// A fast forward response. This contains the data to bring us up and running again.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Response {
    pub(crate) pier: Uuid,
    pub(crate) metadata: Metadata,
    /// [`None`] if our event log is empty.
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
}
