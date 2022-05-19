//! Mechanics for reading the current data from other piers after being offline for some time.
use crate::{utils, Deserialize, Duration, HashMap, Serialize, SystemTime};

/// The metadata of a resource.
#[derive(Debug, Serialize, Deserialize, Clone, Copy)]
pub struct ResourceMeta {
    /// mtime of the [`Storage::Current`].
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
#[derive(Debug, Serialize, Deserialize, Clone)]
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
    pub fn remove(&mut self, resource: &str)  {
        self.map.remove(resource);
    }
    /// Insert the `meta` for `resource`.
    pub fn insert(&mut self, resource: String, meta: ResourceMeta)  {
        self.map.insert(resource, meta);
    }
}

/// A request to fast forward the local data.
#[derive(Debug)]
pub struct Request {}
