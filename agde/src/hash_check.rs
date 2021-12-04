//! Checking of hashes of [`crate::resource`]s.

use std::collections::BTreeMap;
use std::hash::Hasher;

use twox_hash::xxh3::HasherExt;

use crate::{resource, Deserialize, SelectedPier, Serialize, Uuid};

/// A check to affirm the selected resources contain the same data.
///
/// For this to work, we assume the resulting hash is unique.
///
/// # Eq implementation
///
/// If the [receiver](Self::pier) is the same, this is considered equal.
/// Only one of these conversations should be communicated at once, therefore the filter doesn't
/// matter.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[must_use]
pub struct Request {
    pier: Uuid,
    resources: resource::Matcher,
}
impl Request {
    /// Creates a new request which tells `pier` to return all resources according to the `filter`.
    pub fn new(pier: SelectedPier, filter: resource::Matcher) -> Self {
        Self {
            pier: pier.uuid(),
            resources: filter,
        }
    }
    /// Requests the `pier` to send back the hashes of all it's resources.
    pub fn all(pier: SelectedPier) -> Self {
        Self::new(pier, resource::Matcher::all())
    }
    /// Get the receiver's UUID.
    pub fn pier(&self) -> Uuid {
        self.pier
    }
    pub fn matches(&self, resource: &str) -> bool {
        self.resources.matches(resource)
    }
}
impl PartialEq for Request {
    fn eq(&self, other: &Self) -> bool {
        self.pier == other.pier
    }
}
impl Eq for Request {}

/// The hash data for the response.
pub type ResponseHash = [u8; 16];

/// A response to [`Request`].
///
/// Contains the [`Self::hashes`] for all the [`Request::resouces`] the sender has.
///
/// # Eq implementation
///
/// If the [receiver](Self::pier) is the same, this is considered equal.
/// Only one of these conversations should be communicated at once, therefore the filter doesn't
/// matter.
#[must_use]
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Response {
    pier: Uuid,
    hashes: BTreeMap<String, ResponseHash>,
}

impl Response {
    /// Get the receiver's UUID.
    pub fn pier(&self) -> Uuid {
        self.pier
    }
    /// Get a reference to the response's hashes.
    #[must_use]
    pub fn hashes(&self) -> &BTreeMap<String, ResponseHash> {
        &self.hashes
    }
}
impl PartialEq for Response {
    fn eq(&self, other: &Self) -> bool {
        self.pier == other.pier && self.hashes == other.hashes
    }
}
impl Eq for Response {}
#[derive(Debug, Clone)]
#[repr(transparent)]
pub struct ResponseBuilder(Response);
impl ResponseBuilder {
    pub(crate) fn new(pier: Uuid) -> Self {
        Self(Response {
            pier,
            hashes: BTreeMap::new(),
        })
    }
    pub fn insert(&mut self, resource: String, hash: ResponseHasher) {
        self.0
            .hashes
            .insert(resource, hash.0.finish_ext().to_le_bytes());
    }
    pub fn finish(self) -> Response {
        self.0
    }
}
#[allow(missing_debug_implementations)]
pub struct ResponseHasher(twox_hash::Xxh3Hash128);
impl ResponseHasher {
    pub fn new() -> Self {
        Self(twox_hash::Xxh3Hash128::default())
    }
    pub fn write(&mut self, bytes: &[u8]) {
        self.0.write(bytes);
    }
}

impl Default for ResponseHasher {
    fn default() -> Self {
        Self::new()
    }
}
