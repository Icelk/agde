//! Checking of hashes of [`crate::resource`]s.

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::time::Duration;

use crate::{resource, utils, SelectedPier, Uuid};

/// A check to affirm the selected resources contain the same data.
///
/// For this to work, we assume the resulting hash is unique.
///
/// # Eq implementation
///
/// If the [receiver](Self::recipient) is the same, this is considered equal.
/// Only one of these conversations should be communicated at once, therefore the filter doesn't
/// matter.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[must_use]
pub struct Request {
    pier: Uuid,
    resources: resource::Matcher,
    /// The actual timestamp
    cutoff_timestamp: Duration,
    /// The offset to get `cutoff_timestamp` from `now`.
    /// To get when this was created, take `cutoff_timestamp + offset`.
    offset: Duration,
}
impl Request {
    /// Creates a new request which tells `pier` to return all resources according to the `filter`.
    /// Targets `now() - timestamp` as the rewind position.
    ///
    /// `timestamp` should be before the middle of the log lifetime of the two piers conversing.
    pub(crate) fn new(
        pier: SelectedPier,
        filter: resource::Matcher,
        timestamp_offset: Duration,
    ) -> Self {
        Self {
            pier: pier.uuid(),
            resources: filter,
            cutoff_timestamp: utils::dur_now().saturating_sub(timestamp_offset),
            offset: timestamp_offset,
        }
    }
    /// Get the receiver's UUID.
    #[inline]
    pub fn recipient(&self) -> Uuid {
        self.pier
    }
    /// Test if `resource` is included in the requested hash check.
    #[inline]
    #[must_use]
    pub fn matches(&self, resource: &str) -> bool {
        self.resources.matches(resource)
    }

    /// Get the request's cutoff offset.
    #[inline]
    pub(crate) fn cutoff_offset(&self) -> Duration {
        self.offset
    }
}
impl PartialEq for Request {
    fn eq(&self, other: &Self) -> bool {
        self.pier == other.pier
            && self.cutoff_timestamp == other.cutoff_timestamp
            && self.offset == other.offset
    }
}
impl Eq for Request {}

/// The hash data for the response.
pub type ResponseHash = [u8; 16];

/// A response to [`Request`].
///
/// Contains the [`Self::hashes`] for all the resources the sender wants.
///
/// # Eq implementation
///
/// If the [receiver](Self::recipient) is the same, this is considered equal.
/// Only one of these conversations should be communicated at once, therefore the filter doesn't
/// matter.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[must_use]
pub struct Response {
    pier: Uuid,
    hashes: BTreeMap<String, ResponseHash>,
    requested_cutoff_timestamp: Duration,
    cutoff_timestamp: Duration,
}

impl Response {
    /// Get the receiver's UUID.
    #[inline]
    pub fn recipient(&self) -> Uuid {
        self.pier
    }
    /// Get a reference to the response's hashes.
    #[must_use]
    #[inline]
    pub fn hashes(&self) -> &BTreeMap<String, ResponseHash> {
        &self.hashes
    }
    /// Tests if the requested cutoff (in time) is the same as the one reponded with.
    ///
    /// While the pier is handling the [`Request`] we sent, we can process our own response, to
    /// be ready once we get this response. This checks if the remote was forced to change the
    /// cutoff. If the returned value is true, we cannot rely on the hash check response we created
    /// while the pier processed it.
    #[must_use]
    #[inline]
    pub fn different_cutoff(&self) -> bool {
        self.requested_cutoff_timestamp == self.cutoff_timestamp
    }
}
impl PartialEq for Response {
    fn eq(&self, other: &Self) -> bool {
        self.pier == other.pier && self.hashes == other.hashes
    }
}
impl Eq for Response {}
/// Builder struct for [`Response`].
#[derive(Debug)]
#[repr(transparent)]
pub struct ResponseBuilder(Response);
impl ResponseBuilder {
    pub(crate) fn new(pier: Uuid, request: &Request, selected_cutoff_offset: Duration) -> Self {
        Self(Response {
            pier,
            hashes: BTreeMap::new(),
            requested_cutoff_timestamp: request.cutoff_timestamp,
            cutoff_timestamp: (request.cutoff_timestamp + request.offset)
                .saturating_sub(selected_cutoff_offset),
        })
    }
    /// It's a logic error to pass a `resource` that isn't included in the [`Request::matches`].
    #[allow(clippy::needless_pass_by_value)] // The hasher is consumed for one resource.
    #[inline]
    pub fn insert(&mut self, resource: String, hash: ResponseHasher) {
        self.0
            .hashes
            .insert(resource, hash.0.digest128().to_le_bytes());
    }
    /// Get the built [`Response`].
    #[inline]
    pub fn finish(self) -> Response {
        self.0
    }
}
/// A hash builder for adding the hashed signature of a `resource`.
/// Should be [created](Self::new), [written to](Self::write) and the
/// [added](ResponseBuilder::insert) when all the data is written.
#[allow(missing_debug_implementations)]
#[must_use]
pub struct ResponseHasher(xxhash_rust::xxh3::Xxh3);
impl ResponseHasher {
    /// Creates a new, empty hasher.
    ///
    /// Add data using [`Self::write`].
    #[inline]
    pub fn new() -> Self {
        Self(xxhash_rust::xxh3::Xxh3::default())
    }
    /// Write data from resource to the internal hasher.
    ///
    /// After all the data for one resource is written, call [`ResponseBuilder::insert`].
    #[allow(clippy::inline_always)]
    #[inline(always)]
    pub fn write(&mut self, bytes: &[u8]) {
        self.0.update(bytes);
    }
}

impl Default for ResponseHasher {
    fn default() -> Self {
        Self::new()
    }
}
