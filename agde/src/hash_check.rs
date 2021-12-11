//! Checking of hashes of [`crate::resource`]s.

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::hash::Hasher;
use std::time::Duration;

use twox_hash::xxh3::HasherExt;

use crate::{event, resource, SelectedPier, Uuid};

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
            cutoff_timestamp: event::dur_now().saturating_sub(timestamp_offset),
            offset: timestamp_offset,
        }
    }
    /// Get the receiver's UUID.
    #[inline]
    pub fn pier(&self) -> Uuid {
        self.pier
    }
    #[must_use]
    #[inline]
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
/// If the [receiver](Self::pier) is the same, this is considered equal.
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
    pub fn pier(&self) -> Uuid {
        self.pier
    }
    /// Get a reference to the response's hashes.
    #[must_use]
    #[inline]
    pub fn hashes(&self) -> &BTreeMap<String, ResponseHash> {
        &self.hashes
    }
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
            .insert(resource, hash.0.finish_ext().to_le_bytes());
    }
    #[inline]
    pub fn finish(self) -> Response {
        self.0
    }
}
#[allow(missing_debug_implementations)]
#[must_use]
pub struct ResponseHasher(twox_hash::Xxh3Hash128);
impl ResponseHasher {
    #[inline]
    pub fn new() -> Self {
        Self(twox_hash::Xxh3Hash128::default())
    }
    #[allow(clippy::inline_always)]
    #[inline(always)]
    pub fn write(&mut self, bytes: &[u8]) {
        self.0.write(bytes);
    }
}

impl Default for ResponseHasher {
    fn default() -> Self {
        Self::new()
    }
}
