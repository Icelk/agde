//! Module for handling the sync between piers.
//!
//! This occurs when the hashes don't line up or when fast forwarding on a new connection.

use serde::{Deserialize, Serialize};

use crate::{resource, Uuid};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[must_use]
pub struct Request {
    pier: Uuid,
    resources: resource::Matcher,
    signature: den::Signature,
}
impl Request {}
impl PartialEq for Request {
    fn eq(&self, other: &Self) -> bool {
        self.pier == other.pier && self.signature == other.signature
    }
}
impl Eq for Request {}
#[derive(Debug)]
#[must_use]
pub struct RequestBuilder {
    pier: Uuid,
    resources: resource::Matcher,
}
impl RequestBuilder {
    pub(crate) fn new(pier: Uuid, resources: resource::Matcher) -> Self {
        Self { pier, resources }
    }
    /// Make a [`Request`] from this builder and a signature of all the resources matched using
    /// [`Self::matches`].
    #[inline]
    pub fn finish(self, signature: den::Signature) -> Request {
        Request {
            pier: self.pier,
            resources: self.resources,
            signature,
        }
    }
    /// Test if this `resource` should be part of the `signature` in [`Self::finish`].
    #[must_use]
    #[inline]
    pub fn matches(&self, resource: &str) -> bool {
        self.resources.matches(resource)
    }
}

#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
#[must_use]
pub struct Response {
    pier: Uuid,
    diff: den::Difference,
}
