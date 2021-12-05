//! Module for handling the sync between piers.
//!
//! This occurs when the hashes don't line up or when fast forwarding on a new connection.

use serde::{Deserialize, Serialize};

use crate::{resource, Uuid};

// `TODO`: Add to [`Manager::process_sync`] to keep all events until this is resolved.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[must_use]
pub struct Request {
    pier: Uuid,
    resources: resource::Matcher,
}
impl Request {
    pub fn new(pier: Uuid, resources: resource::Matcher) -> Self {
        Self { pier, resources }
    }
}
impl PartialEq for Request {
    fn eq(&self, other: &Self) -> bool {
        self.pier == other.pier
    }
}
impl Eq for Request {}
