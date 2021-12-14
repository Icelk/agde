//! Module for handling the sync between piers.
//!
//! This occurs when the hashes don't line up or when fast forwarding on a new connection.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::{resource, Uuid};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[must_use]
pub struct Request {
    pier: Uuid,
    resources: resource::Matcher,
    signature: HashMap<String, den::Signature>,
}
impl Request {
    pub(crate) fn recipient(&self) -> Uuid {
        self.pier
    }
}
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
    signature: HashMap<String, den::Signature>,
}
impl RequestBuilder {
    pub(crate) fn new(pier: Uuid, resources: resource::Matcher) -> Self {
        Self {
            pier,
            resources,
            signature: HashMap::new(),
        }
    }
    pub fn insert(&mut self, resource: String, signature: den::Signature) -> &mut Self {
        self.signature.insert(resource, signature);
        self
    }
    /// Make a [`Request`] from this builder and a signature of all the resources matched using
    /// [`Self::matches`].
    #[inline]
    pub fn finish(self) -> Request {
        Request {
            pier: self.pier,
            resources: self.resources,
            signature: self.signature,
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
// `TODO`: apply the response, returning a iterator of the enum above?
// `TODO`: Also sync event log
pub struct Response {
    pier: Uuid,
    // event_log:
    diff: Vec<(String, den::Difference)>,
    create: Vec<(String, Vec<u8>)>,
    delete: Vec<String>,
}
impl Response {
    pub(crate) fn recipient(&self) -> Uuid {
        self.pier
    }
}
#[derive(Debug)]
pub struct ResponseBuilder<'a> {
    request: &'a Request,
    pier: Uuid,
    /// Binary sorted by String
    diff: Vec<(String, den::Difference)>,
    /// Binary sorted by String
    create: Vec<(String, Vec<u8>)>,
}
impl<'a> ResponseBuilder<'a> {
    pub(crate) fn new(request: &'a Request, pier: Uuid) -> Self {
        Self {
            request,
            pier,
            diff: Vec::new(),
            create: Vec::new(),
        }
    }
    pub fn matches(&self, resource: &str) -> ResponseBuilderAction {
        if self.request.resources.matches(resource) {
            match self.request.signature.get(resource) {
                Some(_) => ResponseBuilderAction::Difference,
                None => ResponseBuilderAction::Create,
            }
        } else {
            ResponseBuilderAction::Ignore
        }
    }
    pub fn diff(&mut self, resource: String, diff: den::Difference) -> &mut Self {
        self.diff.push((resource, diff));
        self
    }
    pub fn create(&mut self, resource: String, content: Vec<u8>) -> &mut Self {
        self.create.push((resource, content));
        self
    }
    pub(crate) fn finish(self) -> Response {
        let mut delete = Vec::new();
        for resource in self.request.signature.keys() {
            if !(self
                .diff
                .binary_search_by(|item| item.0.cmp(resource))
                .is_ok()
                || self
                    .create
                    .binary_search_by(|item| item.0.cmp(resource))
                    .is_ok())
            {
                delete.push(resource.clone());
            }
        }
        Response {
            pier: self.pier,
            diff: self.diff,
            create: self.create,
            delete,
        }
    }
}
/// An action to take for a local resource, dictated by the [`Request`].
#[derive(Debug, Clone, Copy)]
#[must_use]
pub enum ResponseBuilderAction {
    /// Ignore this resource
    Ignore,
    /// Call [`ResponseBuilder::diff`]
    Difference,
    /// Call [`ResponseBuilder::create`]
    Create,
}
