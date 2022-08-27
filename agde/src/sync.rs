//! Module for handling the sync between piers.
//!
//! This occurs when the hashes don't line up or when fast forwarding on a new connection.

use std::cmp;
use std::collections::{hash_map, HashMap};
use std::time::{Duration, SystemTime};

use serde::{Deserialize, Serialize};

use crate::{event, log, Manager, Uuid};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Copy)]
pub(crate) enum RevertTo {
    /// Don't revert.
    Latest,
    /// Revert all the events in our [`crate::log`].
    Origin,
    /// Revert so our data is at the message with the contained UUID.
    To(Uuid),
}

/// Request to sync selected resources.
///
/// Obtained from [`RequestBuilder`], which you get from
/// [`crate::Manager::apply_hash_check_reply`]. This is the last request of the series of assuring
/// data is the same across all piers.
// `TODO`: send Signature of event log.
// ↑ We won't have to send as much data.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[must_use]
pub struct Request {
    pier: Uuid,
    signatures: HashMap<String, den::Signature>,
    log_settings: (Duration, u32),
    revert: RevertTo,
}
impl Request {
    pub(crate) fn recipient(&self) -> Uuid {
        self.pier
    }
}
/// A builder struct for a [`Request`].
///
/// See [`crate::Manager::apply_hash_check_reply`] for usage details.
#[derive(Debug)]
#[must_use]
pub struct RequestBuilder {
    req: Request,
}
impl RequestBuilder {
    pub(crate) fn new(
        pier: Uuid,
        log_lifetime: Duration,
        log_limit: u32,
        revert: RevertTo,
    ) -> Self {
        Self {
            req: Request {
                pier,
                signatures: HashMap::new(),
                log_settings: (log_lifetime, log_limit),
                revert,
            },
        }
    }
    /// Insert the `resource`'s `signature` to this response.
    ///
    /// The [`den::Signature`] allows the pier to get the diff for us.
    pub fn insert(&mut self, resource: String, signature: den::Signature) -> &mut Self {
        self.req.signatures.insert(resource, signature);
        self
    }
    /// Make a [`Request`] from this builder and a signature of all the resources inserted.
    ///
    /// Call [`crate::Manager::process_sync_reply`] to get a [`crate::Message`].
    #[inline]
    pub fn finish(self) -> Request {
        self.req
    }
}

/// The diffs to make the pier's data the same as ours.
#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
#[must_use]
pub struct Response {
    pier: Uuid,
    log: Vec<log::ReceivedEvent>,
    diff: Vec<(String, den::Difference)>,
    delete: Vec<String>,
    revert: RevertTo,
}
impl Response {
    pub(crate) fn recipient(&self) -> Uuid {
        self.pier
    }
    /// Extract the event log. After this function, it's reset to an empty list.
    pub(crate) fn take_event_log(&mut self) -> Vec<log::ReceivedEvent> {
        std::mem::take(&mut self.log)
    }
    pub(crate) fn revert(&self) -> RevertTo {
        self.revert
    }
    /// Returns a list with `(resource, diff)`,
    /// where you should call [`den::Difference::apply`] on the data
    /// `resource` holds.
    pub fn diff(&self) -> &[(impl AsRef<str>, den::Difference)] {
        &self.diff
    }
    /// Returns a list with `resource`,
    /// where you should delete `resource`.
    #[must_use]
    pub fn delete(&self) -> &[impl AsRef<str>] {
        &self.delete
    }
}
/// Builder for a [`Response`].
///
/// Follow the instructions from how you got this builder and [insert](`Self::add_diff`) the appropriate
/// resources.
/// Execute the action returned by the aforementioned function.
#[derive(Debug)]
pub struct ResponseBuilder<'a> {
    request: &'a Request,
    signature_iter: hash_map::Iter<'a, String, den::Signature>,
    pier: Uuid,
    /// Binary sorted by String
    diff: Vec<(String, den::Difference)>,

    unwinder: Option<event::Unwinder<'a>>,
}
impl<'a> ResponseBuilder<'a> {
    pub(crate) fn new(request: &'a Request, pier: Uuid, manager: &'a Manager) -> Self {
        Self {
            request,
            signature_iter: request.signatures.iter(),
            pier,
            diff: Vec::new(),

            unwinder: match request.revert {
                RevertTo::Latest => None,
                RevertTo::Origin => Some(manager.unwinder_to(SystemTime::UNIX_EPOCH)),
                RevertTo::To(uuid) => Some({
                    let events_start = manager.event_log.cutoff_from_uuid(uuid).unwrap_or(0);
                    let events = &manager.event_log.list[events_start + 1..];
                    event::Unwinder::new(events, Some(manager))
                }),
            },
        }
    }
    /// Use this in a `while let Some((resource, signature)) = response_builder.next_signature()`
    /// loop to add all the returned values to [`Self::add_diff`].
    ///
    /// If you don't have a resource, just don't call [`Self::add_diff`]. Agde will then automatically
    /// send a delete event then.
    pub fn next_signature(
        &mut self,
    ) -> Option<(&str, &den::Signature, Option<&mut event::Unwinder<'a>>)> {
        self.unwinder();
        let unwinder = self.unwinder.as_mut();
        self.signature_iter.next().map(|(k, v)| (&**k, v, unwinder))
    }
    /// Returns the unwinder (if any) to use for the `resource` before [adding](Self::add_diff) it.
    pub fn unwinder(&mut self) -> Option<&mut event::Unwinder<'a>> {
        self.unwinder.as_mut().map(|unwinder| {
            unwinder.clear_unwound();
            unwinder
        })
    }
    /// Tell the requester their `resource` needs to apply `diff` to get our data.
    ///
    /// You have to unwind `resource` before getting the `diff` if [`Self::unwinder`] returns
    /// [`Some`].
    ///
    /// # Panics
    ///
    /// Panics if you've called this with the same `resource` before.
    pub fn add_diff(&mut self, resource: String, diff: den::Difference) -> &mut Self {
        if let Err(idx) = self.diff.binary_search_by(|item| item.0.cmp(&resource)) {
            self.diff.insert(idx, (resource, diff));
        } else {
            panic!("you cannot add two diffs with the same resource name");
        }
        self
    }
    /// Finish the response.
    pub fn finish(mut self, manager: &Manager) -> Response {
        let mut delete = Vec::new();
        for resource in self.request.signatures.keys() {
            if self
                .diff
                .binary_search_by(|item| item.0.cmp(resource))
                .is_err()
            {
                delete.push(resource.clone());
            }
        }
        let max = cmp::min(
            manager
                .event_log
                .cutoff_from_time(self.request.log_settings.0)
                .unwrap_or(manager.event_log.limit() as usize),
            self.request.log_settings.1 as usize,
        );
        let event_log = manager.event_log.get_max(max).to_vec();
        let last = event_log.last().map(|ev| ev.message_uuid);
        self.diff.retain(|diff| !diff.1.is_empty());
        Response {
            pier: self.pier,
            log: event_log,
            diff: self.diff,
            delete,
            revert: last.map_or(RevertTo::Origin, RevertTo::To),
        }
    }
}
