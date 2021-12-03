//! Handling of getting and setting resources.

use crate::{Deserialize, Serialize};

/// Matches `resource`s.
///
/// If no [`Self::include`]s are given, all but the [`Self::exclude`] will be matched.
/// Simmilaraly, if no `exclude`s are given, all but the `include`s will be rejected.
#[derive(Debug, Serialize, Deserialize)]
pub struct Matcher<Includes: Matches, Excludes: Matches> {
    include: Includes,
    exclude: Excludes,
}

/// A filter to match a `resource`.
pub trait Matches {
    /// If `resource` should be included, return `true`. Otherwise, `false`.
    fn matches(&self, resource: &str) -> bool;
}
