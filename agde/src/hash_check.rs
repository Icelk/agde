//! Checking of hashes of [`crate::resource`]s.

use crate::{Deserialize, Serialize};

/// A check to affirm the selected resources contain the same data.
///
/// For this to work, we assume the resulting hash is unique.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct Check {}
