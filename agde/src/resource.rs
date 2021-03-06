//! Handling of getting and setting resources.

use crate::{Deserialize, Serialize};

/// A filter to match a `resource`.
#[derive(Debug, Serialize, Deserialize, Clone)]
#[must_use]
pub enum Matches {
    /// Matches noting.
    None,
    /// Matches everything.
    All,
    /// Matches the exact string.
    Exact(String),
    /// Matches if any [`Matches`] in the list match.
    List(Vec<Matches>),
}
impl Matches {
    /// Checks if this `resource` is allowed with the filter.
    #[must_use]
    #[inline]
    pub fn matches(&self, resource: &str) -> bool {
        match self {
            Self::All => true,
            Self::None => false,
            Self::Exact(target) => resource == target,
            Self::List(list) => list.iter().any(|matches| matches.matches(resource)),
        }
    }
    #[inline]
    fn make_list(&mut self) {
        let mut vec = Vec::with_capacity(2);
        let included = std::mem::replace(self, Matches::None);
        vec.push(included);
        *self = Matches::List(vec);
    }
}

/// Matches `resource`s.
///
/// If no [`Self::include`]s are given, all but the [`Self::exclude`] will be matched.
/// Simmilaraly, if no `exclude`s are given, all but the `include`s will be rejected.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[must_use]
pub struct Matcher {
    include: Matches,
    exclude: Matches,
}
impl Matcher {
    /// Matches no resource.
    pub fn new() -> Matcher {
        Matcher {
            include: Matches::None,
            exclude: Matches::None,
        }
    }
    /// Match all.
    pub fn all() -> Self {
        Self::new().set_include(Matches::All)
    }
    /// Set the included matches.
    ///
    /// See [`Self`] for more info.
    #[inline]
    pub fn set_include(mut self, include: Matches) -> Self {
        self.include = include;
        self
    }
    /// Set the excluded matches.
    ///
    /// See [`Self`] for more info.
    #[inline]
    pub fn set_exclude(mut self, exclude: Matches) -> Self {
        self.exclude = exclude;
        self
    }
    /// Additionally includes `include`.
    ///
    /// If [`Self::get_include`] is [`Matches::All`], this does nothing.
    #[inline]
    pub fn include(mut self, include: Matches) -> Self {
        match &mut self.include {
            Matches::All => self,
            Matches::None => self.set_include(include),
            Matches::List(list) => {
                list.push(include);
                self
            }
            Matches::Exact(_) => {
                self.include.make_list();
                self.include(include)
            }
        }
    }
    /// Additionally excludes `exclude`.
    ///
    /// If [`Self::get_exclude`] is [`Matches::All`], this does nothing.
    #[inline]
    pub fn exclude(mut self, exclude: Matches) -> Self {
        match &mut self.include {
            Matches::All => self,
            Matches::None => self.set_exclude(exclude),
            Matches::List(list) => {
                list.push(exclude);
                self
            }
            Matches::Exact(_) => {
                self.exclude.make_list();
                self.exclude(exclude)
            }
        }
    }

    /// Get a reference to the include filter.
    #[inline]
    pub fn get_include(&self) -> &Matches {
        &self.include
    }

    /// Get a reference to the exclude filter.
    #[inline]
    pub fn get_exclude(&self) -> &Matches {
        &self.exclude
    }
    /// Test if the resource matches the filter.
    ///
    /// Tests if [`Self::include`] matches AND [`Self::exclude`] doesn't.
    #[must_use]
    #[allow(clippy::inline_always)]
    #[inline(always)]
    pub fn matches(&self, resource: &str) -> bool {
        self.include.matches(resource) && !self.exclude.matches(resource)
    }
}

impl Default for Matcher {
    fn default() -> Self {
        Self::new()
    }
}
