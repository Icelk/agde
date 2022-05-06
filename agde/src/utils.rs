//! Utilities for working with some datatypes used by agde.

use crate::{Duration, SystemTime, UNIX_EPOCH};

/// Returns the time since [`UNIX_EPOCH`].
#[must_use]
pub fn dur_now() -> Duration {
    systime_to_dur(SystemTime::now())
}
/// Convert `dur` (duration since [`UNIX_EPOCH`]) to [`SystemTime`].
/// Very cheap conversion.
#[must_use]
pub fn dur_to_systime(dur: Duration) -> SystemTime {
    SystemTime::UNIX_EPOCH + dur
}
/// Convert `systime` to [`Duration`] (since [`UNIX_EPOCH`]).
/// Very cheap conversion.
#[must_use]
pub fn systime_to_dur(systime: SystemTime) -> Duration {
    systime.duration_since(UNIX_EPOCH).unwrap_or(Duration::ZERO)
}

/// `a-b` but with better overflow properties than `a as isize - b as isize`.
#[must_use]
#[allow(clippy::cast_possible_wrap)]
pub fn sub_usize(a: usize, b: usize) -> isize {
    if a > b {
        (a - b) as isize
    } else {
        -((b - a) as isize)
    }
}
/// `a+b` but with better overflow properties than `(a as isize + b) as usize`.
/// Returns [`None`] if `(-b) > a`
#[must_use]
#[allow(clippy::cast_sign_loss)]
pub fn iusize_add(a: usize, b: isize) -> Option<usize> {
    if b.is_negative() {
        let b = (-b) as usize;
        if b > a {
            None
        } else {
            Some(a - b)
        }
    } else {
        Some(a + b as usize)
    }
}
