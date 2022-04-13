//! Utilities for working with the diff library, currently [`den`].
//!
//! These functions are wrappers for integrating `agde` with it's diff library.
//! It handles converting data structures so the two can efficiently work together.

use den::{Difference, Signature};

/// Creates a granular [`Difference`] between `base` and `target`.
///
/// If you [`Difference::apply`] this on `base`, you **should** get `target`.
pub fn diff(base: &[u8], target: &[u8]) -> Difference {
    let mut sig = Signature::new(256);
    sig.write(base);
    let sig = sig.finish();
    let rough_diff = sig.diff(target);
    println!("rough {rough_diff:?}");
    #[allow(clippy::let_and_return)]
    let granular_diff = rough_diff
        .minify(8, base)
        .expect("The way we are using the function, this should never err.");
    granular_diff
}
