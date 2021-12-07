# v0.2.1

## Added

- More trait implementations for `Difference` & `Signature` & `Segment`.
- `HashResult::to_bytes` now takes ownership of `self`.

# v0.2.0

## Added

- `Difference::minify`, makes diff smaller (if original data is available)

## Changed

- `diff` -> `Signature::diff`
- `apply` -> `Difference::apply`
