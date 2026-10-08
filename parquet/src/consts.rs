//! Constants of the crate.

/// `state` of the `no_std` spin lock counts readers; this reserved value
/// marks the single writer.
pub(crate) const WRITER: usize = usize::MAX;
