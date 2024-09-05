//! This module defines the trait [ByteSized],
//! which should be implemented by types that can
//! calculate their own size.

/// Objects that are able calculate their current approximate size in bytes.
/// 
/// We use `u64` rather than `usize` here to avoid overflows in case of overestimations.
pub trait ByteSized {
    /// Return the number of bytes this object consumes
    fn size_bytes(&self) -> u64;
}


