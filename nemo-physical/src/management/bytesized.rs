//! This module defines the trait [ByteSized],
//! which should be implemented by types that can
//! calculate their own size.

use bytesize::ByteSize;

/// Objects that are able calculate their current size in bytes
pub trait ByteSized {
    /// Return the number of bytes this object consumes
    fn size_bytes(&self) -> ByteSize;
}

/// Helper method to sum up a collection of [ByteSize],
/// since the `Sum` is not implemented.
pub(crate) fn sum_bytes<ByteIterator: Iterator<Item = ByteSize>>(
    iterator: ByteIterator,
) -> ByteSize {
    let mut result = ByteSize::b(0);

    for byte_size in iterator {
        result += byte_size;
    }

    result
}
