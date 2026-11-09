//! The payload encodings of the inline types.
//!
//! An inline type is a type whose values, or some of them, a
//! [StorageValue](crate::storage::StorageValue) encodes directly in its payload.
//! Each inline type implements [PayloadEncoding] in a submodule,
//! for booleans, for numbers, and for points in time and durations.
//! Encodings that pack several integers into a payload
//! lay them out as [bit fields](bit_field::BitField).

mod bit_field;
mod boolean;
mod number;
mod temporal;

use crate::storage::Tag;

/// The payload encoding of an inline type.
///
/// The encoding preserves order:
/// comparing two payloads as unsigned integers compares the values that they encode.
/// Identical values have the same payload.
pub(crate) trait PayloadEncoding: Sized {
    /// The tag of the values that this encoding produces.
    const TAG: Tag;

    /// The result of encoding a value.
    ///
    /// It is `u64` for an encoding of every value of the type,
    /// and `Option<u64>` for an encoding of some values,
    /// which is `None` for a value that does not fit in a payload.
    type Encoded: Into<Option<u64>>;

    /// Encodes this value as its payload.
    fn to_payload(self) -> Self::Encoded;

    /// Decodes a payload of this type's tag.
    fn from_payload(payload: u64) -> Self;
}
