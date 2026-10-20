//! The payload encodings of the inline types.
//!
//! Each inline type implements [PayloadEncoding] in its own submodule,
//! grouped by kind as the value module groups the types:
//! booleans, numbers, and points in time and durations.
//! Encodings that pack integers into their payload lay them out as [bit fields](bit_field::BitField).

mod bit_field;
mod boolean;
mod number;
mod temporal;

use crate::storage::Tag;

/// The payload encoding of an inline type.
///
/// The encoding preserves order:
/// comparing two payloads as unsigned integers compares the values they encode.
/// Equal values share one payload.
pub(crate) trait PayloadEncoding: Sized {
    /// The tag of the values this encoding produces.
    const TAG: Tag;

    /// The result of encoding a value:
    /// `u64` for a total encoding,
    /// `Option<u64>` for a partial one,
    /// where `None` means the value is not representable.
    type Encoded: Into<Option<u64>>;

    /// Encodes this value into its payload.
    fn to_payload(self) -> Self::Encoded;

    /// Decodes a payload of this type's tag.
    fn from_payload(payload: u64) -> Self;
}
