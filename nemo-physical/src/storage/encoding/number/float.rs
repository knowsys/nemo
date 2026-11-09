//! The payload encoding of [Tag::Float].

use crate::{
    storage::{Tag, encoding::PayloadEncoding},
    value::Float,
};

/// A float is encoded as its [order key](Float::order_key), whose order is the term order,
/// in the low 32 bits of the payload.
///
/// The encoding covers the value space of XSD 1.1, which has one NaN.
impl PayloadEncoding for Float {
    const TAG: Tag = Tag::Float;
    type Encoded = u64;

    fn to_payload(self) -> u64 {
        u64::from(self.order_key())
    }

    fn from_payload(payload: u64) -> Self {
        Self::from_order_key(u32::try_from(payload).expect("a float payload fits 32 bits"))
    }
}
