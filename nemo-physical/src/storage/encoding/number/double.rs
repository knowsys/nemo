//! Payload encoding of [Tag::Double].

use crate::{
    storage::{Tag, encoding::PayloadEncoding},
    value::Double,
};

/// Doubles encode as their term order key,
/// which covers the XSD 1.1 value space with its one NaN.
impl PayloadEncoding for Double {
    const TAG: Tag = Tag::Double;
    type Encoded = u64;

    fn to_payload(self) -> u64 {
        self.order_key()
    }

    fn from_payload(payload: u64) -> Self {
        Self::from_order_key(payload)
    }
}
