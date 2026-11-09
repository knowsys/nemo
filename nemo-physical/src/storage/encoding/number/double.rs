//! The payload encoding of [Tag::Double].

use crate::{
    storage::{Tag, encoding::PayloadEncoding},
    value::Double,
};

/// A double is encoded as its [order key](Double::order_key), whose order is the term order.
///
/// The encoding covers the value space of XSD 1.1, which has one NaN.
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
