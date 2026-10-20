//! Payload encoding of [Tag::Bool].

use crate::storage::{Tag, encoding::PayloadEncoding};

/// Booleans encode as 0 for `false` and 1 for `true`.
impl PayloadEncoding for bool {
    const TAG: Tag = Tag::Bool;
    type Encoded = u64;

    fn to_payload(self) -> u64 {
        u64::from(self)
    }

    fn from_payload(payload: u64) -> Self {
        debug_assert!(payload <= 1, "boolean payloads are 0 or 1");
        payload != 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encoding_round_trips_in_order() {
        assert!(false.to_payload() < true.to_payload());
        assert!(!bool::from_payload(false.to_payload()));
        assert!(bool::from_payload(true.to_payload()));
    }
}
