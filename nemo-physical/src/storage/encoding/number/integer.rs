//! Payload encoding of [Tag::Int].

use crate::storage::{
    Tag,
    encoding::{PayloadEncoding, bit_field::BitField},
};

/// The field holding the integer, spanning the whole payload.
const INTEGER: BitField = BitField::new(0..64);

/// Signed integers encode as a biased field spanning the whole payload,
/// which maps `i64::MIN..=i64::MAX` in order onto `0..=u64::MAX`.
impl PayloadEncoding for i64 {
    const TAG: Tag = Tag::Int;
    type Encoded = u64;

    fn to_payload(self) -> u64 {
        INTEGER
            .put_signed(self)
            .expect("every integer fits the whole payload")
    }

    fn from_payload(payload: u64) -> Self {
        INTEGER.get_signed(payload)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encoding_preserves_order() {
        let values = [i64::MIN, -100, -1, 0, 1, 100, i64::MAX];
        for window in values.windows(2) {
            assert!(window[0].to_payload() < window[1].to_payload());
        }
    }

    #[test]
    fn encoding_round_trips() {
        for value in [i64::MIN, -42, 0, 42, i64::MAX] {
            assert_eq!(i64::from_payload(value.to_payload()), value);
        }
    }
}
