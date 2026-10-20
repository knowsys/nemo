//! The bit fields encodings lay out their payloads in.
//!
//! A [BitField] is a run of bits at a fixed position in the payload,
//! given as a range of bit positions such as `54..63`.
//! An encoding declares one constant per field.
//! Since each field knows its position,
//! [put](BitField::put) returns a whole payload,
//! the value in the field's bits and zeros elsewhere,
//! and [get](BitField::get) reads the field from a whole payload.
//! A payload is the bitwise or of one `put` per field,
//! and comparing payloads compares the fields from the most significant down.
//!
//! A field holds an unsigned value as is, and a signed one biased:
//! shifted up by half the field's range, which makes unsigned order signed order;
//! in two's complement, a negative value would compare above every other.
//! An 8-bit field holds -128 as `0x00`, 0 as `0x80` and 127 as `0xff`.

use std::ops::Range;

/// A run of bits at a fixed position in a payload.
#[derive(Debug, Clone, Copy)]
pub(super) struct BitField {
    /// The position of the field's lowest bit.
    start: u32,
    /// The number of bits in the field.
    width: u32,
}

impl BitField {
    /// Returns the field of the given bit positions, counted from 0 for the least significant bit.
    ///
    /// Panics if the range is empty or reaches past the 64 bits of a payload.
    pub(super) const fn new(bits: Range<u32>) -> Self {
        assert!(
            bits.start < bits.end && bits.end <= u64::BITS,
            "a bit field is a nonempty range within the payload"
        );

        Self {
            start: bits.start,
            width: bits.end - bits.start,
        }
    }

    /// Returns the number of bits in this field.
    pub(super) const fn width(self) -> u32 {
        self.width
    }

    /// Returns the unsigned value this field holds in a payload.
    pub(super) fn get(self, payload: u64) -> u64 {
        (payload >> self.start) & self.mask()
    }

    /// Returns the payload holding an unsigned value in this field and zeros elsewhere.
    ///
    /// The value must fit the field.
    pub(super) fn put(self, value: u64) -> u64 {
        debug_assert!(value <= self.mask(), "the value fits the field");
        value << self.start
    }

    /// Returns the signed value this field holds in a payload.
    pub(super) fn get_signed(self, payload: u64) -> i64 {
        let unused = u64::BITS - self.width;

        // Shifting the field to the top drops the bits above it; shifting back sign-extends.
        (((payload >> self.start) ^ self.top_bit()) << unused).cast_signed() >> unused
    }

    /// Returns the payload holding a signed value in this field and zeros elsewhere,
    /// or `None` beyond the field's range, `-2^(width-1)..2^(width-1)`.
    ///
    /// Biasing adds half the range, which in two's complement flips the field's top bit.
    pub(super) fn put_signed(self, value: i64) -> Option<u64> {
        let unused = u64::BITS - self.width;

        // The value fits if truncating it to the field and sign-extending it back keeps it.
        let fits = (value << unused) >> unused == value;
        fits.then(|| ((value.cast_unsigned() & self.mask()) ^ self.top_bit()) << self.start)
    }

    /// Returns the mask of the low bits as wide as this field.
    const fn mask(self) -> u64 {
        u64::MAX >> (u64::BITS - self.width)
    }

    /// Returns the top bit of a value as wide as this field.
    const fn top_bit(self) -> u64 {
        1 << (self.width - 1)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn signed_values_keep_order_and_round_trip() {
        let field = BitField::new(0..8);
        let values = [-128, -1, 0, 1, 127];

        assert_eq!(
            [-128, 0, 127].map(|value| field.put_signed(value)),
            [Some(0x00), Some(0x80), Some(0xff)]
        );
        for window in values.windows(2) {
            assert!(field.put_signed(window[0]) < field.put_signed(window[1]));
        }
        for value in values {
            let payload = field.put_signed(value).expect("value is in range");
            assert_eq!(field.get_signed(payload), value);
        }
    }

    #[test]
    fn signed_values_beyond_the_width_do_not_fit() {
        let field = BitField::new(0..8);

        assert_eq!(field.put_signed(-129), None);
        assert_eq!(field.put_signed(128), None);
    }

    #[test]
    fn fields_side_by_side_keep_their_values_and_order() {
        let (upper, lower) = (BitField::new(8..16), BitField::new(0..8));
        let payload = |unsigned, signed| {
            upper.put(unsigned) | lower.put_signed(signed).expect("value is in range")
        };

        assert_eq!(payload(0xab, -1), 0xab7f);
        assert_eq!(upper.get(payload(0xab, -1)), 0xab);
        assert_eq!(lower.get_signed(payload(0xab, -1)), -1);
        assert!(payload(1, 127) < payload(2, -128));
    }

    #[test]
    fn a_field_may_span_the_whole_payload() {
        let field = BitField::new(0..64);

        assert_eq!(field.get(u64::MAX), u64::MAX);
        assert_eq!(field.put_signed(i64::MIN), Some(0));
        assert_eq!(field.put_signed(-1), Some(u64::MAX >> 1));
        assert_eq!(field.put_signed(i64::MAX), Some(u64::MAX));

        for value in [i64::MIN, -1, 0, 1, i64::MAX] {
            let payload = field.put_signed(value).expect("every value fits");
            assert_eq!(field.get_signed(payload), value);
        }
    }
}
