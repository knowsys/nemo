//! Payload encoding of [Tag::Decimal].

use crate::{
    storage::{
        Tag,
        encoding::{PayloadEncoding, bit_field::BitField},
    },
    value::number::decimal,
};

/// The sign bit of a payload.
const SIGN_BIT: u64 = 1 << 63;

/// The field holding the scaled exponent, below the sign bit.
const EXPONENT: BitField = BitField::new(54..63);

/// The field holding the digits of the mantissa, padded to full length, in the low bits.
const MANTISSA: BitField = BitField::new(0..54);

/// The compact range fits the fields:
/// sixteen digits in the mantissa, and the scaled exponents in the exponent.
const _: () = {
    assert!(10u64.pow(decimal::Compact::SIGNIFICANT_DIGITS) <= 1 << MANTISSA.width());
    let half = 1 << (EXPONENT.width() - 1);
    let range = decimal::Compact::SCALED_EXPONENT_RANGE;
    assert!(-half <= range.start && range.end <= half);
};

/// Compact decimals are exactly the values the payload holds.
impl PayloadEncoding for decimal::Compact {
    const TAG: Tag = Tag::Decimal;
    type Encoded = u64;

    fn to_payload(self) -> u64 {
        let padding = decimal::Compact::SIGNIFICANT_DIGITS - self.digits();
        let digits = self.mantissa().unsigned_abs() * 10u64.pow(padding);
        let exponent = EXPONENT
            .put_signed(self.scaled_exponent().into())
            .expect("the scaled exponent fits");

        let key = exponent | MANTISSA.put(digits);
        if self.mantissa() < 0 {
            !(SIGN_BIT | key)
        } else {
            SIGN_BIT | key
        }
    }

    fn from_payload(payload: u64) -> Self {
        let (key, negative) = if payload & SIGN_BIT != 0 {
            (payload & !SIGN_BIT, false)
        } else {
            (!payload & !SIGN_BIT, true)
        };

        let mut digits = MANTISSA.get(key).cast_signed();
        let mut exponent =
            i32::try_from(EXPONENT.get_signed(key)).expect("the scaled exponent fits");

        // Removing the padding restores the normal form.
        while digits % 10 == 0 {
            digits /= 10;
            exponent += 1;
        }

        Self::from_encoded(if negative { -digits } else { digits }, exponent)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use rand::{Rng, SeedableRng, rngs::StdRng};

    /// Number of random samples drawn by the randomized tests.
    const SAMPLES: usize = 100_000;

    /// Draws a random compact decimal.
    fn random_input(rng: &mut StdRng) -> decimal::Compact {
        loop {
            if let Some(value) = decimal::Compact::new(
                rng.gen_range(-9_999_999_999_999_999..=9_999_999_999_999_999),
                rng.gen_range(-9..=-1),
            ) {
                return value;
            }
        }
    }

    /// The edges of the compact range.
    fn edge_cases() -> Vec<decimal::Compact> {
        [
            (9_999_999_999_999_999, -9),
            (-9_999_999_999_999_999, -9),
            (9_999_999_999_999_999, -1),
            (-9_999_999_999_999_999, -1),
            (9_999_999_999_999_999, -256),
            (-9_999_999_999_999_999, -256),
            (1, -241),
            (-1, -241),
            (1, -1),
            (-1, -1),
            (15, -1),
            (-15, -1),
            (123, -9),
            (-123, -9),
        ]
        .map(|(mantissa, exponent)| {
            decimal::Compact::new(mantissa, exponent).expect("an edge of the compact range")
        })
        .to_vec()
    }

    #[test]
    fn encoding_round_trips() {
        let mut rng = StdRng::seed_from_u64(1);
        for value in edge_cases() {
            assert_eq!(decimal::Compact::from_payload(value.to_payload()), value);
        }

        for _ in 0..SAMPLES {
            let value = random_input(&mut rng);
            assert_eq!(decimal::Compact::from_payload(value.to_payload()), value);
        }
    }

    #[test]
    fn encoding_preserves_order() {
        // Across signs, across scaled exponents (0.09 and 0.1),
        // within one scaled exponent, and at the ends of the range.
        let ordered = [
            (-9_999_999_999_999_999, -1),
            (-1, -1),
            (-9, -2),
            (-1, -241),
            (1, -241),
            (9, -2),
            (1, -1),
            (1_000_000_000_000_001, -16),
            (15, -1),
            (9_999_999_999_999_999, -1),
        ]
        .map(|(mantissa, exponent)| {
            decimal::Compact::new(mantissa, exponent).expect("within the compact range")
        });
        for window in ordered.windows(2) {
            assert!(window[0] < window[1], "{} < {}", window[0], window[1]);
            assert!(
                window[0].to_payload() < window[1].to_payload(),
                "{} < {}",
                window[0],
                window[1]
            );
        }

        let edges = edge_cases();
        for left in &edges {
            for right in &edges {
                let payloads = left.to_payload().cmp(&right.to_payload());
                assert_eq!(payloads, left.cmp(right), "{left} against {right}");
            }
        }

        let mut rng = StdRng::seed_from_u64(2);
        for _ in 0..SAMPLES {
            let (left, right) = (random_input(&mut rng), random_input(&mut rng));
            let payloads = left.to_payload().cmp(&right.to_payload());
            assert_eq!(payloads, left.cmp(&right), "{left} against {right}");
        }
    }
}
