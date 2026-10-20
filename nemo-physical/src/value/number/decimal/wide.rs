//! Exact numbers beyond the compact ranges, owned and borrowed.

use std::{cmp::Ordering, fmt, sync::Arc};

use num_bigint::{BigUint, Sign};

use crate::value::number::{decimal::workspace, lexical::Lexical};

/// A borrowed [`xsd:decimal`](https://www.w3.org/TR/xmlschema11-2/#decimal) value beyond the compact ranges:
/// an integer outside the 64-bit range,
/// or a non-integral decimal with more significant digits, or a smaller magnitude,
/// than a [Compact](super::Compact) holds.
/// Its owned form is [Wide].
///
/// An integral value is an [`xsd:integer`](https://www.w3.org/TR/xmlschema11-2/#integer).
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct WideRef<'a> {
    /// Whether the value is negative.
    negative: bool,
    /// The base-10 exponent.
    exponent: i64,
    /// The magnitude of the mantissa, little-endian, without trailing zero bytes.
    magnitude: &'a [u8],
}

impl<'a> WideRef<'a> {
    /// Creates a view from a stored sign, exponent and magnitude.
    ///
    /// The parts must be canonical and beyond the compact ranges,
    /// as the owned values and the dictionary store them.
    pub(crate) fn new(negative: bool, exponent: i64, magnitude: &'a [u8]) -> Self {
        Self {
            negative,
            exponent,
            magnitude,
        }
    }

    /// Returns whether the value is negative.
    pub(crate) fn is_negative(self) -> bool {
        self.negative
    }

    /// Returns whether the value denotes an integer.
    pub(crate) fn is_integral(self) -> bool {
        self.exponent >= 0
    }

    /// Returns the base-10 exponent of the value written as `mantissa * 10^exponent`,
    /// the mantissa not divisible by ten.
    pub(crate) fn exponent(self) -> i64 {
        self.exponent
    }

    /// Returns the little-endian bytes of the mantissa's magnitude.
    pub(crate) fn magnitude(self) -> &'a [u8] {
        self.magnitude
    }

    /// Returns the magnitude of the mantissa as a big integer.
    pub(crate) fn mantissa_magnitude(self) -> BigUint {
        BigUint::from_bytes_le(self.magnitude)
    }

    /// Returns the value as a 128-bit integer,
    /// or `None` for a fraction and beyond the 128-bit range.
    pub(crate) fn to_i128(self) -> Option<i128> {
        if !self.is_integral() || self.magnitude.len() > 16 {
            return None;
        }

        let mut bytes = [0; 16];
        bytes[..self.magnitude.len()].copy_from_slice(self.magnitude);
        let factor = 10u128.checked_pow(u32::try_from(self.exponent).ok()?)?;
        let magnitude = u128::from_le_bytes(bytes).checked_mul(factor)?;

        if self.negative {
            0i128.checked_sub_unsigned(magnitude)
        } else {
            i128::try_from(magnitude).ok()
        }
    }

    /// Returns the nearest double, or an infinity beyond the double range.
    pub(crate) fn to_f64(self) -> f64 {
        Lexical::nearest(self.negative, self.mantissa_magnitude(), self.exponent)
    }

    /// Returns the nearest float, or an infinity beyond the float range.
    pub(crate) fn to_f32(self) -> f32 {
        Lexical::nearest(self.negative, self.mantissa_magnitude(), self.exponent)
    }

    /// Clones the view into an owned [Wide].
    pub(crate) fn to_owned(self) -> Wide {
        Wide(Arc::new(Record {
            negative: self.negative,
            exponent: self.exponent,
            magnitude: self.magnitude.into(),
        }))
    }
}

impl fmt::Debug for WideRef<'_> {
    /// Writes the value in its canonical form.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self}")
    }
}

impl fmt::Display for WideRef<'_> {
    /// Writes the canonical `xsd:decimal` lexical form,
    /// which for an integral value is the `xsd:integer` form.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.negative {
            f.write_str("-")?;
        }
        Lexical::write_canonical(f, &self.mantissa_magnitude().to_string(), self.exponent)
    }
}

impl Ord for WideRef<'_> {
    fn cmp(&self, other: &Self) -> Ordering {
        match (self.negative, other.negative) {
            (false, true) => Ordering::Greater,
            (true, false) => Ordering::Less,
            (false, false) => self.cmp_magnitude(*other),
            (true, true) => other.cmp_magnitude(*self),
        }
    }
}

impl PartialOrd for WideRef<'_> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl WideRef<'_> {
    /// Compares the magnitudes of two values.
    ///
    /// Equal exponents compare the mantissas directly;
    /// the workspace aligns everything else.
    fn cmp_magnitude(self, other: Self) -> Ordering {
        if self.exponent == other.exponent {
            return cmp_magnitude(self.magnitude, other.magnitude);
        }
        workspace::Big::from_wide(self)
            .abs()
            .cmp(&workspace::Big::from_wide(other).abs())
    }
}

/// Compares two canonical little-endian magnitudes by value:
/// the longer one is larger,
/// and equal lengths compare from the most significant byte down.
pub(crate) fn cmp_magnitude(left: &[u8], right: &[u8]) -> Ordering {
    left.len()
        .cmp(&right.len())
        .then_with(|| left.iter().rev().cmp(right.iter().rev()))
}

/// Returns the canonical little-endian bytes of a magnitude:
/// no trailing zero bytes, and a single zero byte for zero.
pub(crate) fn magnitude_bytes(magnitude: &BigUint) -> Box<[u8]> {
    magnitude.to_bytes_le().into_boxed_slice()
}

/// An owned [`xsd:decimal`](https://www.w3.org/TR/xmlschema11-2/#decimal) value beyond the compact ranges:
/// an integer outside the 64-bit range,
/// or a non-integral decimal with more significant digits, or a smaller magnitude,
/// than a [Compact](super::Compact) holds.
/// Its borrowed view is [WideRef].
///
/// An integral value is an [`xsd:integer`](https://www.w3.org/TR/xmlschema11-2/#integer).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Wide(Arc<Record>);

/// The record behind a [Wide].
#[derive(Debug, PartialEq, Eq)]
struct Record {
    /// Whether the value is negative.
    negative: bool,
    /// The base-10 exponent.
    exponent: i64,
    /// The magnitude of the mantissa, little-endian, without trailing zero bytes.
    magnitude: Box<[u8]>,
}

impl Wide {
    /// Creates the owned form of a workspace value, which is normalized and bounded already,
    /// or `None` for a value the compact cases hold.
    pub(crate) fn new(value: &workspace::Big) -> Option<Self> {
        if value.to_i64().is_some() || value.to_compact().is_some() {
            return None;
        }
        Some(Self(Arc::new(Record {
            negative: value.mantissa().sign() == Sign::Minus,
            exponent: value.exponent(),
            magnitude: magnitude_bytes(value.mantissa().magnitude()),
        })))
    }

    /// Returns the borrowed view.
    pub(crate) fn as_ref(&self) -> WideRef<'_> {
        WideRef {
            negative: self.0.negative,
            exponent: self.0.exponent,
            magnitude: &self.0.magnitude,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::str::FromStr;

    use num_bigint::BigInt;

    impl Wide {
        /// Returns the value as a big integer, or `None` for a fraction.
        fn to_bigint(&self) -> Option<BigInt> {
            workspace::Big::from_wide(self.as_ref()).to_bigint()
        }
    }

    /// Parses the owned wide form of a number lexical.
    fn wide(lexical: &str) -> Wide {
        let (digits, exponent) = match lexical.split_once('.') {
            Some((integer, fraction)) => (
                format!("{integer}{fraction}"),
                -i64::try_from(fraction.len()).expect("short"),
            ),
            None => (lexical.to_owned(), 0),
        };
        let mantissa = BigInt::from_str(&digits).expect("well-formed");
        let value = workspace::Big::new(mantissa, exponent).expect("bounded");

        Wide::new(&value).expect("beyond the compact ranges")
    }

    #[test]
    fn the_compact_ranges_are_refused() {
        let integer =
            |value: i128| Wide::new(&workspace::Big::from_parts(value, 0).expect("bounded"));
        assert!(integer(i128::from(i64::MAX)).is_none());
        assert!(integer(i128::from(i64::MIN)).is_none());
        assert!(integer(i128::from(i64::MAX) + 1).is_some());
        assert!(integer(i128::from(i64::MIN) - 1).is_some());

        let fraction = |mantissa: i128, exponent: i64| {
            Wide::new(&workspace::Big::from_parts(mantissa, exponent).expect("bounded"))
        };
        assert!(fraction(15, -1).is_none());
        assert!(fraction(1_234_567_890_123_456, -1).is_none());
        assert!(fraction(12_345_678_901_234_567, -1).is_some());
        assert!(fraction(15, -300).is_some());
    }

    #[test]
    fn integral_values_store_the_normalized_record() {
        let power = wide("1000000000000000000000000000000");
        assert_eq!(power.as_ref().exponent(), 30);
        assert_eq!(power.as_ref().magnitude(), &[1]);
        assert!(power.as_ref().is_integral());

        let odd = wide("1000000000000000000000000000005");
        assert_eq!(odd.as_ref().exponent(), 0);
        assert_eq!(
            odd.to_bigint(),
            Some(BigInt::from_str("1000000000000000000000000000005").expect("well-formed"))
        );

        // The same digits with a negative exponent are a fraction.
        let fraction = wide("1.000000000000000000000000000005");
        assert_eq!(fraction.as_ref().exponent(), -30);
        assert!(!fraction.as_ref().is_integral());
        assert_ne!(fraction.as_ref(), odd.as_ref());
        assert_eq!(fraction.to_bigint(), None);
    }

    #[test]
    fn views_render_and_round_trip() {
        let value = wide("-123456789012345678901");
        assert_eq!(value.as_ref().to_string(), "-123456789012345678901");
        assert_eq!(value.as_ref().to_f64(), -1.2345678901234568e20);
        assert_eq!(
            wide("1.2345678901234567").as_ref().to_string(),
            "1.2345678901234567"
        );
        assert_eq!(
            wide("1.2345678901234567").as_ref().to_f64(),
            1.2345678901234567
        );
        let copy = value.as_ref().to_owned();
        assert_eq!(copy, value);
        assert_eq!(copy.as_ref(), value.as_ref());
    }

    #[test]
    fn order_is_the_value_order() {
        let ordered = [
            "-1234567890123456789012",
            "-123456789012345678901.5",
            "-123456789012345678901",
            "-1.2345678901234567",
            "0.000000000000000000000000000000000000012345678901234567",
            "1.2345678901234567",
            "1.2345678901234568",
            "123456789012345678901",
            "123456789012345678901.5",
            "1234567890123456789012",
        ]
        .map(wide);

        for window in ordered.windows(2) {
            assert!(
                window[0].as_ref() < window[1].as_ref(),
                "{:?} < {:?}",
                window[0],
                window[1]
            );
            assert_eq!(window[0].as_ref().cmp(&window[0].as_ref()), Ordering::Equal);
        }
    }
}
