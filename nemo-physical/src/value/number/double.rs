//! 64-bit floating point numbers.

use std::{
    cmp::Ordering,
    fmt,
    hash::{Hash, Hasher},
    ops::{Add, Div, Mul, Neg, Sub},
};

use crate::value::number::lexical::Lexical;

/// An [`xsd:double`](https://www.w3.org/TR/xmlschema11-2/#double) value:
/// a double-precision IEEE number in which all NaNs are one value.
///
/// Equality, order and hashing are term identity:
/// the NaN is identical to itself and above `INF`,
/// and `-0.0` is a term of its own directly below `0.0`.
/// XSD's value comparison is [value_eq](Self::value_eq) and [value_cmp](Self::value_cmp).
#[derive(Clone, Copy)]
pub struct Double(f64);

/// The value takes the bits of its number.
const _: () = assert!(size_of::<Double>() == 8);

impl Double {
    /// The bit pattern every NaN canonicalizes to, a quiet NaN.
    const CANONICAL_NAN: u64 = 0x7FF8_0000_0000_0000;

    /// The sign bit of the bit pattern.
    const SIGN_BIT: u64 = 1 << 63;

    /// Creates the value of a number, with every NaN replaced by the canonical one.
    pub fn new(value: f64) -> Self {
        if value.is_nan() {
            Self(f64::from_bits(Self::CANONICAL_NAN))
        } else {
            Self(value)
        }
    }

    /// Parses an `xsd:double` lexical form,
    /// a signed decimal with an optional exponent
    /// or one of `INF`, `+INF`, `-INF` and `NaN`,
    /// or `None` for malformed input.
    pub fn from_lexical(lexical: &str) -> Option<Self> {
        match lexical {
            "INF" | "+INF" => Some(Self(f64::INFINITY)),
            "-INF" => Some(Self(f64::NEG_INFINITY)),
            "NaN" => Some(Self::new(f64::NAN)),
            // The standard parser accepts more spellings than the XSD form.
            _ if Lexical::is_float_form(lexical) => lexical.parse().ok().map(Self),
            _ => None,
        }
    }

    /// Returns the number.
    pub fn get(self) -> f64 {
        self.0
    }

    /// Returns whether the value is neither infinite nor NaN.
    pub fn is_finite(self) -> bool {
        self.0.is_finite()
    }

    /// Returns whether the value is the NaN.
    pub fn is_nan(self) -> bool {
        self.0.is_nan()
    }

    /// Returns the absolute value, `fn:abs`.
    pub fn abs(self) -> Self {
        Self(self.0.abs())
    }

    /// Returns whether two values are equal in XSD's value space, `op:numeric-equal`:
    /// the NaN equals nothing, and the two zeros are equal.
    pub fn value_eq(self, other: Self) -> bool {
        self.0 == other.0
    }

    /// Returns the order of two values in XSD's value space, `op:numeric-less-than`,
    /// or `None` if either is the NaN.
    pub fn value_cmp(self, other: Self) -> Option<Ordering> {
        self.0.partial_cmp(&other.0)
    }
}

/// The hooks of the storage encoding.
impl Double {
    /// Returns an unsigned key whose order is the term order,
    /// and which two values share exactly if they are one term.
    ///
    /// A positive value gets its sign bit set and a negative one has all bits flipped,
    /// which maps the IEEE order onto the unsigned order.
    pub(crate) fn order_key(self) -> u64 {
        let bits = self.0.to_bits();
        if bits & Self::SIGN_BIT == 0 {
            bits | Self::SIGN_BIT
        } else {
            !bits
        }
    }

    /// Decodes the value of an order key.
    pub(crate) fn from_order_key(key: u64) -> Self {
        Self(f64::from_bits(if key & Self::SIGN_BIT != 0 {
            key ^ Self::SIGN_BIT
        } else {
            !key
        }))
    }
}

impl From<f64> for Double {
    fn from(value: f64) -> Self {
        Self::new(value)
    }
}

impl Add for Double {
    type Output = Self;

    /// Returns the IEEE sum, `op:numeric-add`.
    fn add(self, other: Self) -> Self {
        Self::new(self.0 + other.0)
    }
}

impl Sub for Double {
    type Output = Self;

    /// Returns the IEEE difference, `op:numeric-subtract`.
    fn sub(self, other: Self) -> Self {
        Self::new(self.0 - other.0)
    }
}

impl Mul for Double {
    type Output = Self;

    /// Returns the IEEE product, `op:numeric-multiply`.
    fn mul(self, other: Self) -> Self {
        Self::new(self.0 * other.0)
    }
}

impl Div for Double {
    type Output = Self;

    /// Returns the IEEE quotient, `op:numeric-divide`; a zero divisor gives an infinity or the NaN.
    fn div(self, other: Self) -> Self {
        Self::new(self.0 / other.0)
    }
}

impl Neg for Double {
    type Output = Self;

    /// Returns the negated value, `op:numeric-unary-minus`.
    fn neg(self) -> Self {
        Self::new(-self.0)
    }
}

impl PartialEq for Double {
    fn eq(&self, other: &Self) -> bool {
        self.0.to_bits() == other.0.to_bits()
    }
}

impl Eq for Double {}

impl Ord for Double {
    fn cmp(&self, other: &Self) -> Ordering {
        self.order_key().cmp(&other.order_key())
    }
}

impl PartialOrd for Double {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Hash for Double {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.0.to_bits().hash(state);
    }
}

impl fmt::Debug for Double {
    /// Writes the value in its lexical form.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self}")
    }
}

impl fmt::Display for Double {
    /// Writes the `xsd:double` lexical form:
    /// the shortest digits that read back as the value,
    /// or `INF`, `-INF` and `NaN`.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.0.is_nan() {
            f.write_str("NaN")
        } else if self.0.is_finite() {
            write!(f, "{}", self.0)
        } else if self.0.is_sign_positive() {
            f.write_str("INF")
        } else {
            f.write_str("-INF")
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::hash::{BuildHasher, RandomState};

    #[test]
    fn keys_order_the_xsd_value_space() {
        // NaN above +INF, `-0.0` below `+0.0`.
        let values = [
            f64::NEG_INFINITY,
            -1e100,
            -1.5,
            -f64::MIN_POSITIVE,
            -0.0,
            0.0,
            f64::MIN_POSITIVE,
            1.5,
            1e100,
            f64::INFINITY,
            f64::NAN,
        ]
        .map(Double::new);

        for window in values.windows(2) {
            assert!(
                window[0].order_key() < window[1].order_key(),
                "{} < {}",
                window[0],
                window[1]
            );
            assert!(window[0] < window[1]);
        }
    }

    #[test]
    fn keys_round_trip_and_collapse_nans() {
        for value in [f64::NEG_INFINITY, -1.5, -0.0, 0.0, 1.5, f64::INFINITY] {
            let value = Double::new(value);
            assert_eq!(Double::from_order_key(value.order_key()), value);
        }

        let payload_nan = Double::new(f64::from_bits(0x7FF0_0000_0000_1234));

        assert_eq!(Double::new(f64::NAN), Double::new(-f64::NAN));
        assert_eq!(Double::new(f64::NAN), payload_nan);
        assert_eq!(Double::new(f64::NAN).order_key(), payload_nan.order_key());
        assert!(Double::from_order_key(payload_nan.order_key()).is_nan());
        assert_ne!(Double::new(-0.0), Double::new(0.0));

        let state = RandomState::new();

        assert_eq!(
            state.hash_one(Double::new(f64::NAN)),
            state.hash_one(payload_nan)
        );
    }

    #[test]
    fn special_values_spell_as_xsd_does() {
        assert_eq!(Double::new(f64::INFINITY).to_string(), "INF");
        assert_eq!(Double::new(f64::NEG_INFINITY).to_string(), "-INF");
        assert_eq!(Double::new(f64::NAN).to_string(), "NaN");
        assert_eq!(Double::new(2.5).to_string(), "2.5");
        assert_eq!(
            Double::new(1e100).to_string(),
            format!("1{}", "0".repeat(100))
        );

        for value in [f64::INFINITY, f64::NEG_INFINITY, f64::NAN, 2.5, -0.0] {
            let value = Double::new(value);
            assert_eq!(Double::from_lexical(&value.to_string()), Some(value));
        }
    }

    #[test]
    fn lexicals_take_the_xsd_form_only() {
        assert_eq!(
            Double::from_lexical("+INF"),
            Some(Double::new(f64::INFINITY))
        );
        assert_eq!(Double::from_lexical("-1.5E3"), Some(Double::new(-1500.0)));
        assert_eq!(Double::from_lexical("1e-3"), Some(Double::new(0.001)));

        for lexical in [
            "nan", "inf", "Infinity", "+inf", "1e", "e5", ".", "0x1", " 1", "",
        ] {
            assert_eq!(Double::from_lexical(lexical), None, "{lexical:?}");
        }
    }

    #[test]
    fn operators_canonicalize_and_values_compare_by_ieee() {
        let inf = Double::new(f64::INFINITY);

        assert_eq!(inf - inf, Double::new(f64::NAN));
        assert_eq!(inf * Double::new(0.0), Double::new(f64::NAN));
        assert_eq!(-Double::new(0.0), Double::new(-0.0));
        assert_eq!(Double::new(-1.0) / Double::new(0.0), -inf);
        assert!(Double::new(-0.0).value_eq(Double::new(0.0)));
        assert!(!Double::new(f64::NAN).value_eq(Double::new(f64::NAN)));
        assert_eq!(Double::new(f64::NAN).value_cmp(Double::new(1.0)), None);
        assert_eq!(
            Double::new(2.0).value_cmp(Double::new(1.5)),
            Some(Ordering::Greater)
        );
        assert_eq!(Double::new(-2.5).abs(), Double::new(2.5));
    }
}
