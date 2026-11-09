//! 64-bit floating-point numbers.

use std::{
    cmp::Ordering,
    fmt,
    hash::{Hash, Hasher},
    ops::{Add, Div, Mul, Neg, Sub},
};

use crate::value::number::lexical::{Lexical, scientific, write_float};

/// An [`xsd:double`](https://www.w3.org/TR/xmlschema11-2/#double) value,
/// a double-precision IEEE 754 number in which all NaNs are one value.
///
/// Equality, order and hashing follow term identity:
/// NaN is identical to itself and lies above `INF`,
/// and `-0.0` is a term of its own, directly below `0.0`.
/// XSD's comparison by value is [value_eq](Self::value_eq) and [value_cmp](Self::value_cmp).
#[derive(Clone, Copy)]
pub struct Double(f64);

/// Checks at compile time that a value occupies exactly the 8 bytes of its number.
const _: () = assert!(size_of::<Double>() == 8);

impl Double {
    /// The bit pattern of the canonical NaN, a quiet NaN, which replaces every other NaN.
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

    /// Parses an `xsd:double` lexical form, or returns `None` for a malformed lexical form.
    ///
    /// The lexical form is a decimal with an optional sign and an optional exponent,
    /// or one of `INF`, `+INF`, `-INF` and `NaN`.
    pub fn from_lexical(lexical: &str) -> Option<Self> {
        match lexical {
            "INF" | "+INF" => Some(Self(f64::INFINITY)),
            "-INF" => Some(Self(f64::NEG_INFINITY)),
            "NaN" => Some(Self::new(f64::NAN)),
            // The standard parser accepts more spellings than XSD, so the form is checked first.
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

    /// Returns whether the value is NaN.
    pub fn is_nan(self) -> bool {
        self.0.is_nan()
    }

    /// Returns the absolute value, `fn:abs`.
    pub fn abs(self) -> Self {
        Self(self.0.abs())
    }

    /// Returns whether two values are equal by value, `op:numeric-equal`.
    ///
    /// NaN equals nothing, and the two zeros are equal.
    pub fn value_eq(self, other: Self) -> bool {
        self.0 == other.0
    }

    /// Returns the order of two values by value, `op:numeric-less-than`,
    /// or `None` if either is NaN.
    pub fn value_cmp(self, other: Self) -> Option<Ordering> {
        self.0.partial_cmp(&other.0)
    }
}

/// The order keys of the storage encoding.
impl Double {
    /// Returns an unsigned key whose order is the term order,
    /// and which two values share exactly if they are the same term.
    ///
    /// A value with a clear sign bit gets the bit set,
    /// and a value with a set sign bit has all its bits flipped.
    /// This maps the IEEE order onto the order of unsigned integers.
    pub(crate) fn order_key(self) -> u64 {
        let bits = self.0.to_bits();
        if bits & Self::SIGN_BIT == 0 {
            bits | Self::SIGN_BIT
        } else {
            !bits
        }
    }

    /// Returns the value of an order key.
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

    /// Returns the IEEE quotient, `op:numeric-divide`.
    ///
    /// A zero divisor gives an infinity or NaN.
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
    /// Writes the value as its `Display` does.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self}")
    }
}

impl Double {
    /// Returns XSD's canonical lexical form of the value,
    /// such as `2.5E0`, `1.0E3`, `INF`, `-INF` or `NaN`.
    ///
    /// [Display](fmt::Display) writes the value as XPath casts it to a string instead.
    pub fn canonical_lexical(self) -> String {
        if self.0.is_finite() {
            scientific(self.0)
        } else {
            self.to_string()
        }
    }
}

impl fmt::Display for Double {
    /// Writes the value as XPath casts it to a string,
    /// which is one of the `xsd:double` lexical forms.
    ///
    /// - A magnitude from `0.000001` to below `1000000`, and zero,
    ///   is written in the shortest plain digits, such as `5.5`, `1000`, `0` or `-0`.
    /// - Any other finite value is written in XSD's canonical scientific form,
    ///   such as `1.0E100`.
    /// - The special values are written `INF`, `-INF` and `NaN`.
    ///
    /// The source is the casting to `xs:string` in XPath and XQuery Functions and Operators.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.0.is_nan() {
            f.write_str("NaN")
        } else if self.0.is_finite() {
            write_float(f, self.0)
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

    use crate::value::{NumberRef, ValueRef};

    /// Order keys and the term order sort doubles from `-INF` to `INF`,
    /// with `-0.0` below `0.0` and NaN at the top.
    #[test]
    fn keys_order_the_xsd_value_space() {
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

    /// Order keys convert back to their values, and every NaN has the same key, hash and value.
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

    /// Doubles are written as XPath casts them to strings, and canonically as XSD writes them,
    /// and the written forms are parsed back.
    #[test]
    fn special_values_spell_as_xsd_does() {
        assert_eq!(Double::new(f64::INFINITY).to_string(), "INF");
        assert_eq!(Double::new(f64::NEG_INFINITY).to_string(), "-INF");
        assert_eq!(Double::new(f64::NAN).to_string(), "NaN");
        // XPath casts a double to a string in plain digits within a middle range of magnitudes,
        // and in the canonical scientific form outside it.
        assert_eq!(Double::new(2.5).to_string(), "2.5");
        assert_eq!(Double::new(2.0).to_string(), "2");
        assert_eq!(Double::new(1000.0).to_string(), "1000");
        assert_eq!(Double::new(-0.1).to_string(), "-0.1");
        assert_eq!(Double::new(0.0).to_string(), "0");
        assert_eq!(Double::new(-0.0).to_string(), "-0");
        assert_eq!(Double::new(0.000001).to_string(), "0.000001");
        assert_eq!(Double::new(0.0000009).to_string(), "9.0E-7");
        assert_eq!(Double::new(999999.5).to_string(), "999999.5");
        assert_eq!(Double::new(1e6).to_string(), "1.0E6");
        assert_eq!(Double::new(1e100).to_string(), "1.0E100");
        assert_eq!(Double::new(1.25e-300).to_string(), "1.25E-300");

        // XSD's canonical form has one digit before the point, at least one after it,
        // and an exponent.
        assert_eq!(Double::new(2.5).canonical_lexical(), "2.5E0");
        assert_eq!(Double::new(1000.0).canonical_lexical(), "1.0E3");
        assert_eq!(Double::new(0.0).canonical_lexical(), "0.0E0");
        assert_eq!(Double::new(f64::NAN).canonical_lexical(), "NaN");

        // Term syntax writes the same digits before its suffix.
        let term = |value: f64| ValueRef::Number(NumberRef::Double(Double::new(value))).to_string();
        assert_eq!(term(2.5), "2.5d");
        assert_eq!(term(2.0), "2d");
        assert_eq!(term(1e100), "1.0E100d");

        for value in [
            f64::INFINITY,
            f64::NEG_INFINITY,
            f64::NAN,
            2.5,
            -0.0,
            1e100,
            -0.1,
        ] {
            let value = Double::new(value);
            assert_eq!(Double::from_lexical(&value.to_string()), Some(value));
        }
    }

    /// Only XSD's lexical forms of doubles are parsed.
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

    /// The operators canonicalize NaN, and comparison by value follows IEEE 754.
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
