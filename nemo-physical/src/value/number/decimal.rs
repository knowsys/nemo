//! Exact numbers, owned and borrowed.
//!
//! Each value is stored in one of three representations, chosen by its range:
//! - an integer within the 64-bit range;
//! - a [compact](Compact) decimal that is not integral;
//! - a [wide](Wide) number beyond both.
//!
//! Arithmetic computes in the [workspaces](workspace).

mod arithmetic;
pub(crate) mod bitwise;
mod compact;
mod wide;
pub(crate) mod workspace;

use std::{
    cmp::Ordering,
    fmt,
    hash::{Hash, Hasher},
};

use crate::value::number::lexical::Parsed;

pub(crate) use compact::Compact;
pub(crate) use wide::{Wide, WideRef};

use arithmetic::big;

/// An owned [`xsd:decimal`](https://www.w3.org/TR/xmlschema11-2/#decimal) value,
/// an exact number.
///
/// Its integral values are the [`xsd:integer`](https://www.w3.org/TR/xmlschema11-2/#integer)
/// values.
/// Its borrowed view is [DecimalRef].
///
/// An integral value is always an integer, so `1.0` is `1`.
/// Values are ordered by domain, the integers first, and then by value.
///
/// The canonical form of a value has at most 1,000,000 characters, not counting its sign.
/// This is the *length limit* of numbers: creating a value or computing one fails beyond it.
#[derive(Clone)]
pub struct Decimal(Repr);

/// The representation of an owned exact number, chosen by the range of its value.
#[derive(Clone)]
enum Repr {
    /// An integer within the 64-bit range.
    Int(i64),
    /// A non-integral decimal within the compact range.
    Compact(Compact),
    /// An integer or a decimal beyond the compact ranges.
    Wide(Wide),
}

/// Checks at compile time that an owned exact number occupies at most 24 bytes.
const _: () = assert!(size_of::<Decimal>() <= 24);

impl Decimal {
    /// Creates the number `mantissa * 10^exponent`,
    /// or `None` for a value beyond the length limit.
    ///
    /// # Example
    ///
    /// ```
    /// use nemo_physical::value::Decimal;
    ///
    /// let price = Decimal::new(150, -2).unwrap();
    /// assert_eq!(price.to_string(), "1.5");
    /// assert_eq!(Decimal::new(1500, -2), Some(Decimal::from(15)));
    /// ```
    pub fn new(mantissa: i128, exponent: i64) -> Option<Self> {
        // Most values are integers within the 64-bit range, which need no normalization.
        if exponent == 0
            && let Ok(integer) = i64::try_from(mantissa)
        {
            return Some(Self(Repr::Int(integer)));
        }

        let normalized = workspace::Fixed::new(mantissa, exponent).normalized();
        let (mantissa, exponent) = (normalized.mantissa(), normalized.exponent());

        if exponent >= 0 {
            let integer = u32::try_from(exponent)
                .ok()
                .and_then(|exponent| 10i128.checked_pow(exponent))
                .and_then(|factor| mantissa.checked_mul(factor))
                .and_then(|integer| i64::try_from(integer).ok());

            if let Some(integer) = integer {
                return Some(Self(Repr::Int(integer)));
            }
        } else if let Ok(compact) = i64::try_from(mantissa)
            && let Ok(exponent) = i32::try_from(exponent)
            && let Some(value) = Compact::new(compact, exponent)
        {
            return Some(Self(Repr::Compact(value)));
        }

        workspace::Big::from_fixed(normalized).map(Self::from)
    }

    /// Creates a number from an `xsd:decimal` lexical form,
    /// or `None` for a malformed lexical form or a value beyond the length limit.
    ///
    /// Every `xsd:integer` lexical form is also an `xsd:decimal` lexical form.
    pub fn from_lexical(lexical: &str) -> Option<Self> {
        match Parsed::parse(lexical)? {
            Parsed::Zero => Some(Self(Repr::Int(0))),
            Parsed::Small {
                negative,
                magnitude,
                exponent,
            } => {
                let mantissa = i128::from(magnitude);
                Self::new(if negative { -mantissa } else { mantissa }, exponent)
            }
            Parsed::Big {
                negative,
                digits,
                exponent,
            } => workspace::Big::from_digits(negative, &digits, exponent).map(Self::from),
        }
    }

    /// Returns the borrowed view of this value.
    pub fn as_ref(&self) -> DecimalRef<'_> {
        DecimalRef(match &self.0 {
            Repr::Int(value) => ReprRef::Int(*value),
            Repr::Compact(value) => ReprRef::Compact(*value),
            Repr::Wide(value) => ReprRef::Wide(value.as_ref()),
        })
    }
}

impl From<i64> for Decimal {
    fn from(value: i64) -> Self {
        Self(Repr::Int(value))
    }
}

impl From<workspace::Big> for Decimal {
    /// Converts a workspace value, which is within the length limit,
    /// into the representation for its range.
    fn from(value: workspace::Big) -> Self {
        if let Some(integer) = value.to_i64() {
            return Self(Repr::Int(integer));
        }
        if let Some(compact) = value.to_compact() {
            return Self(Repr::Compact(compact));
        }
        let wide = Wide::new(&value).expect("the compact cases do not hold the value");
        Self(Repr::Wide(wide))
    }
}

impl PartialEq for Decimal {
    fn eq(&self, other: &Self) -> bool {
        self.as_ref() == other.as_ref()
    }
}

impl Eq for Decimal {}

impl Ord for Decimal {
    fn cmp(&self, other: &Self) -> Ordering {
        self.as_ref().cmp(&other.as_ref())
    }
}

impl PartialOrd for Decimal {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Hash for Decimal {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.as_ref().hash(state);
    }
}

impl fmt::Debug for Decimal {
    /// Writes the value in its canonical form.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self}")
    }
}

impl fmt::Display for Decimal {
    /// Writes the canonical `xsd:decimal` lexical form,
    /// which is the `xsd:integer` lexical form for an integral value.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_ref())
    }
}

/// A borrowed [`xsd:decimal`](https://www.w3.org/TR/xmlschema11-2/#decimal) value,
/// an exact number.
///
/// Its integral values are the [`xsd:integer`](https://www.w3.org/TR/xmlschema11-2/#integer)
/// values.
/// Its owned form is [Decimal], which also states the length limit of numbers.
///
/// An integral value is always an integer, so `1.0` is `1`.
/// Values are ordered by domain, the integers first, and then by value.
#[derive(Clone, Copy)]
pub struct DecimalRef<'a>(pub(crate) ReprRef<'a>);

/// The representation of a borrowed exact number, chosen by the range of its value.
///
/// Every value has exactly one representation:
/// an integer within the 64-bit range is never wide,
/// and neither is a decimal within the compact range.
#[derive(Clone, Copy)]
pub(crate) enum ReprRef<'a> {
    /// An integer within the 64-bit range.
    Int(i64),
    /// A non-integral decimal within the compact range.
    Compact(Compact),
    /// An integer or a decimal beyond the compact ranges.
    Wide(WideRef<'a>),
}

impl<'a> DecimalRef<'a> {
    /// Returns whether the value is an integer.
    pub fn is_integral(self) -> bool {
        match self.0 {
            ReprRef::Int(_) => true,
            ReprRef::Compact(_) => false,
            ReprRef::Wide(value) => value.is_integral(),
        }
    }

    /// Returns whether the value is below zero.
    pub fn is_negative(self) -> bool {
        match self.0 {
            ReprRef::Int(value) => value < 0,
            ReprRef::Compact(value) => value.mantissa() < 0,
            ReprRef::Wide(value) => value.is_negative(),
        }
    }

    /// Returns whether the value is zero.
    pub(crate) fn is_zero(self) -> bool {
        // Zero is an integer within the 64-bit range.
        matches!(self.0, ReprRef::Int(0))
    }

    /// Returns whether the value is an odd integer.
    pub(crate) fn is_odd(self) -> bool {
        match self.0 {
            ReprRef::Int(value) => value % 2 != 0,
            // A wide integer with a positive exponent is a multiple of ten.
            ReprRef::Wide(value) => {
                value.exponent() == 0 && value.magnitude().first().is_some_and(|byte| byte % 2 == 1)
            }
            ReprRef::Compact(_) => false,
        }
    }

    /// Returns the value as a 64-bit integer,
    /// or `None` for a value that is not integral or lies beyond the 64-bit range.
    pub fn to_i64(self) -> Option<i64> {
        match self.0 {
            ReprRef::Int(value) => Some(value),
            // A wide integer lies beyond the 64-bit range.
            ReprRef::Compact(_) | ReprRef::Wide(_) => None,
        }
    }

    /// Returns the value as a 128-bit integer,
    /// or `None` for a value that is not integral or lies beyond the 128-bit range.
    pub fn to_i128(self) -> Option<i128> {
        match self.0 {
            ReprRef::Int(value) => Some(i128::from(value)),
            ReprRef::Compact(_) => None,
            ReprRef::Wide(value) => value.to_i128(),
        }
    }

    /// Returns the nearest double, or an infinity beyond the double range.
    pub fn to_f64(self) -> f64 {
        match self.0 {
            ReprRef::Int(value) => value as f64,
            ReprRef::Compact(value) => value.to_f64(),
            ReprRef::Wide(value) => value.to_f64(),
        }
    }

    /// Returns the nearest float, or an infinity beyond the float range.
    pub(crate) fn to_f32(self) -> f32 {
        match self.0 {
            ReprRef::Int(value) => value as f32,
            ReprRef::Compact(value) => value.to_f32(),
            ReprRef::Wide(value) => value.to_f32(),
        }
    }

    /// Returns an owned copy of the value.
    pub fn to_owned(self) -> Decimal {
        Decimal(match self.0 {
            ReprRef::Int(value) => Repr::Int(value),
            ReprRef::Compact(value) => Repr::Compact(value),
            ReprRef::Wide(value) => Repr::Wide(value.to_owned()),
        })
    }

    /// Returns the order of two values by their mathematical values.
    pub(crate) fn value_cmp(self, other: Self) -> Ordering {
        if let (ReprRef::Int(left), ReprRef::Int(right)) = (self.0, other.0) {
            return left.cmp(&right);
        }
        match (workspace::Fixed::of(self), workspace::Fixed::of(other)) {
            (Some(left), Some(right)) => left.cmp(right),
            _ => big(self).cmp(&big(other)),
        }
    }

    /// Compares this value with a double by their mathematical values,
    /// or returns `None` if the double is NaN.
    pub(crate) fn floating_cmp(self, floating: f64) -> Option<Ordering> {
        if floating.is_nan() {
            return None;
        }
        if floating.is_infinite() {
            return Some(if floating > 0.0 {
                Ordering::Less
            } else {
                Ordering::Greater
            });
        }

        // The nearest double never decreases as the value grows.
        // So if it differs from the double, it orders the two,
        // and otherwise their exact values decide.
        let nearest = self.to_f64();
        if nearest != floating {
            return nearest.partial_cmp(&floating);
        }
        Some(match self.0 {
            // If the double is the nearest double of an integer, the double is integral too.
            // Below 2^53 the two are equal, and from 2^53 on every double is an integer.
            // So the cast is exact, and its magnitude of at most 2^63 fits into an `i128`.
            #[allow(clippy::cast_possible_truncation)]
            ReprRef::Int(value) => i128::from(value).cmp(&(floating as i128)),
            _ => big(self).cmp(&workspace::Big::from_finite_double(floating)),
        })
    }
}

impl From<i64> for DecimalRef<'_> {
    fn from(value: i64) -> Self {
        Self(ReprRef::Int(value))
    }
}

impl PartialEq for DecimalRef<'_> {
    fn eq(&self, other: &Self) -> bool {
        // Each value has one representation, so only values in the same one can be equal.
        match (self.0, other.0) {
            (ReprRef::Int(left), ReprRef::Int(right)) => left == right,
            (ReprRef::Compact(left), ReprRef::Compact(right)) => left == right,
            (ReprRef::Wide(left), ReprRef::Wide(right)) => left == right,
            _ => false,
        }
    }
}

impl Eq for DecimalRef<'_> {}

impl Ord for DecimalRef<'_> {
    fn cmp(&self, other: &Self) -> Ordering {
        match (self.0, other.0) {
            (ReprRef::Int(left), ReprRef::Int(right)) => left.cmp(&right),
            (ReprRef::Compact(left), ReprRef::Compact(right)) => left.cmp(&right),
            (ReprRef::Wide(left), ReprRef::Wide(right))
                if left.is_integral() == right.is_integral() =>
            {
                left.cmp(&right)
            }
            // A wide integer lies beyond the 64-bit range,
            // so its sign alone orders it against a 64-bit integer.
            (ReprRef::Int(_), ReprRef::Wide(right)) if right.is_integral() => {
                if right.is_negative() {
                    Ordering::Greater
                } else {
                    Ordering::Less
                }
            }
            (ReprRef::Wide(left), ReprRef::Int(_)) if left.is_integral() => {
                if left.is_negative() {
                    Ordering::Less
                } else {
                    Ordering::Greater
                }
            }
            // A compact and a wide decimal are rarely compared.
            // Their signs decide if they differ, and the workspace compares the rest exactly.
            (ReprRef::Compact(_), ReprRef::Wide(right)) if !right.is_integral() => {
                self.cmp_signs_then_values(*other)
            }
            (ReprRef::Wide(left), ReprRef::Compact(_)) if !left.is_integral() => {
                self.cmp_signs_then_values(*other)
            }
            // The integers come before the other decimals.
            _ => other.is_integral().cmp(&self.is_integral()),
        }
    }
}

impl PartialOrd for DecimalRef<'_> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl DecimalRef<'_> {
    /// Compares two values by their signs, and by their exact values if those agree.
    fn cmp_signs_then_values(self, other: Self) -> Ordering {
        match (self.is_negative(), other.is_negative()) {
            (false, true) => Ordering::Greater,
            (true, false) => Ordering::Less,
            _ => big(self).cmp(&big(other)),
        }
    }
}

impl Hash for DecimalRef<'_> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        // Each value has one representation, so each representation hashes its own form.
        match self.0 {
            ReprRef::Int(value) => value.hash(state),
            ReprRef::Compact(value) => value.hash(state),
            ReprRef::Wide(value) => value.hash(state),
        }
    }
}

impl fmt::Debug for DecimalRef<'_> {
    /// Writes the value in its canonical form.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self}")
    }
}

impl fmt::Display for DecimalRef<'_> {
    /// Writes the canonical `xsd:decimal` lexical form,
    /// which is the `xsd:integer` lexical form for an integral value.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.0 {
            ReprRef::Int(value) => write!(f, "{value}"),
            ReprRef::Compact(value) => write!(f, "{value}"),
            ReprRef::Wide(value) => write!(f, "{value}"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Returns the value of a well-formed lexical form.
    fn value(lexical: &str) -> Decimal {
        Decimal::from_lexical(lexical).expect("well-formed")
    }

    /// Returns whether a value is an integer in the wide representation.
    fn is_wide_integer(value: &Decimal) -> bool {
        matches!(&value.0, Repr::Wide(wide) if wide.as_ref().is_integral())
    }

    /// Creating a value stores it in the representation for its range.
    #[test]
    fn construction_routes_to_the_case_that_holds_the_value() {
        // A value that is not integral is a compact decimal.
        assert!(matches!(
            Decimal::new(15, -1),
            Some(Decimal(Repr::Compact(_)))
        ));

        // An integral value within the 64-bit range is a 64-bit integer.
        assert_eq!(Decimal::new(1500, -2), Some(Decimal::from(15)));
        assert_eq!(
            Decimal::new(i128::from(i64::MIN), 0),
            Some(Decimal::from(i64::MIN))
        );

        // An integral value beyond the 64-bit range is a wide integer.
        let big = Decimal::new(5, 30).expect("bounded");
        assert!(is_wide_integer(&big));
        assert_eq!(big, value("5000000000000000000000000000000"));
        assert_eq!(
            Decimal::new(i128::from(i64::MAX) + 1, 0),
            Some(value("9223372036854775808"))
        );

        // A compact mantissa with an extreme exponent is a wide decimal.
        let tiny = Decimal::new(15, -300).expect("bounded");
        assert!(matches!(tiny.0, Repr::Wide(_)));
        assert!(!is_wide_integer(&tiny));
        assert_eq!(Decimal::new(1, i64::from(i32::MAX)), None);
    }

    /// Parsing a lexical form gives the same value as creating it from mantissa and exponent.
    #[test]
    fn lexical_construction_agrees_with_direct_construction() {
        assert_eq!(value("42"), Decimal::from(42));
        assert_eq!(Some(value("-1.50")), Decimal::new(-15, -1));
        assert_eq!(value("-9223372036854775808"), Decimal::from(i64::MIN));

        // Exponent notation is a lexical form of floats, not of decimals.
        assert_eq!(Decimal::from_lexical("1e5"), None);

        // A decimal with seventeen digits is wide, however it is created.
        let wide = value("1.2345678901234567");
        assert!(matches!(wide.0, Repr::Wide(_)));
        assert_eq!(Some(wide), Decimal::new(12_345_678_901_234_567, -16));

        // A wide integer is normalized, however it is created.
        assert_eq!(
            Some(value("100000000000000000000000000000")),
            Decimal::new(1, 29)
        );
    }

    /// An integer converts to `i64` or `i128` exactly if it lies within the type's range.
    #[test]
    fn integers_convert_within_their_range() {
        assert_eq!(value("42").as_ref().to_i64(), Some(42));
        assert_eq!(value("42").as_ref().to_i128(), Some(42));
        assert_eq!(value("9223372036854775808").as_ref().to_i64(), None);
        assert_eq!(
            value("9223372036854775808").as_ref().to_i128(),
            Some(9_223_372_036_854_775_808)
        );
        assert_eq!(
            value("-170141183460469231731687303715884105728")
                .as_ref()
                .to_i128(),
            Some(i128::MIN)
        );
        assert_eq!(
            value("170141183460469231731687303715884105728")
                .as_ref()
                .to_i128(),
            None
        );
        assert_eq!(
            value("100000000000000000000000000000000000000")
                .as_ref()
                .to_i128(),
            Some(10i128.pow(38))
        );
        assert_eq!(
            value("1000000000000000000000000000000000000000")
                .as_ref()
                .to_i128(),
            None
        );
        assert_eq!(value("1.5").as_ref().to_i64(), None);
        assert_eq!(value("1.5").as_ref().to_i128(), None);
    }

    /// The integers come before the decimals that are not integral,
    /// and each group is ordered by value.
    #[test]
    fn integers_order_before_the_other_decimals() {
        let ordered = [
            "-123456789012345678901",
            "-1",
            "2",
            "123456789012345678901",
            "-123456789012345678901.5",
            "-1.5",
            "0.5",
            "123456789012345678901.5",
        ]
        .map(value);

        for window in ordered.windows(2) {
            assert!(window[0] < window[1], "{} < {}", window[0], window[1]);
        }
    }

    /// A value in each representation is unchanged by a round trip through its view.
    #[test]
    fn views_round_trip() {
        for value in [
            value("-42"),
            value("1.5"),
            value("123456789012345678901"),
            value("123456789012345678901.5"),
        ] {
            assert_eq!(value.as_ref().to_owned(), value);
        }
    }
}
