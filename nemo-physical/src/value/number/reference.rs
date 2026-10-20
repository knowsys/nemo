//! The view over every number: its domain, term identity and lexical form.

use std::{
    cmp::Ordering,
    fmt,
    hash::{Hash, Hasher},
};

use crate::value::{
    ValueDomain,
    number::{DecimalRef, Double, Float, Number},
};

/// A borrowed number of any of the four numeric domains:
/// integer, decimal, float and double.
/// Its owned form is [Number].
///
/// The domains are distinct: `1`, `1.0f` and `1.0d` are three values,
/// while `1.0` is the integer `1`.
/// XSD's value comparison, under which the three are equal,
/// is [value_eq](Self::value_eq) and [value_cmp](Self::value_cmp).
///
/// The arithmetic promotes the operands along the ladder
/// integer, decimal, float, double to the smallest type holding both.
#[derive(Clone, Copy)]
pub enum NumberRef<'a> {
    /// An exact number, an `xsd:decimal` value;
    /// the integers are its integral values.
    Decimal(DecimalRef<'a>),
    /// A 32-bit float, an `xsd:float` value.
    Float(Float),
    /// A 64-bit float, an `xsd:double` value.
    Double(Double),
}

impl<'a> NumberRef<'a> {
    /// Returns the domain of the value.
    pub fn domain(self) -> ValueDomain {
        match self {
            NumberRef::Decimal(value) if value.is_integral() => ValueDomain::Integer,
            NumberRef::Decimal(_) => ValueDomain::Decimal,
            NumberRef::Float(_) => ValueDomain::Float,
            NumberRef::Double(_) => ValueDomain::Double,
        }
    }

    /// Returns whether the value is an integer, a value of the integer domain.
    /// A float without a fraction is not one.
    pub fn is_integral(self) -> bool {
        matches!(self, NumberRef::Decimal(value) if value.is_integral())
    }

    /// Returns whether the value is below zero, which the NaN and the negative zero are not.
    pub fn is_negative(self) -> bool {
        match self {
            NumberRef::Decimal(value) => value.is_negative(),
            NumberRef::Float(value) => value.get() < 0.0,
            NumberRef::Double(value) => value.get() < 0.0,
        }
    }

    /// Returns whether the value is zero, either floating zero included.
    pub fn is_zero(self) -> bool {
        match self {
            NumberRef::Decimal(value) => value.is_zero(),
            NumberRef::Float(value) => value.get() == 0.0,
            NumberRef::Double(value) => value.get() == 0.0,
        }
    }

    /// Returns whether the value is the NaN of the floats or of the doubles.
    pub fn is_nan(self) -> bool {
        match self {
            NumberRef::Decimal(_) => false,
            NumberRef::Float(value) => value.is_nan(),
            NumberRef::Double(value) => value.is_nan(),
        }
    }

    /// Returns whether the value is an odd integer, a value of the integer domain.
    pub(crate) fn is_odd(self) -> bool {
        matches!(self, NumberRef::Decimal(value) if value.is_odd())
    }

    /// Returns the value as a 64-bit integer,
    /// or `None` for every other value, a float without a fraction included.
    pub fn to_i64(self) -> Option<i64> {
        match self {
            NumberRef::Decimal(value) => value.to_i64(),
            NumberRef::Float(_) | NumberRef::Double(_) => None,
        }
    }

    /// Returns the nearest double, or an infinity beyond the double range.
    pub fn to_f64(self) -> f64 {
        match self {
            NumberRef::Decimal(value) => value.to_f64(),
            NumberRef::Float(value) => value.to_f64(),
            NumberRef::Double(value) => value.get(),
        }
    }

    /// Returns the nearest float, or an infinity beyond the float range.
    ///
    /// The value rounds once:
    /// through the nearest double it would round twice,
    /// and miss if the double lands midway between two floats.
    pub fn to_f32(self) -> f32 {
        match self {
            NumberRef::Decimal(value) => value.to_f32(),
            NumberRef::Float(value) => value.get(),
            // Demotion to the narrower type is the intended rounding.
            #[allow(clippy::cast_possible_truncation)]
            NumberRef::Double(value) => value.get() as f32,
        }
    }

    /// Returns whether two values are equal in XSD's value space, `op:numeric-equal`,
    /// by their mathematical values as [value_cmp](Self::value_cmp) compares them:
    /// `1`, `1.0f` and `1.0d` are equal, `0.3` and `0.3d` are not,
    /// and the NaN equals nothing.
    pub fn value_eq(self, other: Self) -> bool {
        self.value_cmp(other) == Some(Ordering::Equal)
    }

    /// Returns the order of two values in XSD's value space, `op:numeric-less-than`,
    /// or `None` if either is the NaN.
    ///
    /// Values compare by their mathematical values, without rounding,
    /// as XPath 4.0 compares numbers of different types:
    /// a float or a double is the binary fraction it denotes,
    /// so `0.3` lies above `0.3d`, whose exact value is `0.2999999999999999888…`.
    /// Rounding to a common type would make the comparison intransitive:
    /// `2^53 + 1` would equal `2^53` as a double, which equals the integer `2^53`.
    /// The infinities lie beyond every exact value.
    pub fn value_cmp(self, other: Self) -> Option<Ordering> {
        match (self, other) {
            (NumberRef::Decimal(left), NumberRef::Decimal(right)) => Some(left.value_cmp(right)),
            (NumberRef::Decimal(exact), floating) => exact.floating_cmp(floating.to_f64()),
            (floating, NumberRef::Decimal(exact)) => {
                exact.floating_cmp(floating.to_f64()).map(Ordering::reverse)
            }
            // A float widens to a double exactly.
            (floating, other) => floating.to_f64().partial_cmp(&other.to_f64()),
        }
    }

    /// Clones the viewed number into an owned [Number].
    pub fn to_owned(self) -> Number {
        match self {
            NumberRef::Decimal(value) => Number::Decimal(value.to_owned()),
            NumberRef::Float(value) => Number::Float(value),
            NumberRef::Double(value) => Number::Double(value),
        }
    }
}

impl PartialEq for NumberRef<'_> {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (NumberRef::Decimal(left), NumberRef::Decimal(right)) => left == right,
            (NumberRef::Float(left), NumberRef::Float(right)) => left == right,
            (NumberRef::Double(left), NumberRef::Double(right)) => left == right,
            _ => false,
        }
    }
}

impl Eq for NumberRef<'_> {}

impl Ord for NumberRef<'_> {
    fn cmp(&self, other: &Self) -> Ordering {
        match (self, other) {
            (NumberRef::Decimal(left), NumberRef::Decimal(right)) => left.cmp(right),
            (NumberRef::Float(left), NumberRef::Float(right)) => left.cmp(right),
            (NumberRef::Double(left), NumberRef::Double(right)) => left.cmp(right),
            // The term order across domains is their declaration order.
            _ => self.domain().cmp(&other.domain()),
        }
    }
}

impl PartialOrd for NumberRef<'_> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Hash for NumberRef<'_> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        match self {
            NumberRef::Decimal(value) => value.hash(state),
            NumberRef::Float(value) => value.hash(state),
            NumberRef::Double(value) => value.hash(state),
        }
    }
}

impl fmt::Debug for NumberRef<'_> {
    /// Writes the value in its canonical form,
    /// a float with the `f` and a double with the `d` suffix of the term syntax,
    /// so that the domain shows.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            NumberRef::Decimal(value) => write!(f, "{value}"),
            NumberRef::Float(value) => write!(f, "{value}f"),
            NumberRef::Double(value) => write!(f, "{value}d"),
        }
    }
}

impl fmt::Display for NumberRef<'_> {
    /// Writes the canonical lexical form of the value's datatype.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            NumberRef::Decimal(value) => write!(f, "{value}"),
            NumberRef::Float(value) => write!(f, "{value}"),
            NumberRef::Double(value) => write!(f, "{value}"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::hash::{BuildHasher, RandomState};

    use crate::{
        testing::number,
        value::{DataValue, ValueRef},
    };

    /// Returns the number a value holds.
    fn of(value: &DataValue) -> NumberRef<'_> {
        match value.as_ref() {
            ValueRef::Number(number) => number,
            other => panic!("{other:?} is not a number"),
        }
    }

    /// Returns a float.
    fn float(value: f32) -> NumberRef<'static> {
        NumberRef::Float(Float::new(value))
    }

    /// Returns a double.
    fn double(value: f64) -> NumberRef<'static> {
        NumberRef::Double(Double::new(value))
    }

    #[test]
    fn identity_is_by_domain_then_by_value() {
        // One term: the canonical integer.
        assert_eq!(number("1"), number("1.0"));

        // Distinct terms across the domains, ordered by declaration.
        let distinct = [
            DataValue::integer(1),
            DataValue::float(1.0),
            DataValue::double(1.0),
        ];

        for (index, left) in distinct.iter().enumerate() {
            for right in &distinct[index + 1..] {
                assert_ne!(left, right);
                assert!(left < right);
            }
        }

        assert_ne!(DataValue::integer(1), DataValue::string("1"));

        // The wide cases take their place by value among the compact ones.
        let ordered = [
            number("-123456789012345678901"),
            DataValue::integer(i64::MIN),
            DataValue::integer(i64::MAX),
            number("123456789012345678901"),
            number("-123456789012345678901.5"),
            number("-1.5"),
            number("1.5"),
            number("1.50000000000000001"),
            number("123456789012345678901.5"),
            DataValue::float(f32::NEG_INFINITY),
            DataValue::float(f32::NAN),
            DataValue::double(-0.0),
            DataValue::double(0.0),
        ];

        for window in ordered.windows(2) {
            assert!(window[0] < window[1], "{} < {}", window[0], window[1]);
            assert_eq!(window[0].cmp(&window[0]), Ordering::Equal);
        }

        // Hashing agrees with equality.
        let state = RandomState::new();
        for value in &ordered {
            assert_eq!(
                state.hash_one(of(value)),
                state.hash_one(of(&value.clone()))
            );
        }

        assert_eq!(
            state.hash_one(of(&number("1"))),
            state.hash_one(of(&number("1.0")))
        );
    }

    #[test]
    fn domains_follow_integrality() {
        assert_eq!(
            of(&number("123456789012345678901")).domain(),
            ValueDomain::Integer
        );
        assert_eq!(
            of(&number("123456789012345678901.5")).domain(),
            ValueDomain::Decimal
        );
        assert_eq!(of(&number("15.0")).domain(), ValueDomain::Integer);
        assert_eq!(of(&number("1.5")).domain(), ValueDomain::Decimal);
        assert!(of(&number("15.0")).is_integral());
        assert!(!float(2.0).is_integral());
        assert!(of(&number("-0.5")).is_negative());
        assert!(!double(-0.0).is_negative());
        assert!(double(-0.0).is_zero());
        assert!(!of(&number("0.5")).is_zero());
        assert_eq!(of(&number("42")).to_i64(), Some(42));
        assert_eq!(of(&number("123456789012345678901")).to_i64(), None);
    }

    #[test]
    fn values_compare_across_the_ladder() {
        let cmp = |left: &DataValue, right: &DataValue| of(left).value_cmp(of(right));

        assert!(of(&number("1")).value_eq(double(1.0)));
        assert!(of(&number("1")).value_eq(float(1.0)));
        assert!(of(&number("0.5")).value_eq(double(0.5)));
        assert!(of(&number("0")).value_eq(double(-0.0)));

        // A double is the binary fraction it denotes, compared without rounding.
        assert_eq!(
            of(&number("0.3")).value_cmp(double(0.3)),
            Some(Ordering::Greater)
        );
        assert_eq!(
            double(0.3).value_cmp(of(&number("0.3"))),
            Some(Ordering::Less)
        );
        assert!(
            of(&number(
                "0.1000000000000000055511151231257827021181583404541015625"
            ))
            .value_eq(double(0.1))
        );
        assert_eq!(
            of(&number("2.0000000000000001")).value_cmp(double(2.0)),
            Some(Ordering::Greater)
        );
        assert_eq!(
            of(&DataValue::integer(16_777_217)).value_cmp(float(16_777_216.0)),
            Some(Ordering::Greater)
        );

        // So the comparison is transitive where rounding to doubles would not be.
        let (above, power) = (
            DataValue::integer((1 << 53) + 1),
            DataValue::integer(1 << 53),
        );
        assert_eq!(
            of(&above).value_cmp(double(9_007_199_254_740_992.0)),
            Some(Ordering::Greater)
        );
        assert!(of(&power).value_eq(double(9_007_199_254_740_992.0)));
        assert_eq!(
            of(&DataValue::integer(i64::MAX)).value_cmp(double(9_223_372_036_854_775_808.0)),
            Some(Ordering::Less)
        );

        // The infinities lie beyond every exact value, the wide ones included.
        let vast = number(&format!("1{}", "0".repeat(400)));
        assert_eq!(
            of(&vast).value_cmp(double(f64::INFINITY)),
            Some(Ordering::Less)
        );
        assert_eq!(
            of(&vast).value_cmp(double(f64::MAX)),
            Some(Ordering::Greater)
        );
        assert_eq!(
            double(f64::NEG_INFINITY).value_cmp(of(&vast)),
            Some(Ordering::Less)
        );
        assert_eq!(
            of(&number("100000000000000000000000000000000000000000"))
                .value_cmp(float(f32::INFINITY)),
            Some(Ordering::Less)
        );

        assert_eq!(double(f64::NAN).value_cmp(of(&number("1"))), None);
        assert_eq!(of(&number("1")).value_cmp(double(f64::NAN)), None);
        assert!(!double(f64::NAN).value_eq(double(f64::NAN)));
        assert_eq!(
            cmp(
                &number("123456789012345678901"),
                &number("123456789012345678901.5")
            ),
            Some(Ordering::Less)
        );
        assert_eq!(
            cmp(&number("123456789012345678901.5"), &number("1.5")),
            Some(Ordering::Greater)
        );
        assert_eq!(cmp(&number("1.5"), &number("2")), Some(Ordering::Less));
        assert_eq!(cmp(&number("2.0"), &number("2")), Some(Ordering::Equal));
        assert_eq!(
            cmp(
                &number("-123456789012345678901"),
                &DataValue::integer(i64::MIN)
            ),
            Some(Ordering::Less)
        );
        assert_eq!(cmp(&number("0.5"), &number("0.55")), Some(Ordering::Less));
    }

    #[test]
    fn decimals_approximate_to_the_nearest_double() {
        assert_eq!(of(&number("0.3")).to_f64(), 0.3);
        assert_eq!(
            of(&number("123456789012345678901")).to_f64(),
            1.2345678901234568e20
        );
        assert_eq!(float(0.1).to_f64(), f64::from(0.1f32));
    }

    #[test]
    fn numbers_approximate_to_the_nearest_float() {
        // Just above the midpoint 2^60 + 2^36 of two floats, where the double lands.
        assert_eq!(
            of(&DataValue::integer(1_152_921_573_326_323_713)).to_f32(),
            1_152_921_642_045_800_448.0
        );
        assert_eq!(
            of(&number("1.00000005960464477539062500000001")).to_f32(),
            1.0 + f32::EPSILON
        );
        assert_eq!(of(&number("0.3")).to_f32(), 0.3);
        assert_eq!(double(0.1).to_f32(), 0.1);
        assert!(double(1e300).to_f32().is_infinite());
    }
}
