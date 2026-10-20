//! The arithmetic over numbers: promotion along the type ladder,
//! then the operation of the type both operands meet at,
//! exact for [decimals](DecimalRef) and IEEE's for floats and doubles.
//!
//! The operators are the `std::ops` traits with a [Result] output,
//! and every failure is one of two:
//! a [DivisionByZero](EvaluationError::DivisionByZero) for an exact zero divisor,
//! an [Overflow](EvaluationError::Overflow) for a result past the length limit of numbers.
//! Floats never fail: an infinity or the NaN is a value of their domain.

use std::ops::{Add, Div, Mul, Neg, Rem, Sub};

use crate::value::{
    DataValue, EvaluationError, ValueDomain,
    number::{DecimalRef, Double, Float, Number, NumberRef},
};

impl Add for NumberRef<'_> {
    type Output = Result<DataValue, EvaluationError>;

    /// Returns the sum, `op:numeric-add`.
    ///
    /// # Errors
    ///
    /// Returns [Overflow](EvaluationError::Overflow) for a result past the length limit of numbers.
    fn add(self, other: Self) -> Self::Output {
        Ok(match promote(self, other) {
            Pair::Decimal(left, right) => DataValue::from((left + right)?),
            Pair::Float(left, right) => DataValue::float(left + right),
            Pair::Double(left, right) => DataValue::double(left + right),
        })
    }
}

impl Sub for NumberRef<'_> {
    type Output = Result<DataValue, EvaluationError>;

    /// Returns the difference, `op:numeric-subtract`.
    ///
    /// # Errors
    ///
    /// Returns [Overflow](EvaluationError::Overflow) for a result past the length limit of numbers.
    fn sub(self, other: Self) -> Self::Output {
        Ok(match promote(self, other) {
            Pair::Decimal(left, right) => DataValue::from((left - right)?),
            Pair::Float(left, right) => DataValue::float(left - right),
            Pair::Double(left, right) => DataValue::double(left - right),
        })
    }
}

impl Mul for NumberRef<'_> {
    type Output = Result<DataValue, EvaluationError>;

    /// Returns the product, `op:numeric-multiply`.
    ///
    /// # Errors
    ///
    /// Returns [Overflow](EvaluationError::Overflow) for a result past the length limit of numbers.
    fn mul(self, other: Self) -> Self::Output {
        Ok(match promote(self, other) {
            Pair::Decimal(left, right) => DataValue::from((left * right)?),
            Pair::Float(left, right) => DataValue::float(left * right),
            Pair::Double(left, right) => DataValue::double(left * right),
        })
    }
}

impl Div for NumberRef<'_> {
    type Output = Result<DataValue, EvaluationError>;

    /// Returns the quotient, `op:numeric-divide`.
    ///
    /// Exact operands divide exactly if the quotient terminates within the scale,
    /// and are rounded half to even at it otherwise.
    /// The scale is the largest of eighteen significant digits, eighteen fraction digits,
    /// and the fraction digits of either operand.
    /// Floats divide as IEEE does, to an infinity or the NaN for a zero divisor.
    ///
    /// # Errors
    ///
    /// Returns [DivisionByZero](EvaluationError::DivisionByZero) for an exact zero divisor,
    /// and [Overflow](EvaluationError::Overflow) for a result past the length limit of numbers.
    ///
    /// # Example
    ///
    /// ```
    /// use nemo_physical::value::{DataValue, EvaluationError, NumberRef};
    ///
    /// let int = |value: i64| NumberRef::Decimal(value.into());
    /// let quotient = |left, right| (int(left) / int(right)).unwrap().to_string();
    /// assert_eq!(quotient(1, 3), "0.333333333333333333");
    /// assert_eq!(quotient(2, 3), "0.666666666666666667");
    /// assert_eq!(quotient(1, 4), "0.25");
    /// assert_eq!(quotient(6, 3), "2");
    /// assert_eq!(int(1) / int(0), Err(EvaluationError::DivisionByZero));
    /// ```
    fn div(self, other: Self) -> Self::Output {
        Ok(match promote(self, other) {
            Pair::Decimal(left, right) => DataValue::from((left / right)?),
            Pair::Float(left, right) => DataValue::float(left / right),
            Pair::Double(left, right) => DataValue::double(left / right),
        })
    }
}

impl Rem for NumberRef<'_> {
    type Output = Result<DataValue, EvaluationError>;

    /// Returns the remainder of integer division with the dividend's sign, `op:numeric-mod`.
    ///
    /// # Errors
    ///
    /// Returns [TypeMismatch](EvaluationError::TypeMismatch) unless both values are integers,
    /// and [DivisionByZero](EvaluationError::DivisionByZero) for a zero divisor.
    fn rem(self, other: Self) -> Self::Output {
        match (self, other) {
            (NumberRef::Decimal(left), NumberRef::Decimal(right)) => {
                (left % right).map(DataValue::from)
            }
            _ => Err(EvaluationError::TypeMismatch),
        }
    }
}

impl NumberRef<'_> {
    /// Returns the value promoted along the ladder to a numeric domain at or above its own,
    /// as the operations promote their operands.
    ///
    /// The value becomes a float or a double for those domains.
    /// For an exact domain it stays as it is,
    /// since integers and decimals share one value space.
    pub(crate) fn promote_to(self, domain: ValueDomain) -> Self {
        match domain {
            ValueDomain::Float => NumberRef::Float(Float::new(self.to_f32())),
            ValueDomain::Double => NumberRef::Double(Double::new(self.to_f64())),
            _ => self,
        }
    }

    /// Returns the value raised to a non-negative integer power, exactly.
    ///
    /// # Errors
    ///
    /// Returns [TypeMismatch](EvaluationError::TypeMismatch) for a float or a double,
    /// and [Overflow](EvaluationError::Overflow) for a result past the length limit of numbers.
    pub fn pow(self, power: u32) -> Result<DataValue, EvaluationError> {
        match self {
            NumberRef::Decimal(value) => value.pow(power).map(DataValue::from),
            NumberRef::Float(_) | NumberRef::Double(_) => Err(EvaluationError::TypeMismatch),
        }
    }

    /// Returns the absolute value, `fn:abs`.
    pub fn abs(self) -> DataValue {
        match self {
            NumberRef::Decimal(value) => DataValue::from(value.abs()),
            NumberRef::Float(value) => DataValue::float(value.abs()),
            NumberRef::Double(value) => DataValue::double(value.abs()),
        }
    }

    /// Returns the nearest integer, halves toward positive infinity, `fn:round`.
    /// A decimal becomes an integer, and a float stays a float.
    pub fn round(self) -> DataValue {
        self.to_integral(Rounding::Nearest)
    }

    /// Returns the smallest integer not below the value, `fn:ceiling`.
    /// A decimal becomes an integer, and a float stays a float.
    pub fn ceil(self) -> DataValue {
        self.to_integral(Rounding::Up)
    }

    /// Returns the largest integer not above the value, `fn:floor`.
    /// A decimal becomes an integer, and a float stays a float.
    pub fn floor(self) -> DataValue {
        self.to_integral(Rounding::Down)
    }

    /// Returns the value taken to an integer by the given rounding, within its domain.
    fn to_integral(self, rounding: Rounding) -> DataValue {
        match self {
            NumberRef::Decimal(value) => DataValue::from(value.to_integral(rounding)),
            // An integral value near a float is representable in it: the cast back is exact.
            #[allow(clippy::cast_possible_truncation)]
            NumberRef::Float(value) => DataValue::float(rounding.apply(value.to_f64()) as f32),
            NumberRef::Double(value) => DataValue::double(rounding.apply(value.get())),
        }
    }
}

impl Neg for NumberRef<'_> {
    type Output = DataValue;

    /// Returns the negated value, `op:numeric-unary-minus`.
    fn neg(self) -> DataValue {
        match self {
            NumberRef::Decimal(value) => DataValue::from(-value),
            NumberRef::Float(value) => DataValue::float(-value),
            NumberRef::Double(value) => DataValue::double(-value),
        }
    }
}

/// Forwards a binary operator on owned numbers to the [view](Number::as_ref).
macro_rules! forward_binary {
    ($trait:ident, $method:ident) => {
        impl $trait for &Number {
            type Output = Result<DataValue, EvaluationError>;

            /// Forwards to the operator on [NumberRef].
            fn $method(self, other: Self) -> Self::Output {
                self.as_ref().$method(other.as_ref())
            }
        }
    };
}

forward_binary!(Add, add);
forward_binary!(Sub, sub);
forward_binary!(Mul, mul);
forward_binary!(Div, div);
forward_binary!(Rem, rem);

impl Number {
    /// Returns the value raised to a power. See [NumberRef::pow].
    pub fn pow(&self, power: u32) -> Result<DataValue, EvaluationError> {
        self.as_ref().pow(power)
    }

    /// Returns the absolute value. See [NumberRef::abs].
    pub fn abs(&self) -> DataValue {
        self.as_ref().abs()
    }

    /// Returns the nearest integer. See [NumberRef::round].
    pub fn round(&self) -> DataValue {
        self.as_ref().round()
    }

    /// Returns the smallest integer not below the value. See [NumberRef::ceil].
    pub fn ceil(&self) -> DataValue {
        self.as_ref().ceil()
    }

    /// Returns the largest integer not above the value. See [NumberRef::floor].
    pub fn floor(&self) -> DataValue {
        self.as_ref().floor()
    }
}

impl Neg for &Number {
    type Output = DataValue;

    /// Returns the negated value, `op:numeric-unary-minus`, as the negation of a [NumberRef] does.
    fn neg(self) -> DataValue {
        -self.as_ref()
    }
}

impl Neg for Number {
    type Output = DataValue;

    /// Returns the negated value, `op:numeric-unary-minus`, as the negation of a [NumberRef] does.
    fn neg(self) -> DataValue {
        -self.as_ref()
    }
}

/// The ways a number is taken to an integer.
#[derive(Debug, Clone, Copy)]
pub(crate) enum Rounding {
    /// To the nearest integer, halves toward positive infinity.
    Nearest,
    /// To the nearest integer not below the value.
    Up,
    /// To the nearest integer not above the value.
    Down,
}

impl Rounding {
    /// Applies the rounding to a double.
    fn apply(self, value: f64) -> f64 {
        match self {
            Rounding::Nearest => {
                // The primitive takes halves away from zero;
                // a negative half comes back up, to negative zero from -0.5,
                // as `fn:round` has it.
                let rounded = value.round();
                if rounded - value == -0.5 {
                    (rounded + 1.0).copysign(value)
                } else {
                    rounded
                }
            }
            Rounding::Up => value.ceil(),
            Rounding::Down => value.floor(),
        }
    }
}

/// A pair of numbers promoted to their smallest common type.
enum Pair<'a> {
    /// Both are exact.
    Decimal(DecimalRef<'a>, DecimalRef<'a>),
    /// At least one float, no double.
    Float(Float, Float),
    /// At least one double.
    Double(Double, Double),
}

/// Promotes two numbers to their smallest common type.
fn promote<'a>(left: NumberRef<'a>, right: NumberRef<'a>) -> Pair<'a> {
    match (left, right) {
        (NumberRef::Decimal(left), NumberRef::Decimal(right)) => Pair::Decimal(left, right),
        (NumberRef::Double(left), right) => Pair::Double(left, Double::new(right.to_f64())),
        (left, NumberRef::Double(right)) => Pair::Double(Double::new(left.to_f64()), right),
        (NumberRef::Float(left), right) => Pair::Float(left, Float::new(right.to_f32())),
        (left, NumberRef::Float(right)) => Pair::Float(Float::new(left.to_f32()), right),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::{
        testing::number,
        value::{Decimal, ValueRef},
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
    fn the_ladder_picks_the_smallest_type() {
        let add = |left: &DataValue, right: NumberRef<'_>| (of(left) + right).expect("bounded");

        assert_eq!(add(&number("1"), of(&number("1.5"))), number("2.5"));
        assert_eq!(add(&number("1"), float(1.0)), DataValue::float(2.0));
        assert_eq!(
            add(&DataValue::float(1.0), double(1.0)),
            DataValue::double(2.0)
        );
        assert_eq!(add(&number("0.3"), double(0.0)), DataValue::double(0.3));
        assert_eq!(add(&number("0.7"), float(0.0)), DataValue::float(0.7));
        assert_eq!(
            add(&number("123456789012345678901"), double(0.5)),
            DataValue::double(1.2345678901234568e20)
        );
    }

    #[test]
    fn results_land_in_their_home() {
        /// Applies a binary operation to two number lexicals.
        fn apply(
            left: &str,
            right: &str,
            operation: impl Fn(NumberRef<'_>, NumberRef<'_>) -> Result<DataValue, EvaluationError>,
        ) -> DataValue {
            operation(of(&number(left)), of(&number(right))).expect("bounded")
        }

        assert_eq!(apply("1.5", "1.5", |l, r| l + r), DataValue::integer(3));
        assert_eq!(
            apply("9223372036854775807", "1", |l, r| l + r),
            number("9223372036854775808")
        );
        assert_eq!(
            apply("9223372036854775808", "1", |l, r| l - r),
            DataValue::integer(i64::MAX)
        );
        assert_eq!(
            of(&number("9223372036854775808")).neg(),
            DataValue::integer(i64::MIN)
        );
        assert_eq!(
            of(&DataValue::integer(i64::MIN)).neg(),
            number("9223372036854775808")
        );
        assert_eq!(of(&number("-1.5")).abs(), number("1.5"));
        assert_eq!(
            of(&number("-123456789012345678901.5")).abs(),
            number("123456789012345678901.5")
        );
        assert_eq!(apply("-4", "3", |l, r| l * r), DataValue::integer(-12));
        assert_eq!(apply("0.2", "0.5", |l, r| l * r), number("0.1"));
        assert_eq!(
            apply("1234567890123456", "1000000000000000000", |l, r| l * r),
            number("1234567890123456000000000000000000")
        );
        assert_eq!(apply("1", "0.001", |l, r| l + r), number("1.001"));

        // An alignment beyond the workspace continues in the big one.
        assert_eq!(
            apply("1", "0.0000000000000000000000000000000000000001", |l, r| l
                + r),
            number("1.0000000000000000000000000000000000000001")
        );
    }

    #[test]
    fn division_is_exact_within_the_scale() {
        let div = |left: &str, right: &str| of(&number(left)) / of(&number(right));
        assert_eq!(div("7", "2"), Ok(number("3.5")));
        assert_eq!(div("6", "3"), Ok(DataValue::integer(2)));
        assert_eq!(div("1", "3"), Ok(number("0.333333333333333333")));
        assert_eq!(div("2", "3"), Ok(number("0.666666666666666667")));
        assert_eq!(
            div("12345678901234567890123", "7"),
            Ok(number("1763668414462081127160.428571428571428571"))
        );
        assert_eq!(div("0", "3"), Ok(DataValue::integer(0)));
        assert_eq!(div("0.5", "0.25"), Ok(DataValue::integer(2)));
        assert_eq!(div("1", "0"), Err(EvaluationError::DivisionByZero));
        assert_eq!(div("1.5", "0"), Err(EvaluationError::DivisionByZero));
        assert_eq!(
            div("123456789012345678901", "0"),
            Err(EvaluationError::DivisionByZero)
        );
        assert_eq!(
            div("123456789012345678901", "100"),
            Ok(number("1234567890123456789.01"))
        );

        // A scaled numerator beyond the workspace divides in the big one.
        assert_eq!(
            div(
                "9223372036854775807",
                "0.000000000000000000000000000000000000001"
            ),
            Ok(number(
                "9223372036854775807000000000000000000000000000000000000000"
            ))
        );
    }

    #[test]
    fn floats_follow_ieee() {
        assert_eq!(
            of(&number("1")) / double(0.0),
            Ok(DataValue::double(f64::INFINITY))
        );
        assert_eq!(
            double(-1.0) / double(0.0),
            Ok(DataValue::double(f64::NEG_INFINITY))
        );
        assert_eq!(float(0.0) / float(0.0), Ok(DataValue::float(f32::NAN)));
        assert_eq!(
            double(1e300) * double(1e300),
            Ok(DataValue::double(f64::INFINITY))
        );
        assert_eq!(
            double(f64::INFINITY) - double(f64::INFINITY),
            Ok(DataValue::double(f64::NAN))
        );
        assert_eq!(-double(f64::INFINITY), DataValue::double(f64::NEG_INFINITY));
    }

    #[test]
    fn owned_numbers_forward_to_the_view() {
        let owned =
            |lexical: &str| Number::Decimal(Decimal::from_lexical(lexical).expect("well-formed"));
        let (seven, two) = (owned("7"), owned("2"));

        assert_eq!(&seven + &two, Ok(DataValue::integer(9)));
        assert_eq!(&seven - &two, Ok(DataValue::integer(5)));
        assert_eq!(&seven * &two, Ok(DataValue::integer(14)));
        assert_eq!(&seven / &two, Ok(number("3.5")));
        assert_eq!(&seven % &two, Ok(DataValue::integer(1)));
        assert_eq!(two.pow(10), Ok(DataValue::integer(1024)));
        assert_eq!(owned("-2.5").abs(), number("2.5"));
        assert_eq!(owned("2.5").round(), DataValue::integer(3));
        assert_eq!(owned("2.5").ceil(), DataValue::integer(3));
        assert_eq!(owned("2.5").floor(), DataValue::integer(2));
        assert_eq!(-&seven, DataValue::integer(-7));
        assert_eq!(-seven, DataValue::integer(-7));
    }

    #[test]
    fn remainders_and_powers_are_for_integers() {
        let int = |value: i64| NumberRef::Decimal(value.into());

        assert_eq!(int(-7) % int(3), Ok(DataValue::integer(-1)));
        assert_eq!(int(i64::MIN) % int(-1), Ok(DataValue::integer(0)));
        assert_eq!(int(7) % int(0), Err(EvaluationError::DivisionByZero));
        assert_eq!(
            of(&number("7.5")) % int(2),
            Err(EvaluationError::TypeMismatch)
        );
        assert_eq!(
            of(&number("123456789012345678901")) % int(10),
            Ok(DataValue::integer(1))
        );
        assert_eq!(int(2).pow(10), Ok(DataValue::integer(1024)));
        assert_eq!(int(2).pow(200).map(|value| value.to_string().len()), Ok(61));
        assert_eq!(of(&number("0.5")).pow(3), Ok(number("0.125")));
        assert_eq!(
            of(&number("123456789012345678901")).pow(2),
            Ok(number("15241578753238836750437433565526596567801"))
        );
        assert_eq!(float(2.0).pow(2), Err(EvaluationError::TypeMismatch));
    }

    #[test]
    fn results_past_the_limit_are_refused_before_they_are_computed() {
        assert_eq!(
            NumberRef::Decimal(3.into()).pow(u32::MAX),
            Err(EvaluationError::Overflow)
        );
        let half = number(&format!("1{}", "0".repeat(600_000)));
        assert_eq!(of(&half) * of(&half), Err(EvaluationError::Overflow));
    }

    #[test]
    fn compact_decimals_round_exactly() {
        /// Applies a unary operation to a number lexical.
        fn apply(operation: impl Fn(NumberRef<'_>) -> DataValue, lexical: &str) -> DataValue {
            operation(of(&number(lexical)))
        }

        // Digits past a double's precision still decide the direction.
        assert_eq!(
            apply(|n| n.ceil(), "2.0000000000000001"),
            DataValue::integer(3)
        );
        assert_eq!(
            apply(|n| n.round(), "0.49999999999999999"),
            DataValue::integer(0)
        );
        assert_eq!(
            apply(|n| n.ceil(), "1000000000000000.1"),
            DataValue::integer(1_000_000_000_000_001)
        );
        assert_eq!(
            apply(|n| n.floor(), "-2.0000000000000001"),
            DataValue::integer(-3)
        );

        // Halves go up; the directed roundings move on any fraction toward their side.
        assert_eq!(apply(|n| n.round(), "2.5"), DataValue::integer(3));
        assert_eq!(apply(|n| n.round(), "-2.5"), DataValue::integer(-2));
        assert_eq!(apply(|n| n.round(), "-2.6"), DataValue::integer(-3));
        assert_eq!(apply(|n| n.round(), "2.4"), DataValue::integer(2));
        assert_eq!(float(-2.5).round(), DataValue::float(-2.0));
        assert_eq!(float(-3.5).round(), DataValue::float(-3.0));
        // Between -0.5 and zero, the result is negative zero.
        assert_eq!(double(-0.5).round(), DataValue::double(-0.0));
        assert_eq!(double(-0.4).round(), DataValue::double(-0.0));
        assert_eq!(float(-0.5).round(), DataValue::float(-0.0));
        assert_eq!(double(-1.5).round(), DataValue::double(-1.0));
        assert_eq!(apply(|n| n.ceil(), "-2.5"), DataValue::integer(-2));
        assert_eq!(apply(|n| n.floor(), "2.5"), DataValue::integer(2));

        // A fraction beyond the workspace rounds in the big one.
        let tiny = format!("0.{}1", "0".repeat(39));
        assert_eq!(apply(|n| n.round(), &tiny), DataValue::integer(0));
        assert_eq!(apply(|n| n.ceil(), &tiny), DataValue::integer(1));
        assert_eq!(
            apply(|n| n.floor(), &format!("-{tiny}")),
            DataValue::integer(-1)
        );

        // Integers stay, wide ones included; floats stay floats.
        assert_eq!(
            apply(|n| n.round(), "123456789012345678901"),
            number("123456789012345678901")
        );
        assert_eq!(
            apply(|n| n.round(), "123456789012345678901.5"),
            number("123456789012345678902")
        );
        assert_eq!(float(2.5).round(), DataValue::float(3.0));
        assert_eq!(double(-2.5).ceil(), DataValue::double(-2.0));
    }
}
