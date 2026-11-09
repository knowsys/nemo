//! Arithmetic on exact numbers.
//!
//! An operation computes in the `i128` workspace where the result fits,
//! and continues in the big workspace otherwise.
//!
//! The operators are the `std::ops` traits with a [Result] output.
//! An operation fails in one of three ways:
//! - with [DivisionByZero](EvaluationError::DivisionByZero) for a zero divisor;
//! - with [Overflow](EvaluationError::Overflow) for a result past the length limit of numbers;
//! - with [TypeMismatch](EvaluationError::TypeMismatch) for a remainder of a value
//!   that is not an integer.

use std::ops::{Add, Div, Mul, Neg, Rem, Sub};

use crate::value::{
    EvaluationError,
    number::{
        arithmetic::Rounding,
        decimal::{Decimal, DecimalRef, ReprRef, workspace},
    },
};

impl Add for DecimalRef<'_> {
    type Output = Result<Decimal, EvaluationError>;

    /// Returns the sum, `op:numeric-add`.
    ///
    /// # Errors
    ///
    /// Returns [Overflow](EvaluationError::Overflow) for a result past the length limit of numbers.
    fn add(self, other: Self) -> Self::Output {
        bounded(match operands(self, other) {
            Operands::Int(left, right) => Decimal::new(i128::from(left) + i128::from(right), 0),
            Operands::Fixed(left, right) => match left.align(right) {
                Some((left, right, exponent)) => match left.checked_add(right) {
                    Some(sum) => Decimal::new(sum, exponent),
                    None => big_sum(left, right, exponent, false),
                },
                None => big_binary(left, right, workspace::Big::add),
            },
            Operands::Big(left, right) => left.add(&right).map(Decimal::from),
        })
    }
}

impl Sub for DecimalRef<'_> {
    type Output = Result<Decimal, EvaluationError>;

    /// Returns the difference, `op:numeric-subtract`.
    ///
    /// # Errors
    ///
    /// Returns [Overflow](EvaluationError::Overflow) for a result past the length limit of numbers.
    fn sub(self, other: Self) -> Self::Output {
        bounded(match operands(self, other) {
            Operands::Int(left, right) => Decimal::new(i128::from(left) - i128::from(right), 0),
            Operands::Fixed(left, right) => match left.align(right) {
                Some((left, right, exponent)) => match left.checked_sub(right) {
                    Some(difference) => Decimal::new(difference, exponent),
                    None => big_sum(left, right, exponent, true),
                },
                None => big_binary(left, right, workspace::Big::sub),
            },
            Operands::Big(left, right) => left.sub(&right).map(Decimal::from),
        })
    }
}

impl Mul for DecimalRef<'_> {
    type Output = Result<Decimal, EvaluationError>;

    /// Returns the product, `op:numeric-multiply`.
    ///
    /// # Errors
    ///
    /// Returns [Overflow](EvaluationError::Overflow) for a result past the length limit of numbers.
    fn mul(self, other: Self) -> Self::Output {
        bounded(match operands(self, other) {
            Operands::Int(left, right) => Decimal::new(i128::from(left) * i128::from(right), 0),
            // Two compact mantissas are multiplied in the `i128` workspace if the product fits.
            Operands::Fixed(left, right) => match left.mantissa().checked_mul(right.mantissa()) {
                Some(product) => Decimal::new(product, left.exponent() + right.exponent()),
                None => big_binary(left, right, workspace::Big::mul),
            },
            Operands::Big(left, right) => left.mul(&right).map(Decimal::from),
        })
    }
}

impl Div for DecimalRef<'_> {
    type Output = Result<Decimal, EvaluationError>;

    /// Returns the quotient, `op:numeric-divide`.
    ///
    /// The quotient is exact if it terminates within the scale,
    /// and is rounded half to even at the scale otherwise.
    /// The scale is the largest of eighteen significant digits, eighteen fraction digits,
    /// and the fraction digits of either operand.
    ///
    /// # Errors
    ///
    /// Returns [DivisionByZero](EvaluationError::DivisionByZero) for a zero divisor,
    /// and [Overflow](EvaluationError::Overflow) for a result past the length limit of numbers.
    fn div(self, other: Self) -> Self::Output {
        bounded(match operands(self, other) {
            Operands::Int(left, right) => {
                if right == 0 {
                    return Err(EvaluationError::DivisionByZero);
                }
                let (left, right) = (i128::from(left), i128::from(right));
                if left % right == 0 {
                    Decimal::new(left / right, 0)
                } else {
                    fixed_quotient(
                        workspace::Fixed::new(left, 0),
                        workspace::Fixed::new(right, 0),
                    )
                }
            }
            Operands::Fixed(left, right) => {
                if right.mantissa() == 0 {
                    return Err(EvaluationError::DivisionByZero);
                }
                fixed_quotient(left, right)
            }
            Operands::Big(left, right) => {
                if right.is_zero() {
                    return Err(EvaluationError::DivisionByZero);
                }
                left.div(&right).map(Decimal::from)
            }
        })
    }
}

impl Rem for DecimalRef<'_> {
    type Output = Result<Decimal, EvaluationError>;

    /// Returns the remainder of the division of two integers, which has the sign of the
    /// dividend, `op:numeric-mod`.
    ///
    /// # Errors
    ///
    /// Returns [TypeMismatch](EvaluationError::TypeMismatch) unless both values are integers,
    /// and [DivisionByZero](EvaluationError::DivisionByZero) for a zero divisor.
    fn rem(self, other: Self) -> Self::Output {
        match (self.0, other.0) {
            (ReprRef::Int(left), ReprRef::Int(right)) => {
                if right == 0 {
                    return Err(EvaluationError::DivisionByZero);
                }
                // The wrapping case, `i64::MIN % -1`, has the remainder zero.
                Ok(Decimal::from(left.wrapping_rem(right)))
            }
            _ if self.is_integral() && other.is_integral() => {
                let (left, right) = (big(self), big(other));
                if right.is_zero() {
                    return Err(EvaluationError::DivisionByZero);
                }
                bounded(left.rem(&right).map(Decimal::from))
            }
            _ => Err(EvaluationError::TypeMismatch),
        }
    }
}

impl Neg for DecimalRef<'_> {
    type Output = Decimal;

    /// Returns the negated value, `op:numeric-unary-minus`.
    fn neg(self) -> Decimal {
        match self.0 {
            ReprRef::Int(value) => Decimal::new(-i128::from(value), 0),
            ReprRef::Compact(value) => {
                Decimal::new(-i128::from(value.mantissa()), i64::from(value.exponent()))
            }
            // The negation of a wide value may lie in a compact range.
            ReprRef::Wide(value) => Some(Decimal::from(workspace::Big::from_wide(value).neg())),
        }
        .expect("negation keeps the length")
    }
}

impl DecimalRef<'_> {
    /// Returns the value raised to a non-negative integer power, exactly.
    ///
    /// # Errors
    ///
    /// Returns [Overflow](EvaluationError::Overflow) for a result past the length limit of numbers.
    pub fn pow(self, power: u32) -> Result<Decimal, EvaluationError> {
        bounded(match workspace::Fixed::of(self) {
            Some(fixed) => match (
                fixed.mantissa().checked_pow(power),
                fixed.exponent().checked_mul(i64::from(power)),
            ) {
                (Some(mantissa), Some(exponent)) => Decimal::new(mantissa, exponent),
                _ => big(self).pow(power).map(Decimal::from),
            },
            None => big(self).pow(power).map(Decimal::from),
        })
    }

    /// Returns the absolute value, `fn:abs`.
    pub fn abs(self) -> Decimal {
        match self.0 {
            ReprRef::Int(value) => Decimal::new(i128::from(value).abs(), 0),
            ReprRef::Compact(value) => Decimal::new(
                i128::from(value.mantissa()).abs(),
                i64::from(value.exponent()),
            ),
            ReprRef::Wide(value) => Some(Decimal::from(workspace::Big::from_wide(value).abs())),
        }
        .expect("the absolute value keeps the length")
    }

    /// Returns the nearest integer, with halves rounded toward positive infinity, `fn:round`.
    pub fn round(self) -> Decimal {
        self.to_integral(Rounding::Nearest)
    }

    /// Returns the smallest integer not below the value, `fn:ceiling`.
    pub fn ceil(self) -> Decimal {
        self.to_integral(Rounding::Up)
    }

    /// Returns the largest integer not above the value, `fn:floor`.
    pub fn floor(self) -> Decimal {
        self.to_integral(Rounding::Down)
    }

    /// Returns the value rounded to an integer by `rounding`.
    pub(crate) fn to_integral(self, rounding: Rounding) -> Decimal {
        if self.is_integral() {
            return self.to_owned();
        }
        match workspace::Fixed::of(self).and_then(|fixed| fixed.to_integral(rounding)) {
            Some(integral) => Decimal::new(integral, 0).expect("an integral part is shorter"),
            None => big_integral(self, rounding),
        }
    }
}

/// Forwards a binary operator on owned exact numbers to the [view](Decimal::as_ref).
macro_rules! forward_binary {
    ($trait:ident, $method:ident) => {
        impl $trait for &Decimal {
            type Output = Result<Decimal, EvaluationError>;

            /// Forwards to the operator on [DecimalRef].
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

impl Decimal {
    /// Returns the value raised to a power, as [DecimalRef::pow] does.
    pub fn pow(&self, power: u32) -> Result<Decimal, EvaluationError> {
        self.as_ref().pow(power)
    }

    /// Returns the absolute value, as [DecimalRef::abs] does.
    pub fn abs(&self) -> Decimal {
        self.as_ref().abs()
    }

    /// Returns the nearest integer, as [DecimalRef::round] does.
    pub fn round(&self) -> Decimal {
        self.as_ref().round()
    }

    /// Returns the smallest integer not below the value, as [DecimalRef::ceil] does.
    pub fn ceil(&self) -> Decimal {
        self.as_ref().ceil()
    }

    /// Returns the largest integer not above the value, as [DecimalRef::floor] does.
    pub fn floor(&self) -> Decimal {
        self.as_ref().floor()
    }
}

impl Neg for &Decimal {
    type Output = Decimal;

    /// Returns the negated value, `op:numeric-unary-minus`, as the negation of a [DecimalRef] does.
    fn neg(self) -> Decimal {
        -self.as_ref()
    }
}

impl Neg for Decimal {
    type Output = Decimal;

    /// Returns the negated value, `op:numeric-unary-minus`, as the negation of a [DecimalRef] does.
    fn neg(self) -> Decimal {
        -self.as_ref()
    }
}

/// Two exact numbers in the smallest workspace that can represent both.
enum Operands {
    /// Both are integers within the 64-bit range.
    Int(i64, i64),
    /// Both lie within the compact ranges, and at least one is a compact decimal.
    Fixed(workspace::Fixed, workspace::Fixed),
    /// At least one is wide.
    Big(workspace::Big, workspace::Big),
}

/// Returns two exact numbers in the smallest workspace that can represent both.
fn operands(left: DecimalRef<'_>, right: DecimalRef<'_>) -> Operands {
    if let (ReprRef::Int(left), ReprRef::Int(right)) = (left.0, right.0) {
        return Operands::Int(left, right);
    }
    match (workspace::Fixed::of(left), workspace::Fixed::of(right)) {
        (Some(left), Some(right)) => Operands::Fixed(left, right),
        _ => Operands::Big(big(left), big(right)),
    }
}

/// Returns the big workspace form of an exact number.
#[cold]
pub(super) fn big(value: DecimalRef<'_>) -> workspace::Big {
    workspace::Big::from_decimal(value)
}

/// Returns the result, or [Overflow](EvaluationError::Overflow) for `None`,
/// which stands for a result beyond the length limit.
fn bounded(result: Option<Decimal>) -> Result<Decimal, EvaluationError> {
    result.ok_or(EvaluationError::Overflow)
}

/// Applies an operation to two workspace values in the big workspace.
#[cold]
fn big_binary(
    left: workspace::Fixed,
    right: workspace::Fixed,
    apply: impl FnOnce(&workspace::Big, &workspace::Big) -> Option<workspace::Big>,
) -> Option<Decimal> {
    apply(
        &workspace::Big::from_fixed(left)?,
        &workspace::Big::from_fixed(right)?,
    )
    .map(Decimal::from)
}

/// Adds or subtracts two aligned mantissas in the big workspace,
/// for a result that does not fit into the `i128` workspace.
#[cold]
fn big_sum(left: i128, right: i128, exponent: i64, subtract: bool) -> Option<Decimal> {
    big_binary(
        workspace::Fixed::new(left, exponent),
        workspace::Fixed::new(right, exponent),
        if subtract {
            workspace::Big::sub
        } else {
            workspace::Big::add
        },
    )
}

/// Divides two `i128` workspace values,
/// in the big workspace if the scaled numerator does not fit.
fn fixed_quotient(left: workspace::Fixed, right: workspace::Fixed) -> Option<Decimal> {
    match left.quotient(right) {
        Some(quotient) => Decimal::new(quotient.mantissa(), quotient.exponent()),
        None => big_binary(left, right, workspace::Big::div),
    }
}

/// Rounds a decimal to an integer in the big workspace.
#[cold]
fn big_integral(value: DecimalRef<'_>, rounding: Rounding) -> Decimal {
    let value = big(value);
    Decimal::from(match rounding {
        Rounding::Nearest => value.round(),
        Rounding::Up => value.ceil(),
        Rounding::Down => value.floor(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Returns the value of a well-formed lexical form.
    fn value(lexical: &str) -> Decimal {
        Decimal::from_lexical(lexical).expect("well-formed")
    }

    /// The operators on exact numbers give exact results.
    #[test]
    fn operators_stay_exact() {
        let (seven, two) = (value("7"), value("2"));

        assert_eq!(&seven + &two, Ok(value("9")));
        assert_eq!(&seven - &two, Ok(value("5")));
        assert_eq!(&seven * &two, Ok(value("14")));
        assert_eq!(&seven / &two, Ok(value("3.5")));
        assert_eq!(&seven % &two, Ok(value("1")));
        assert_eq!(-&seven, value("-7"));
        assert_eq!(&value("0.1") + &value("0.2"), Ok(value("0.3")));
        assert_eq!(two.pow(100), Ok(value("1267650600228229401496703205376")));
        assert_eq!(value("-2.5").abs(), value("2.5"));
        assert_eq!(value("2.5").round(), value("3"));
        assert_eq!(value("-2.5").round(), value("-2"));
        assert_eq!(value("2.5").ceil(), value("3"));
        assert_eq!(value("2.5").floor(), value("2"));
    }

    /// The operators fail with the error that names the cause.
    #[test]
    fn operators_name_their_errors() {
        let zero = value("0");

        assert_eq!(&value("1") / &zero, Err(EvaluationError::DivisionByZero));
        assert_eq!(&value("1") % &zero, Err(EvaluationError::DivisionByZero));
        assert_eq!(
            &value("7.5") % &value("2"),
            Err(EvaluationError::TypeMismatch)
        );
        assert_eq!(value("3").pow(u32::MAX), Err(EvaluationError::Overflow));
    }
}
