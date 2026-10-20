//! Numeric operations: the numeric functions under the error policy of the builtins.
//!
//! The operations are delegated to [ValueRef].
//! Its four arithmetic operators also compute with durations and points in time.

#[cfg(doc)]
use std::ops::{Add, Div, Mul, Sub};

use crate::{
    function::StackValue,
    value::{
        DataValue, Decimal, DecimalRef, EvaluationError, NumberRef, ValueRef,
        number::decimal::bitwise::Bitwise,
    },
};

/// Adds two numbers, two durations, or a duration to a point in time; see [ValueRef::add].
pub(crate) fn add(left: ValueRef<'_>, right: ValueRef<'_>) -> StackValue<'static> {
    (left + right).into()
}

/// Subtracts the second value from the first; see [ValueRef::sub].
pub(crate) fn subtract(left: ValueRef<'_>, right: ValueRef<'_>) -> StackValue<'static> {
    (left - right).into()
}

/// Multiplies two numbers, or a duration by a number; see [ValueRef::mul].
pub(crate) fn multiply(left: ValueRef<'_>, right: ValueRef<'_>) -> StackValue<'static> {
    (left * right).into()
}

/// Divides the first value by the second; see [ValueRef::div].
pub(crate) fn divide(left: ValueRef<'_>, right: ValueRef<'_>) -> StackValue<'static> {
    (left / right).into()
}

/// Negates a number.
pub(crate) fn negate(value: ValueRef<'_>) -> StackValue<'static> {
    (-value).into()
}

/// Returns the absolute value of a number, SPARQL's `ABS`,
/// [`fn:abs`](https://www.w3.org/TR/xpath-functions-31/#func-abs).
pub(crate) fn absolute(value: ValueRef<'_>) -> StackValue<'static> {
    unary(value, NumberRef::abs)
}

/// Rounds a number to the nearest integer value, halves toward positive infinity,
/// SPARQL's `ROUND`, [`fn:round`](https://www.w3.org/TR/xpath-functions-31/#func-round).
///
/// The result stays in the number's domain: an integer stays an integer, a float a float.
pub(crate) fn round(value: ValueRef<'_>) -> StackValue<'static> {
    unary(value, NumberRef::round)
}

/// Rounds a number up to the nearest integer value within its own domain,
/// SPARQL's `CEIL`, [`fn:ceiling`](https://www.w3.org/TR/xpath-functions-31/#func-ceiling).
pub(crate) fn ceiling(value: ValueRef<'_>) -> StackValue<'static> {
    unary(value, NumberRef::ceil)
}

/// Rounds a number down to the nearest integer value within its own domain,
/// SPARQL's `FLOOR`, [`fn:floor`](https://www.w3.org/TR/xpath-functions-31/#func-floor).
pub(crate) fn floor(value: ValueRef<'_>) -> StackValue<'static> {
    unary(value, NumberRef::floor)
}

/// Applies a function of the number type to a number.
fn unary<'a>(
    value: ValueRef<'a>,
    operation: impl FnOnce(NumberRef<'a>) -> DataValue,
) -> StackValue<'static> {
    let ValueRef::Number(value) = value else {
        return EvaluationError::TypeMismatch.into();
    };
    operation(value).into()
}

/// Raises the first number to the second.
///
/// An exact base under a non-negative integer exponent computes exactly;
/// every other pair computes as doubles,
/// as [`math:pow`](https://www.w3.org/TR/xpath-functions-31/#func-math-pow) does throughout,
/// under an integer exponent as IEEE's `pown`.
pub(crate) fn power(base: ValueRef<'_>, exponent: ValueRef<'_>) -> StackValue<'static> {
    let (ValueRef::Number(base), ValueRef::Number(exponent)) = (base, exponent) else {
        return EvaluationError::TypeMismatch.into();
    };

    let exact = !matches!(base, NumberRef::Float(_) | NumberRef::Double(_));
    if exact && exponent.is_integral() && !exponent.is_negative() {
        return exact_power(base, exponent).into();
    }

    let base = base.to_f64();
    let result = base.powf(exponent.to_f64());

    // `pown` gives an odd power the sign of the base,
    // which `powf` misses beyond 2^53, where the exponent converts to an even double.
    let result = if exponent.is_odd() {
        result.copysign(base)
    } else {
        result
    };

    DataValue::double(result).into()
}

/// Raises an exact base to a non-negative integer exponent, exactly.
///
/// Beyond an exponent of 32 bits only the bases 0, 1 and -1 stay within the length limit,
/// and their powers repeat with the exponent's parity.
fn exact_power(base: NumberRef<'_>, exponent: NumberRef<'_>) -> Result<DataValue, EvaluationError> {
    if let Some(power) = exponent
        .to_i64()
        .and_then(|power| u32::try_from(power).ok())
    {
        return base.pow(power);
    }

    match base.to_i64() {
        Some(-1..=1) => base.pow(if exponent.is_odd() { 1 } else { 2 }),
        _ => Err(EvaluationError::Overflow),
    }
}

/// Returns the logarithm of the first number in the base of the second, as a double.
///
/// XPath's [`math:log`](https://www.w3.org/TR/xpath-functions-31/#func-math-log)
/// is the natural logarithm alone; this takes the base explicitly.
pub(crate) fn logarithm(left: ValueRef<'_>, right: ValueRef<'_>) -> StackValue<'static> {
    let (ValueRef::Number(left), ValueRef::Number(right)) = (left, right) else {
        return EvaluationError::TypeMismatch.into();
    };

    DataValue::double(left.to_f64().log(right.to_f64())).into()
}

/// Returns the sine of a number.
pub(crate) fn sine(value: ValueRef<'_>) -> StackValue<'static> {
    floating_unary(value, f64::sin)
}

/// Returns the cosine of a number.
pub(crate) fn cosine(value: ValueRef<'_>) -> StackValue<'static> {
    floating_unary(value, f64::cos)
}

/// Returns the tangent of a number.
pub(crate) fn tangent(value: ValueRef<'_>) -> StackValue<'static> {
    floating_unary(value, f64::tan)
}

/// Returns the square root of a number.
pub(crate) fn square_root(value: ValueRef<'_>) -> StackValue<'static> {
    floating_unary(value, f64::sqrt)
}

/// Applies a floating-point function to a number:
/// a float stays a float, every other number computes as a double.
///
/// The functions are those of XPath's
/// [`math` namespace](https://www.w3.org/TR/xpath-functions-31/#trigonometry),
/// which compute as doubles throughout.
fn floating_unary(value: ValueRef<'_>, apply: impl Fn(f64) -> f64) -> StackValue<'static> {
    let ValueRef::Number(value) = value else {
        return EvaluationError::TypeMismatch.into();
    };

    let result = apply(value.to_f64());

    match value {
        NumberRef::Float(_) => {
            // The float's own precision is what the operand had.
            #[allow(clippy::cast_possible_truncation)]
            DataValue::float(result as f32)
        }
        _ => DataValue::double(result),
    }
    .into()
}

/// Returns the remainder of integer division, with the sign of the dividend,
/// XPath's [`op:numeric-mod`](https://www.w3.org/TR/xpath-functions-31/#func-numeric-mod)
/// restricted to integers.
pub(crate) fn remainder(left: ValueRef<'_>, right: ValueRef<'_>) -> StackValue<'static> {
    (left % right).into()
}

/// Shifts the first integer's bits left by the second, `x * 2^n`; see [DecimalRef::shift_left].
///
/// The shifts, the bitwise connectives, and the t-norm below have no SPARQL counterpart.
pub(crate) fn shift_left(left: ValueRef<'_>, right: ValueRef<'_>) -> StackValue<'static> {
    exact_binary(left, right, DecimalRef::shift_left)
}

/// Shifts the first integer's bits right by the second, keeping the sign, `⌊x / 2^n⌋`;
/// see [DecimalRef::shift_right].
pub(crate) fn shift_right(left: ValueRef<'_>, right: ValueRef<'_>) -> StackValue<'static> {
    exact_binary(left, right, DecimalRef::shift_right)
}

/// Applies a binary operation of exact numbers to two of them.
fn exact_binary<'a>(
    left: ValueRef<'a>,
    right: ValueRef<'a>,
    operation: impl FnOnce(DecimalRef<'a>, DecimalRef<'a>) -> Result<Decimal, EvaluationError>,
) -> StackValue<'static> {
    let (ValueRef::Number(NumberRef::Decimal(left)), ValueRef::Number(NumberRef::Decimal(right))) =
        (left, right)
    else {
        return EvaluationError::TypeMismatch.into();
    };
    operation(left, right).map(DataValue::from).into()
}

/// Returns the bitwise and of integers.
pub(crate) fn bit_and(values: &[StackValue<'_>]) -> StackValue<'static> {
    bitwise(values, Bitwise::And)
}

/// Returns the bitwise or of integers.
pub(crate) fn bit_or(values: &[StackValue<'_>]) -> StackValue<'static> {
    bitwise(values, Bitwise::Or)
}

/// Returns the bitwise exclusive or of integers.
pub(crate) fn bit_xor(values: &[StackValue<'_>]) -> StackValue<'static> {
    bitwise(values, Bitwise::Xor)
}

/// Folds a bitwise connective over integers; see [DecimalRef::bitwise].
fn bitwise(values: &[StackValue<'_>], connective: Bitwise) -> StackValue<'static> {
    let mut folded: Option<Decimal> = None;
    for value in values {
        let value = match value.as_result() {
            Ok(ValueRef::Number(NumberRef::Decimal(value))) if value.is_integral() => value,
            Ok(_) => return EvaluationError::TypeMismatch.into(),
            Err(error) => return error.into(),
        };
        folded = Some(match folded {
            None => value.to_owned(),
            Some(folded) => match folded.as_ref().bitwise(value, connective) {
                Ok(result) => result,
                Err(error) => return error.into(),
            },
        });
    }
    match folded {
        Some(value) => DataValue::from(value).into(),
        None => EvaluationError::TypeMismatch.into(),
    }
}

/// Returns the Łukasiewicz t-norm of numbers, `max(0, a₁ + … + aₙ - (n - 1))`, as a double.
///
/// The n-ary form does not depend on the order of the arguments, also outside [0, 1],
/// where folding the binary t-norm would.
pub(crate) fn lukasiewicz(values: &[StackValue<'_>]) -> StackValue<'static> {
    if values.is_empty() {
        return EvaluationError::TypeMismatch.into();
    }

    let mut sum = 0.0;
    for value in values {
        match value.as_result() {
            Ok(ValueRef::Number(value)) => sum += value.to_f64(),
            Ok(_) => return EvaluationError::TypeMismatch.into(),
            Err(error) => return error.into(),
        }
    }

    // A count of arguments is far below the precision of a double.
    #[allow(clippy::cast_precision_loss)]
    let excess = sum - (values.len() - 1) as f64;
    // A NaN fails the comparison and stays.
    DataValue::double(if excess < 0.0 { 0.0 } else { excess }).into()
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::testing::number;

    #[test]
    fn the_policy_names_the_errors() {
        let apply = |operation: fn(ValueRef<'_>, ValueRef<'_>) -> StackValue<'static>,
                     left: &DataValue,
                     right: &DataValue| {
            operation(left.as_ref(), right.as_ref())
                .as_result()
                .map(ValueRef::to_owned)
        };
        let int = DataValue::integer;
        assert_eq!(
            apply(divide, &int(1), &int(0)),
            Err(EvaluationError::DivisionByZero)
        );
        // A floating zero divisor is no error: the quotient is an infinity or the NaN.
        assert_eq!(
            apply(divide, &DataValue::double(1.0), &int(0)),
            Ok(DataValue::double(f64::INFINITY))
        );
        assert_eq!(
            apply(divide, &int(0), &DataValue::float(0.0)),
            Ok(DataValue::float(f32::NAN))
        );
        assert_eq!(
            apply(remainder, &int(1), &int(0)),
            Err(EvaluationError::DivisionByZero)
        );
        assert_eq!(
            apply(remainder, &number("1.5"), &int(1)),
            Err(EvaluationError::TypeMismatch)
        );
        assert_eq!(
            apply(add, &int(1), &DataValue::string("1")),
            Err(EvaluationError::TypeMismatch)
        );
        assert_eq!(
            apply(
                multiply,
                &DataValue::double(1e300),
                &DataValue::double(1e300)
            ),
            Ok(DataValue::double(f64::INFINITY))
        );
        // A negative exponent leaves the exact path for the doubles.
        assert_eq!(
            apply(power, &int(2), &DataValue::integer(-3)),
            Ok(DataValue::double(0.125))
        );
        assert_eq!(apply(power, &int(2), &int(10)), Ok(int(1024)));
        assert_eq!(
            apply(power, &int(3), &int(i64::from(u32::MAX))),
            Err(EvaluationError::Overflow)
        );
    }

    #[test]
    fn powers_are_exact_for_exact_bases_and_pown_otherwise() {
        let apply = |base: &DataValue, exponent: &DataValue| {
            power(base.as_ref(), exponent.as_ref())
                .as_result()
                .map(ValueRef::to_owned)
        };
        let (int, double) = (DataValue::integer, DataValue::double);

        // A decimal base stays exact like an integral one.
        assert_eq!(apply(&number("1.5"), &int(2)), Ok(number("2.25")));
        assert_eq!(apply(&number("-0.5"), &int(3)), Ok(number("-0.125")));

        // Beyond an exponent of 32 bits only 0, 1 and -1 stay within the length limit,
        // at any width of the exponent.
        let wide_odd = number("100000000000000000001");
        assert_eq!(apply(&int(1), &int(5_000_000_000)), Ok(int(1)));
        assert_eq!(apply(&int(0), &int(5_000_000_000)), Ok(int(0)));
        assert_eq!(apply(&int(-1), &wide_odd), Ok(int(-1)));
        assert_eq!(
            apply(&int(2), &int(5_000_000_000)),
            Err(EvaluationError::Overflow)
        );
        assert_eq!(apply(&int(2), &wide_odd), Err(EvaluationError::Overflow));

        // A floating base under an odd exponent keeps its sign, as `pown` has it,
        // also where no double holds the exponent.
        let odd = int(9_007_199_254_740_993);
        assert_eq!(apply(&double(-1.0), &odd), Ok(double(-1.0)));
        assert_eq!(apply(&double(-2.0), &odd), Ok(double(f64::NEG_INFINITY)));
        assert_eq!(apply(&double(-0.5), &odd), Ok(double(-0.0)));
        assert_eq!(apply(&double(-1.0), &wide_odd), Ok(double(-1.0)));
        assert_eq!(
            apply(&double(-0.0), &int(-3)),
            Ok(double(f64::NEG_INFINITY))
        );
    }

    #[test]
    fn floating_results_follow_ieee() {
        let apply = |operation: fn(ValueRef<'_>) -> StackValue<'static>, value: &DataValue| {
            operation(value.as_ref())
                .as_result()
                .map(ValueRef::to_owned)
        };
        assert_eq!(
            apply(square_root, &DataValue::integer(-1)),
            Ok(DataValue::double(f64::NAN))
        );
        assert_eq!(
            apply(negate, &DataValue::double(f64::INFINITY)),
            Ok(DataValue::double(f64::NEG_INFINITY))
        );
        assert_eq!(
            apply(square_root, &DataValue::integer(4)),
            Ok(DataValue::double(2.0))
        );
        assert_eq!(
            apply(square_root, &DataValue::float(4.0)),
            Ok(DataValue::float(2.0))
        );
        assert_eq!(
            apply(round, &DataValue::float(2.5)),
            Ok(DataValue::float(3.0))
        );
        assert_eq!(
            apply(negate, &DataValue::integer(i64::MIN)),
            Ok(number("9223372036854775808"))
        );
    }
}
