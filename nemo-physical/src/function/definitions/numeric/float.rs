//! This module defines operations on 32-bit floating point numbers.

use num::{CheckedAdd, CheckedDiv, CheckedMul, CheckedSub, traits::CheckedNeg};

use crate::{
    datatypes::Float,
    datavalues::{AnyDataValue, FloatDataValue},
};

use super::traits::{CheckedPow, CheckedSquareRoot};

/// Helper function to build results within this module. This might become
/// obsolete or more elegant once we converge to a more uniform representation
/// of data values (in particular replacing Float by FloatDataValue).
fn some_datavalue_from_float(d: Float) -> Option<AnyDataValue> {
    Some(FloatDataValue::from_f32_unchecked(f32::from(d)).into())
}

/// Addition of 32-bit floating point numbers
///
/// Returns `None` if the resulting value is not representable as a [Float].
pub(super) fn numeric_addition_float(
    parameter_first: Float,
    parameter_second: Float,
) -> Option<AnyDataValue> {
    some_datavalue_from_float(parameter_first.checked_add(&parameter_second)?)
}

/// Subtraction of 32-bit floating point numbers
///
/// Returns `None` if the resulting value is not representable as a [Float].
pub(super) fn numeric_subtraction_float(
    parameter_first: Float,
    parameter_second: Float,
) -> Option<AnyDataValue> {
    some_datavalue_from_float(parameter_first.checked_sub(&parameter_second)?)
}

/// Multiplication of 32-bit floating point numbers
///
/// Returns `None` if the resulting value is not representable as a [Float].
pub(super) fn numeric_multiplication_float(
    parameter_first: Float,
    parameter_second: Float,
) -> Option<AnyDataValue> {
    some_datavalue_from_float(parameter_first.checked_mul(&parameter_second)?)
}

/// Division of 32-bit floating point numbers
///
/// Returns `None` if the resulting value is not representable as a [Float].
pub(super) fn numeric_division_float(
    parameter_first: Float,
    parameter_second: Float,
) -> Option<AnyDataValue> {
    some_datavalue_from_float(parameter_first.checked_div(&parameter_second)?)
}

/// Absolute value of a 32-bit floating point number
pub(super) fn numeric_absolute_float(parameter: Float) -> Option<AnyDataValue> {
    some_datavalue_from_float(parameter.abs())
}

/// Negation of a 32-bit floating point number
pub(super) fn numeric_negation_float(parameter: Float) -> Option<AnyDataValue> {
    some_datavalue_from_float(parameter.checked_neg()?)
}

/// Square root of 32-bit floating point number
///
/// Returns `None` if parameter was negative
pub(super) fn numeric_squareroot_float(parameter: Float) -> Option<AnyDataValue> {
    some_datavalue_from_float(parameter.checked_sqrt()?)
}

/// Logarithm of a 32-bit floating point number given some base
pub(super) fn numeric_logarithm_float(value: Float, base: Float) -> Option<AnyDataValue> {
    some_datavalue_from_float(value.log(base)?)
}

/// Sine of 32-bit floating point number
pub(super) fn numeric_sin_float(parameter: Float) -> Option<AnyDataValue> {
    some_datavalue_from_float(parameter.sin())
}

/// Cosine of 32-bit floating point number
pub(super) fn numeric_cos_float(parameter: Float) -> Option<AnyDataValue> {
    some_datavalue_from_float(parameter.cos())
}

/// Tangent of 32-bit floating point number
pub(super) fn numeric_tan_float(parameter: Float) -> Option<AnyDataValue> {
    some_datavalue_from_float(parameter.tan())
}

/// Raising a 32-bit float to some power
///
/// The first parameter is the base and the second is the exponent.
/// Returns `None` if the resulting value is not representable as a [Float].
pub(super) fn numeric_power_float(base: Float, exponent: Float) -> Option<AnyDataValue> {
    some_datavalue_from_float(base.checked_pow(exponent)?)
}

/// Remainder operation
///
/// Returns the remainder of the (truncated) division `parameter_first / parameter.second`.
/// The value of the result has always the same sign as `paramter_second`.
///
/// Returns `None` if `parameter_second` is zero.
pub(super) fn numeric_remainder_float(
    parameter_first: Float,
    parameter_second: Float,
) -> Option<AnyDataValue> {
    if parameter_second == Float::new(0.0).expect("Zero is not NaN/inf") {
        return None;
    }

    some_datavalue_from_float(parameter_first % parameter_second)
}

/// Less than comparison between 32-bit floating point numbers
pub(super) fn numeric_lessthan_float(
    parameter_first: Float,
    parameter_second: Float,
) -> Option<AnyDataValue> {
    if parameter_first < parameter_second {
        Some(AnyDataValue::new_boolean(true))
    } else {
        Some(AnyDataValue::new_boolean(false))
    }
}

/// Less than or equals comparison between 32-bit floating point numbers
pub(super) fn numeric_lessthaneq_float(
    parameter_first: Float,
    parameter_second: Float,
) -> Option<AnyDataValue> {
    if parameter_first <= parameter_second {
        Some(AnyDataValue::new_boolean(true))
    } else {
        Some(AnyDataValue::new_boolean(false))
    }
}

/// Greater than comparison between 32-bit floating point numbers
pub(super) fn numeric_greaterthan_float(
    parameter_first: Float,
    parameter_second: Float,
) -> Option<AnyDataValue> {
    if parameter_first > parameter_second {
        Some(AnyDataValue::new_boolean(true))
    } else {
        Some(AnyDataValue::new_boolean(false))
    }
}

/// Greater than or equals comparison between 32-bit floating point numbers
pub(super) fn numeric_greaterthaneq_float(
    parameter_first: Float,
    parameter_second: Float,
) -> Option<AnyDataValue> {
    if parameter_first >= parameter_second {
        Some(AnyDataValue::new_boolean(true))
    } else {
        Some(AnyDataValue::new_boolean(false))
    }
}

/// Rounding to the nearest integer.
/// If the result is half-way between two integers, round away from 0.0.
pub(super) fn numeric_round_float(parameter: Float) -> Option<AnyDataValue> {
    some_datavalue_from_float(parameter.round())
}

/// Rounding up to the smallest integer less than or equal to `parameter`.
pub(super) fn numeric_ceil_float(parameter: Float) -> Option<AnyDataValue> {
    some_datavalue_from_float(parameter.ceil())
}

/// Rounding down to the largest integer less than or equal to `parameter`.
pub(super) fn numeric_floor_float(parameter: Float) -> Option<AnyDataValue> {
    some_datavalue_from_float(parameter.floor())
}

/// Return the sum of the given 32-bit floating point numbers.
///
/// Returns `None` if the result (or a intermediate result) is not representable as a 32-bit float.
/// Returns the floating point number zero if no parameters are given.
pub(super) fn numeric_sum_float(parameters: &[Float]) -> Option<AnyDataValue> {
    let mut sum = Float::from_number(0.0);

    for parameter in parameters {
        sum = sum.checked_add(parameter)?;
    }

    some_datavalue_from_float(sum)
}

/// Return the product of the given 32-bit floating point numbers.
///
/// Returns `None` if the result (or a intermediate result) is not representable as a 32-bit float.
/// Returns the floating point number one if no parameters are given.
pub(super) fn numeric_product_float(parameters: &[Float]) -> Option<AnyDataValue> {
    let mut product = Float::from_number(1.0);

    for parameter in parameters {
        product = product.checked_mul(parameter)?;
    }

    some_datavalue_from_float(product)
}

/// Return the minimum of the given 32-bit floating point numbers.
///
/// Returns `None` if no parameters are given.
pub(super) fn numeric_minimum_float(parameters: &[Float]) -> Option<AnyDataValue> {
    let min = *parameters.iter().min()?;

    some_datavalue_from_float(min)
}

/// Return the maximum of the given 32-bit floating point numbers.
///
/// Returns `None` if no parameters are given.
pub(super) fn numeric_maximum_float(parameters: &[Float]) -> Option<AnyDataValue> {
    let max = *parameters.iter().max()?;

    some_datavalue_from_float(max)
}

/// Return the Lukasiewicz t-norm of the given 32-bit floating point numbers.
///
/// Returns `None` if no parameters are given
/// or the result (or an intermediate value) is not representable as a 32-bit float.
pub(super) fn numeric_tnorm_lukasiewicz_float(parameters: &[Float]) -> Option<AnyDataValue> {
    if parameters.is_empty() {
        return None;
    }

    let mut sum = Float::from_number(0.0);

    for parameter in parameters {
        sum = sum.checked_add(parameter)?;
    }

    let result =
        Float::from_number(0.0).max(sum.checked_sub(&Float::try_from(parameters.len() - 1).ok()?)?);

    some_datavalue_from_float(result)
}
