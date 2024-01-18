//! This module defines operations on 64-bit floating point numbers.

use num::{traits::CheckedNeg, CheckedAdd, CheckedDiv, CheckedMul, CheckedSub};

use crate::{
    datatypes::Double,
    datavalues::{AnyDataValue, DoubleDataValue},
};

use super::traits::{CheckedPow, CheckedSquareRoot};

/// Helper function to build results within this module. This might become
/// obsolete or more elegant once we converge to a more uniform representation
/// of data values (in particular replacing Double by DoubleDataValue).
fn some_datavalue_from_double(d: Double) -> Option<AnyDataValue> {
    Some(DoubleDataValue::from_f64_unchecked(f64::from(d)).into())
}

/// Addition of 64-bit floating point numbers
///
/// Doesn't return a value if the operation results in NaN.
pub(super) fn numeric_addition_double(
    parameter_first: Double,
    parameter_second: Double,
) -> Option<AnyDataValue> {
    some_datavalue_from_double(parameter_first.checked_add(&parameter_second)?)
}

/// Subtraction of 64-bit floating point numbers
///
/// Doesn't return a value if the operation results in NaN.
pub(super) fn numeric_subtraction_double(
    parameter_first: Double,
    parameter_second: Double,
) -> Option<AnyDataValue> {
    some_datavalue_from_double(parameter_first.checked_sub(&parameter_second)?)
}

/// Multiplication of 64-bit floating point numbers
///
/// Doesn't return a value if the operation results in NaN.
pub(super) fn numeric_multiplication_double(
    parameter_first: Double,
    parameter_second: Double,
) -> Option<AnyDataValue> {
    some_datavalue_from_double(parameter_first.checked_mul(&parameter_second)?)
}

/// Division of 64-bit floating point numbers
///
/// Doesn't return a value if the operation results in NaN.
pub(super) fn numeric_division_double(
    parameter_first: Double,
    parameter_second: Double,
) -> Option<AnyDataValue> {
    some_datavalue_from_double(parameter_first.checked_div(&parameter_second)?)
}

/// Absolute value of a 64-bit floating point number
pub(super) fn numeric_absolute_double(parameter: Double) -> Option<AnyDataValue> {
    some_datavalue_from_double(parameter.abs())
}

/// Negation of a 64-bit floating point number
pub(super) fn numeric_negation_double(parameter: Double) -> Option<AnyDataValue> {
    some_datavalue_from_double(parameter.checked_neg()?)
}

/// Square root of 64-bit floating point number
///
/// Returns `None` if parameter was negative
pub(super) fn numeric_squareroot_double(parameter: Double) -> Option<AnyDataValue> {
    some_datavalue_from_double(parameter.checked_sqrt()?)
}

/// Logarithm of a 64-bit floating point number given some base
pub(super) fn numeric_logarithm_double(value: Double, base: Double) -> Option<AnyDataValue> {
    some_datavalue_from_double(value.log(base)?)
}

/// Sine of 64-bit floating point number
pub(super) fn numeric_sin_double(parameter: Double) -> Option<AnyDataValue> {
    some_datavalue_from_double(parameter.sin())
}

/// Cosine of 64-bit floating point number
pub(super) fn numeric_cos_double(parameter: Double) -> Option<AnyDataValue> {
    some_datavalue_from_double(parameter.cos())
}

/// Tangent of 64-bit floating point number
pub(super) fn numeric_tan_double(parameter: Double) -> Option<AnyDataValue> {
    some_datavalue_from_double(parameter.tan())
}

/// Raising a 64-bit float to some power
///
/// The first parameter is the base and the second is the exponent.
/// Returns `None` if the result cannot be represented by an 64-bit float.
pub(super) fn numeric_power_double(base: Double, exponent: Double) -> Option<AnyDataValue> {
    some_datavalue_from_double(base.checked_pow(exponent)?)
}

/// Less than comparison between 64-bit floating point numbers
pub(super) fn numeric_lessthan_double(
    parameter_first: Double,
    parameter_second: Double,
) -> Option<AnyDataValue> {
    if parameter_first < parameter_second {
        Some(AnyDataValue::new_boolean(true))
    } else {
        Some(AnyDataValue::new_boolean(false))
    }
}

/// Less than or equals comparison between 64-bit floating point numbers
pub(super) fn numeric_lessthaneq_double(
    parameter_first: Double,
    parameter_second: Double,
) -> Option<AnyDataValue> {
    if parameter_first <= parameter_second {
        Some(AnyDataValue::new_boolean(true))
    } else {
        Some(AnyDataValue::new_boolean(false))
    }
}

/// Greater than comparison between 64-bit floating point numbers
pub(super) fn numeric_greaterthan_double(
    parameter_first: Double,
    parameter_second: Double,
) -> Option<AnyDataValue> {
    if parameter_first > parameter_second {
        Some(AnyDataValue::new_boolean(true))
    } else {
        Some(AnyDataValue::new_boolean(false))
    }
}

/// Greater than or equals comparison between 64-bit floating point numbers
pub(super) fn numeric_greaterthaneq_double(
    parameter_first: Double,
    parameter_second: Double,
) -> Option<AnyDataValue> {
    if parameter_first >= parameter_second {
        Some(AnyDataValue::new_boolean(true))
    } else {
        Some(AnyDataValue::new_boolean(false))
    }
}
