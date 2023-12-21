//! This module defines operations on 64-bit integers.

use num::integer::Roots;

use crate::datavalues::AnyDataValue;

/// Addition of 64-bit integers
///
/// Doesn't return a value if the operation results in an value outside the 64-bit integer range.s
pub(super) fn numeric_addition_integer64(
    parameter_first: i64,
    parameter_second: i64,
) -> Option<AnyDataValue> {
    Some(AnyDataValue::new_integer_from_i64(
        parameter_first.checked_add(parameter_second)?,
    ))
}

/// Subtraction of 64-bit integers
///
/// Doesn't return a value if the operation results in an value outside the 64-bit integer range.s
pub(super) fn numeric_subtraction_integer64(
    parameter_first: i64,
    parameter_second: i64,
) -> Option<AnyDataValue> {
    Some(AnyDataValue::new_integer_from_i64(
        parameter_first.checked_sub(parameter_second)?,
    ))
}

/// Multiplication of 64-bit integers
///
/// Doesn't return a value if the operation results in an value outside the 64-bit integer range.s
pub(super) fn numeric_multiplication_integer64(
    parameter_first: i64,
    parameter_second: i64,
) -> Option<AnyDataValue> {
    Some(AnyDataValue::new_integer_from_i64(
        parameter_first.checked_mul(parameter_second)?,
    ))
}

/// Division of 64-bit integers
///
/// Doesn't return a value if the operation results in an value outside the 64-bit integer range.s
pub(super) fn numeric_division_integer64(
    parameter_first: i64,
    parameter_second: i64,
) -> Option<AnyDataValue> {
    Some(AnyDataValue::new_integer_from_i64(
        parameter_first.checked_div(parameter_second)?,
    ))
}

/// Absolute value of a 64-bit integer
///
/// Doesn't return a value if the operation results in an value outside the 64-bit integer range.s
pub(super) fn numeric_absolute_integer64(parameter: i64) -> Option<AnyDataValue> {
    Some(AnyDataValue::new_integer_from_i64(parameter.abs()))
}

/// Negation of a 64-bit integer
pub(super) fn numeric_negation_integer64(parameter: i64) -> Option<AnyDataValue> {
    Some(AnyDataValue::new_integer_from_i64(parameter.checked_neg()?))
}

/// Square root of 64-bit integer - rounded down
///
/// Returns `None` if parameter was negative
pub(super) fn numeric_squareroot_integer64(parameter: i64) -> Option<AnyDataValue> {
    if parameter < 0 {
        return None;
    }

    Some(AnyDataValue::new_integer_from_i64(Roots::sqrt(&parameter)))
}

/// Logarithm of a 64-bit integer given some base
///
/// The first parameter is the input value to the logarithm and the second its base.
/// Returns `None` if the value is less than or equal to zero, or if base is less than 2.
pub(super) fn numeric_logarithm_integer64(value: i64, base: i64) -> Option<AnyDataValue> {
    if value <= 0 || base < 2 {
        return None;
    }

    Some(AnyDataValue::new_integer_from_i64(value.ilog(base).into()))
}

/// Raising a 64-bit integer to some integer power
///
/// The first parameter is the base and the second is the exponent.
/// Returns `None` if the result cannot be represented by an 64-bit integer.
pub(super) fn numeric_power_integer64(base: i64, exponent: i64) -> Option<AnyDataValue> {
    if exponent < 0 {
        return None;
    }

    Some(AnyDataValue::new_integer_from_i64(num::checked_pow(
        base,
        exponent.try_into().ok()?,
    )?))
}

/// Less than comparison between 64-bit integers
pub(super) fn numeric_lessthan_integer64(
    parameter_first: i64,
    parameter_second: i64,
) -> Option<AnyDataValue> {
    if parameter_first < parameter_second {
        Some(AnyDataValue::new_boolean(true))
    } else {
        Some(AnyDataValue::new_boolean(false))
    }
}

/// Less than or equals comparison between 64-bit integers
pub(super) fn numeric_lessthaneq_integer64(
    parameter_first: i64,
    parameter_second: i64,
) -> Option<AnyDataValue> {
    if parameter_first <= parameter_second {
        Some(AnyDataValue::new_boolean(true))
    } else {
        Some(AnyDataValue::new_boolean(false))
    }
}

/// Greater than comparison between 64-bit integers
pub(super) fn numeric_greaterthan_integer64(
    parameter_first: i64,
    parameter_second: i64,
) -> Option<AnyDataValue> {
    if parameter_first > parameter_second {
        Some(AnyDataValue::new_boolean(true))
    } else {
        Some(AnyDataValue::new_boolean(false))
    }
}

/// Greater than or equals comparison between 64-bit integers
pub(super) fn numeric_greaterthaneq_integer64(
    parameter_first: i64,
    parameter_second: i64,
) -> Option<AnyDataValue> {
    if parameter_first >= parameter_second {
        Some(AnyDataValue::new_boolean(true))
    } else {
        Some(AnyDataValue::new_boolean(false))
    }
}
