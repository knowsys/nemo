//! This module defines operations on 64-bit integers.

use std::ops::{BitAnd, BitOr, BitXor};

use num::integer::Roots;

use crate::datavalues::AnyDataValue;

/// Addition of 64-bit integers
///
/// Doesn't return a value if the operation results in an value outside the 64-bit integer range.
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
/// Doesn't return a value if the operation results in an value outside the 64-bit integer range.
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
/// Doesn't return a value if the operation results in an value outside the 64-bit integer range.
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
/// Doesn't return a value if the operation results in an value outside the 64-bit integer range.
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
/// Doesn't return a value if the operation results in an value outside the 64-bit integer range.
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

/// Remainder operation
///
/// Returns the remainder of the (truncated) division `parameter_first / parameter.second`.
/// The value of the result has always the same sign as `paramter_second`.
///
/// Returns `None` if `parameter_second` is zero.
pub(super) fn numeric_remainder_integer64(
    parameter_first: i64,
    parameter_second: i64,
) -> Option<AnyDataValue> {
    if parameter_second == 0 {
        return None;
    }

    Some(AnyDataValue::new_integer_from_i64(
        parameter_first % parameter_second,
    ))
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

/// Bitwise and operation for 64-bit integers
///
/// Returns the maximum 64-bit integer value if no parameters are given.
pub(super) fn numeric_bitwise_and(parameters: &[i64]) -> Option<AnyDataValue> {
    let mut result = i64::MAX;

    for parameter in parameters {
        result = result.bitand(parameter);
    }

    Some(AnyDataValue::new_integer_from_i64(result))
}

/// Bitwise or operation for 64-bit integers
///
/// Returns zero from the integer value space if no parameters are given.
pub(super) fn numeric_bitwise_or(parameters: &[i64]) -> Option<AnyDataValue> {
    let mut result: i64 = 0;

    for parameter in parameters {
        result = result.bitor(parameter);
    }

    Some(AnyDataValue::new_integer_from_i64(result))
}

/// Bitwise xor operation for 64-bit integers
///
/// Returns zero from the integer value space if no parameters are given.
pub(super) fn numeric_bitwise_xor(parameters: &[i64]) -> Option<AnyDataValue> {
    let mut result: i64 = 0;

    for parameter in parameters {
        result = result.bitxor(parameter);
    }

    Some(AnyDataValue::new_integer_from_i64(result))
}

/// Left shift of 64-bit integer 'value' by 'base'-bits
///
/// Return 'None' if base is negative
pub(super) fn numeric_bitwise_shl(value: i64, base: i64) -> Option<AnyDataValue> {
    if base < 0 {
        return None;
    }

    Some(AnyDataValue::new_integer_from_i64(value << base))
}

/// Right shift of 64-bit integer 'value' by 'base'-bits
///
/// Arithmetic right shift preserves the sign-bit of the binary representation
///
/// Return 'None' if base is negative
pub(super) fn numeric_bitwise_shr(value: i64, base: i64) -> Option<AnyDataValue> {
    if base < 0 {
        return None;
    }

    Some(AnyDataValue::new_integer_from_i64(value >> base))
}

/// Right shift of 64-bit integer 'value' by 'base'-bits
///
/// Logical right shift does not preserve the sign-bit of the binary representation
///
/// Return 'None' if base is negative
pub(super) fn numeric_bitwise_shru(value: i64, base: i64) -> Option<AnyDataValue> {
    if base < 0 {
        return None;
    }

    // Apply arithmetic right shift, if value is positive
    let result: i64 = if value > 0 {
        value >> base
    } else {
        // Convert value in unsigned to do an unsigned right shift
        let unsigned_value = u64::from_ne_bytes(value.to_ne_bytes());
        let unsigned_result = unsigned_value >> base;

        i64::from_ne_bytes(unsigned_result.to_ne_bytes())
    };

    Some(AnyDataValue::new_integer_from_i64(result))
}

/// Return the sum of the given 64-bit integers.
///
/// Returns `None` if the result (or a intermediate result) does not fit into a 64-bit integer.
/// Returns zero from the integer value space if no parameters are given.
pub(super) fn numeric_sum_integer64(parameters: &[i64]) -> Option<AnyDataValue> {
    let mut sum: i64 = 0;

    for &parameter in parameters {
        sum = sum.checked_add(parameter)?;
    }

    Some(AnyDataValue::new_integer_from_i64(sum))
}

/// Return the product of the given 64-bit integers.
///
/// Returns `None` if the result (or a intermediate result) does not fit into a 64-bit integer.
/// Returns one from the integer value space if no parameters are given.
pub(super) fn numeric_product_integer64(parameters: &[i64]) -> Option<AnyDataValue> {
    let mut product: i64 = 1;

    for &parameter in parameters {
        product = product.checked_mul(parameter)?;
    }

    Some(AnyDataValue::new_integer_from_i64(product))
}

/// Return the minimum of the given 64-bit integers.
///
/// Returns `None` if no parameters are given.
pub(super) fn numeric_minimum_integer64(parameters: &[i64]) -> Option<AnyDataValue> {
    parameters
        .iter()
        .min()
        .map(|&min| AnyDataValue::new_integer_from_i64(min))
}

/// Return the maximum of the given 64-bit integers.
///
/// Returns `None` if no parameters are given.
pub(super) fn numeric_maximum_integer64(parameters: &[i64]) -> Option<AnyDataValue> {
    parameters
        .iter()
        .max()
        .map(|&max| AnyDataValue::new_integer_from_i64(max))
}
