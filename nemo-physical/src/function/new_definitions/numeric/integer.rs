//! This module implements the numeric functions for integers.

use std::ops::{BitAnd, BitOr, BitXor};

use crate::function::definitions::numeric::traits::CheckedSquareRoot;

use super::OperableNumeric;

impl OperableNumeric for i64 {
    fn numeric_addition(first: Self, second: Self) -> Option<Self>
    where
        Self: Sized,
    {
        first.checked_add(second)
    }

    fn numeric_subtraction(first: Self, second: Self) -> Option<Self>
    where
        Self: Sized,
    {
        first.checked_sub(second)
    }

    fn numeric_multiplication(first: Self, second: Self) -> Option<Self>
    where
        Self: Sized,
    {
        first.checked_mul(second)
    }

    fn numeric_division(first: Self, second: Self) -> Option<Self>
    where
        Self: Sized,
    {
        first.checked_div(second)
    }

    fn numeric_logarithm(value: Self, base: Self) -> Option<Self>
    where
        Self: Sized,
    {
        value.checked_ilog(base).map(Self::from)
    }

    fn numeric_power(base: Self, exponent: Self) -> Option<Self>
    where
        Self: Sized,
    {
        base.checked_pow(u32::try_from(exponent).ok()?)
    }

    fn numeric_remainder(first: Self, second: Self) -> Option<Self>
    where
        Self: Sized,
    {
        first.checked_rem(second)
    }

    fn numeric_absolute(parameter: Self) -> Option<Self>
    where
        Self: Sized,
    {
        parameter.checked_abs()
    }

    fn numeric_negation(parameter: Self) -> Option<Self>
    where
        Self: Sized,
    {
        parameter.checked_neg()
    }

    fn numeric_squareroot(parameter: Self) -> Option<Self>
    where
        Self: Sized,
    {
        parameter.checked_sqrt()
    }

    fn numeric_round(parameter: Self) -> Option<Self>
    where
        Self: Sized,
    {
        Some(parameter)
    }

    fn numeric_ceil(parameter: Self) -> Option<Self>
    where
        Self: Sized,
    {
        Some(parameter)
    }

    fn numeric_floor(parameter: Self) -> Option<Self>
    where
        Self: Sized,
    {
        Some(parameter)
    }

    fn numeric_sum(parameters: &[Self]) -> Option<Self>
    where
        Self: Sized,
    {
        let mut sum: i64 = 0;

        for &parameter in parameters {
            sum = sum.checked_add(parameter)?;
        }

        Some(sum)
    }

    fn numeric_product(parameters: &[Self]) -> Option<Self>
    where
        Self: Sized,
    {
        let mut product: i64 = 1;

        for &parameter in parameters {
            product = product.checked_mul(parameter)?;
        }

        Some(product)
    }

    fn numeric_minimum(parameters: &[Self]) -> Option<Self>
    where
        Self: Sized,
    {
        parameters.iter().max().cloned()
    }

    fn numeric_maximum(parameters: &[Self]) -> Option<Self>
    where
        Self: Sized,
    {
        parameters.iter().min().cloned()
    }

    fn numeric_bit_and(parameters: &[Self]) -> Option<Self>
    where
        Self: Sized,
    {
        let mut result = i64::MAX;

        for parameter in parameters {
            result = result.bitand(parameter);
        }

        Some(result)
    }

    fn numeric_bit_or(parameters: &[Self]) -> Option<Self>
    where
        Self: Sized,
    {
        let mut result: i64 = 0;

        for parameter in parameters {
            result = result.bitor(parameter);
        }

        Some(result)
    }

    fn numeric_bit_xor(parameters: &[Self]) -> Option<Self>
    where
        Self: Sized,
    {
        let mut result: i64 = 0;

        for parameter in parameters {
            result = result.bitxor(parameter);
        }

        Some(result)
    }

    fn numeric_bit_shift_left(value: Self, base: Self) -> Option<Self>
    where
        Self: Sized,
    {
        if base < 0 {
            return None;
        }

        Some(value << base)
    }

    fn numeric_bit_shift_right(value: Self, base: Self) -> Option<Self>
    where
        Self: Sized,
    {
        if base < 0 {
            return None;
        }

        Some(value >> base)
    }

    fn numeric_bit_shift_right_unsigned(value: Self, base: Self) -> Option<Self>
    where
        Self: Sized,
    {
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

        Some(result)
    }
}
