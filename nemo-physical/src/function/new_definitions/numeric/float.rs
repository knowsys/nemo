//! This module implements the numeric functions for [Float]s.

use num::{traits::CheckedNeg, CheckedAdd, CheckedDiv, CheckedMul, CheckedSub};

use crate::{
    function::definitions::numeric::traits::{CheckedPow, CheckedSquareRoot},
    storagevalues::float::Float,
};

use super::OperableNumeric;

impl OperableNumeric for Float {
    fn numeric_addition(first: Self, second: Self) -> Option<Self>
    where
        Self: Sized,
    {
        first.checked_add(&second)
    }

    fn numeric_subtraction(first: Self, second: Self) -> Option<Self>
    where
        Self: Sized,
    {
        first.checked_sub(&second)
    }

    fn numeric_multiplication(first: Self, second: Self) -> Option<Self>
    where
        Self: Sized,
    {
        first.checked_mul(&second)
    }

    fn numeric_division(first: Self, second: Self) -> Option<Self>
    where
        Self: Sized,
    {
        first.checked_div(&second)
    }

    fn numeric_logarithm(value: Self, base: Self) -> Option<Self>
    where
        Self: Sized,
    {
        value.checked_log(base)
    }

    fn numeric_power(base: Self, exponent: Self) -> Option<Self>
    where
        Self: Sized,
    {
        base.checked_pow(exponent)
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
        Some(parameter.abs())
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

    fn numeric_sine(parameter: Self) -> Option<Self>
    where
        Self: Sized,
    {
        Some(parameter.sin())
    }

    fn numeric_cosine(parameter: Self) -> Option<Self>
    where
        Self: Sized,
    {
        Some(parameter.cos())
    }

    fn numeric_tangent(parameter: Self) -> Option<Self>
    where
        Self: Sized,
    {
        Some(parameter.tan())
    }

    fn numeric_round(parameter: Self) -> Option<Self>
    where
        Self: Sized,
    {
        Some(parameter.round())
    }

    fn numeric_ceil(parameter: Self) -> Option<Self>
    where
        Self: Sized,
    {
        Some(parameter.ceil())
    }

    fn numeric_floor(parameter: Self) -> Option<Self>
    where
        Self: Sized,
    {
        Some(parameter.floor())
    }

    fn numeric_sum(parameters: &[Self]) -> Option<Self>
    where
        Self: Sized,
    {
        let mut sum = Float::new_unchecked(0.0);

        for parameter in parameters {
            sum = sum.checked_add(parameter)?;
        }

        Some(sum)
    }

    fn numeric_product(parameters: &[Self]) -> Option<Self>
    where
        Self: Sized,
    {
        let mut product = Float::new_unchecked(1.0);

        for parameter in parameters {
            product = product.checked_mul(parameter)?;
        }

        Some(product)
    }

    fn numeric_minimum(parameters: &[Self]) -> Option<Self>
    where
        Self: Sized,
    {
        parameters.iter().min().cloned()
    }

    fn numeric_maximum(parameters: &[Self]) -> Option<Self>
    where
        Self: Sized,
    {
        parameters.iter().max().cloned()
    }

    fn numeric_lukasiewicz(parameters: &[Self]) -> Option<Self>
    where
        Self: Sized,
    {
        if parameters.is_empty() {
            return None;
        }

        let mut sum = Float::new_unchecked(0.0);

        for parameter in parameters {
            sum = sum.checked_add(parameter)?;
        }

        let result = Float::new_unchecked(0.0)
            .max(sum.checked_sub(&Float::try_from(parameters.len() - 1).ok()?)?);

        Some(result)
    }
}
