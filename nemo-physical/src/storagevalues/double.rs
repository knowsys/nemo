//! This module defines a wrapper type [Double] for [f64] that excludes NaN and infinity.

use std::{
    cmp::Ordering,
    fmt,
    iter::{Product, Sum},
    ops::{Add, AddAssign, Div, DivAssign, Mul, MulAssign, Rem, Sub, SubAssign},
};

use num::{
    traits::CheckedNeg, Bounded, CheckedAdd, CheckedDiv, CheckedMul, CheckedSub, FromPrimitive,
    One, Zero,
};

use crate::{
    error::{Error, ReadingError, ReadingErrorKind},
    function::definitions::numeric::traits::{CheckedPow, CheckedSquareRoot},
};

#[cfg(test)]
use quickcheck::{Arbitrary, Gen};

/// Wrapper for [f64] that excludes [f64::NAN] and infinite values
#[derive(Copy, Clone, Debug, PartialEq, Default)]
pub struct Double(f64);

impl Double {
    /// Wraps the given [f64]-`value` as a value over [Double].
    ///
    /// # Errors
    /// Returns an error if `value` is [f32::NAN] or infinite.
    pub fn new(value: f64) -> Result<Self, ReadingError> {
        if !value.is_finite() {
            return Err(ReadingError::new(ReadingErrorKind::InvalidFloat));
        }

        Ok(Self(value))
    }

    /// Wraps the given [f64]-`value` as a value over [Double].
    ///
    /// # Panics
    /// Panics if `value` is [f64::NAN] or not finite.
    pub fn new_unchecked(value: f64) -> Self {
        debug_assert!(value.is_finite());

        Double(value)
    }

    /// Computes the absolute value.
    pub(crate) fn abs(self) -> Self {
        Double::new(self.0.abs()).expect("operation returns valid float")
    }

    /// Returns the logarithm of the number with respect to an arbitrary base.
    pub(crate) fn checked_log(self, base: Self) -> Option<Self> {
        Double::new(self.0.log(base.0)).ok()
    }

    /// Computes the sine of a number (in radians).
    pub(crate) fn sin(self) -> Self {
        Double::new(self.0.sin()).expect("operation returns valid float")
    }

    /// Computes the cosine of a number (in radians).
    pub(crate) fn cos(self) -> Self {
        Double::new(self.0.cos()).expect("operation returns valid float")
    }

    /// Computes the tangent of a number (in radians).
    pub(crate) fn tan(self) -> Self {
        Double::new(self.0.tan()).expect("operation returns valid float")
    }

    /// Returns the nearest integer to `self`.
    /// If a value is half-way between two integers, round away from 0.0.
    pub(crate) fn round(self) -> Self {
        Double::new(self.0.round()).expect("operation returns valid float")
    }

    /// Returns the nearest integer to `self`.
    /// If a value is half-way between two integers, round away from 0.0.
    pub(crate) fn ceil(self) -> Self {
        Double::new(self.0.ceil()).expect("operation returns valid float")
    }

    /// Returns the largest integer less than or equal to `self`.
    pub(crate) fn floor(self) -> Self {
        Double::new(self.0.floor()).expect("operation returns valid float")
    }

    /// Return the remainder when dividing one [Double] with another
    pub(crate) fn checked_rem(self, other: Self) -> Option<Self> {
        Self::new(self.0.rem(other.0)).ok()
    }
}

impl Eq for Double {}

impl PartialOrd for Double {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Double {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0
            .partial_cmp(&other.0)
            .expect("Comparison can only fail on NaN values which have been forbidden in this type")
    }
}

impl Add for Double {
    type Output = Double;

    fn add(self, rhs: Self) -> Self::Output {
        Double(self.0.add(rhs.0))
    }
}

impl AddAssign for Double {
    fn add_assign(&mut self, rhs: Self) {
        self.0.add_assign(rhs.0)
    }
}

impl Sub for Double {
    type Output = Double;

    fn sub(self, rhs: Self) -> Self::Output {
        Double(self.0.sub(rhs.0))
    }
}

impl SubAssign for Double {
    fn sub_assign(&mut self, rhs: Self) {
        self.0.sub_assign(rhs.0)
    }
}

impl Mul for Double {
    type Output = Double;

    fn mul(self, rhs: Self) -> Self::Output {
        Double(self.0.mul(rhs.0))
    }
}

impl MulAssign for Double {
    fn mul_assign(&mut self, rhs: Self) {
        self.0.mul_assign(rhs.0)
    }
}

impl Div for Double {
    type Output = Double;

    fn div(self, rhs: Self) -> Self::Output {
        Double(self.0.div(rhs.0))
    }
}

impl DivAssign for Double {
    fn div_assign(&mut self, rhs: Self) {
        self.0.div_assign(rhs.0)
    }
}

impl fmt::Display for Double {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl TryFrom<f64> for Double {
    type Error = ReadingError;

    fn try_from(value: f64) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl From<Double> for f64 {
    fn from(value: Double) -> Self {
        value.0
    }
}

impl TryFrom<usize> for Double {
    type Error = Error;

    fn try_from(value: usize) -> Result<Self, Self::Error> {
        let res = f64::from_usize(value).ok_or(Error::UsizeToFloatingPointValue(value))?;

        Ok(Double::new(res)?)
    }
}

impl Zero for Double {
    fn zero() -> Self {
        Double::new_unchecked(f64::zero())
    }

    fn is_zero(&self) -> bool {
        self.0.is_zero()
    }
}

impl One for Double {
    fn one() -> Self {
        Double::new_unchecked(f64::one())
    }

    fn is_one(&self) -> bool {
        self.0.is_one()
    }
}

impl Sum for Double {
    fn sum<I>(iter: I) -> Self
    where
        I: Iterator<Item = Self>,
    {
        Double::new_unchecked(iter.map(|f| f.0).sum())
    }
}

impl Product for Double {
    fn product<I>(iter: I) -> Self
    where
        I: Iterator<Item = Self>,
    {
        Double::new_unchecked(iter.map(|f| f.0).product())
    }
}

impl CheckedAdd for Double {
    fn checked_add(&self, v: &Self) -> Option<Self> {
        Double::new(self.0 + v.0).ok()
    }
}

impl CheckedSub for Double {
    fn checked_sub(&self, v: &Self) -> Option<Self> {
        Double::new(self.0 - v.0).ok()
    }
}

impl CheckedDiv for Double {
    fn checked_div(&self, v: &Self) -> Option<Self> {
        Double::new(self.0 / v.0).ok()
    }
}

impl CheckedMul for Double {
    fn checked_mul(&self, v: &Self) -> Option<Self> {
        Double::new(self.0 * v.0).ok()
    }
}

impl CheckedSquareRoot for Double {
    fn checked_sqrt(self) -> Option<Self> {
        Double::new(self.0.checked_sqrt()?).ok()
    }
}

impl CheckedPow for Double {
    fn checked_pow(self, exponent: Self) -> Option<Self> {
        Double::new(self.0.checked_pow(exponent.0)?).ok()
    }
}

impl CheckedNeg for Double {
    fn checked_neg(&self) -> Option<Self> {
        Double::new(-1.0 * self.0).ok()
    }
}

impl Bounded for Double {
    fn min_value() -> Self {
        Self(f64::MIN)
    }

    fn max_value() -> Self {
        Self(f64::MAX)
    }
}

#[cfg(test)]
impl Arbitrary for Double {
    fn arbitrary(g: &mut Gen) -> Self {
        let mut value = f64::arbitrary(g);
        while !value.is_finite() {
            value = f64::arbitrary(g);
        }

        Self::new_unchecked(value)
    }
}
