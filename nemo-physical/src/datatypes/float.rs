//! This module defines a wrapper type [Float] for [f32] that excludes NaN and infinity.

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

use super::{run_length_encodable::FloatingStep, FloorToUsize, RunLengthEncodable};
use crate::{
    error::{Error, ReadingError},
    function::definitions::numeric::traits::{CheckedPow, CheckedSquareRoot},
};

#[cfg(test)]
use quickcheck::{Arbitrary, Gen};

/// Wrapper for [f32] that excludes [f32::NAN] and infinite values
#[derive(Copy, Clone, Debug, PartialEq, Default)]
pub struct Float(f32);

impl Float {
    /// Wraps the given [f32]-`value` as a value over [Float].
    ///
    /// # Errors
    /// Returns an error if `value` is [f32::NAN] or infinite.
    pub fn new(value: f32) -> Result<Self, ReadingError> {
        if !value.is_finite() {
            return Err(ReadingError::InvalidFloat);
        }

        Ok(Float(value))
    }

    /// Wraps the given [f32]-`value` as a value over [Float].
    ///
    /// # Panics
    /// Panics if `value` is [f32::NAN] or not finite.
    pub fn from_number(value: f32) -> Self {
        if !value.is_finite() {
            panic!("floating point values must be finite")
        }

        Float(value)
    }

    /// Computes the absolute value.
    pub(crate) fn abs(self) -> Self {
        Float::new(self.0.abs()).expect("operation returns valid float")
    }

    /// Returns the logarithm of the number with respect to an arbitrary base.
    pub(crate) fn log(self, base: Self) -> Option<Self> {
        Float::new(self.0.log(base.0)).ok()
    }

    /// Computes the sine of a number (in radians).
    pub(crate) fn sin(self) -> Self {
        Float::new(self.0.sin()).expect("operation returns valid float")
    }

    /// Computes the cosine of a number (in radians).
    pub(crate) fn cos(self) -> Self {
        Float::new(self.0.cos()).expect("operation returns valid float")
    }

    /// Computes the tangent of a number (in radians).
    pub(crate) fn tan(self) -> Self {
        Float::new(self.0.tan()).expect("operation returns valid float")
    }

    /// Returns the nearest integer to `self`.
    /// If a value is half-way between two integers, round away from 0.0.
    pub(crate) fn round(self) -> Self {
        Float::new(self.0.round()).expect("operation returns valid float")
    }

    /// Returns the smallest integer greater than or equal to `self`.
    pub(crate) fn ceil(self) -> Self {
        Float::new(self.0.ceil()).expect("operation returns valid float")
    }

    /// Returns the largest integer less than or equal to `self`.
    pub(crate) fn floor(self) -> Self {
        Float::new(self.0.floor()).expect("operation returns valid float")
    }
}

impl Eq for Float {}

impl PartialOrd for Float {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Float {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0
            .partial_cmp(&other.0)
            .expect("comparison can only fail on NaN values which have been forbidden in this type")
    }
}

impl Add for Float {
    type Output = Float;

    fn add(self, rhs: Self) -> Self::Output {
        Float(self.0.add(rhs.0))
    }
}

impl AddAssign for Float {
    fn add_assign(&mut self, rhs: Self) {
        self.0.add_assign(rhs.0)
    }
}

impl Sub for Float {
    type Output = Float;

    fn sub(self, rhs: Self) -> Self::Output {
        Float(self.0.sub(rhs.0))
    }
}

impl SubAssign for Float {
    fn sub_assign(&mut self, rhs: Self) {
        self.0.sub_assign(rhs.0)
    }
}

impl Mul for Float {
    type Output = Float;

    fn mul(self, rhs: Self) -> Self::Output {
        Float(self.0.mul(rhs.0))
    }
}

impl MulAssign for Float {
    fn mul_assign(&mut self, rhs: Self) {
        self.0.mul_assign(rhs.0)
    }
}

impl Div for Float {
    type Output = Float;

    fn div(self, rhs: Self) -> Self::Output {
        Float(self.0.div(rhs.0))
    }
}

impl Rem for Float {
    type Output = Float;

    fn rem(self, rhs: Self) -> Self::Output {
        Float(self.0.rem(rhs.0))
    }
}

impl DivAssign for Float {
    fn div_assign(&mut self, rhs: Self) {
        self.0.div_assign(rhs.0)
    }
}

impl fmt::Display for Float {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl TryFrom<f32> for Float {
    type Error = ReadingError;

    fn try_from(value: f32) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl From<Float> for f32 {
    fn from(value: Float) -> Self {
        value.0
    }
}

impl TryFrom<usize> for Float {
    type Error = Error;

    fn try_from(value: usize) -> Result<Self, Self::Error> {
        let res = f32::from_usize(value).ok_or(Error::UsizeToFloatingPointValue(value))?;

        Ok(Float::new(res)?)
    }
}

impl Zero for Float {
    fn zero() -> Self {
        Float::from_number(f32::zero())
    }

    fn is_zero(&self) -> bool {
        self.0.is_zero()
    }
}

impl One for Float {
    fn one() -> Self {
        Float::from_number(f32::one())
    }

    fn is_one(&self) -> bool {
        self.0.is_one()
    }
}

impl Sum for Float {
    fn sum<I>(iter: I) -> Self
    where
        I: Iterator<Item = Self>,
    {
        Float::from_number(iter.map(|f| f.0).sum())
    }
}

impl Product for Float {
    fn product<I>(iter: I) -> Self
    where
        I: Iterator<Item = Self>,
    {
        Float::from_number(iter.map(|f| f.0).product())
    }
}

impl CheckedAdd for Float {
    fn checked_add(&self, v: &Self) -> Option<Self> {
        Float::new(self.0 + v.0).ok()
    }
}

impl CheckedSub for Float {
    fn checked_sub(&self, v: &Self) -> Option<Self> {
        Float::new(self.0 - v.0).ok()
    }
}

impl CheckedDiv for Float {
    fn checked_div(&self, v: &Self) -> Option<Self> {
        Float::new(self.0 / v.0).ok()
    }
}

impl CheckedMul for Float {
    fn checked_mul(&self, v: &Self) -> Option<Self> {
        Float::new(self.0 * v.0).ok()
    }
}

impl CheckedNeg for Float {
    fn checked_neg(&self) -> Option<Self> {
        Float::new(-1.0 * self.0).ok()
    }
}

impl CheckedSquareRoot for Float {
    fn checked_sqrt(self) -> Option<Self> {
        Float::new(self.0.checked_sqrt()?).ok()
    }
}

impl CheckedPow for Float {
    fn checked_pow(self, exponent: Self) -> Option<Self> {
        Float::new(self.0.checked_pow(exponent.0)?).ok()
    }
}

impl FloorToUsize for Float {
    fn floor_to_usize(self) -> Option<usize> {
        usize::from_f32(self.0.floor())
    }
}

impl Bounded for Float {
    fn min_value() -> Self {
        Self(f32::MIN)
    }

    fn max_value() -> Self {
        Self(f32::MAX)
    }
}

#[cfg(test)]
impl Arbitrary for Float {
    fn arbitrary(g: &mut Gen) -> Self {
        let mut value = f32::arbitrary(g);
        while !value.is_finite() {
            value = f32::arbitrary(g);
        }

        Self::from_number(value)
    }
}

impl RunLengthEncodable for Float {
    type Step = FloatingStep;

    fn diff_step(a: Self, b: Self) -> Option<Self::Step> {
        if a == b {
            Some(FloatingStep {})
        } else {
            None
        }
    }

    fn get_step_increment(_: Self::Step) -> Option<Self> {
        Some(Self::zero())
    }

    fn offset(self, _: Self::Step, _: usize) -> Self {
        self
    }
}
