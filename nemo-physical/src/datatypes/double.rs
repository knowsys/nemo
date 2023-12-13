use super::run_length_encodable::FloatingStep;
use super::{Float, FloatIsNaN, FloorToUsize, RunLengthEncodable};
use crate::arithmetic::traits::{CheckedPow, CheckedSquareRoot};
use crate::error::{Error, ReadingError};
use num::traits::CheckedNeg;
use num::{Bounded, CheckedAdd, CheckedDiv, CheckedMul, CheckedSub, FromPrimitive, One, Zero};
use std::cmp::Ordering;
use std::convert::TryFrom;
use std::fmt;
use std::iter::{Product, Sum};
use std::ops::{Add, AddAssign, Div, DivAssign, Mul, MulAssign, Sub, SubAssign};

#[cfg(test)]
use quickcheck::{Arbitrary, Gen};

/// Wrapper for [`f64`] that does not allow [`f64::NAN`] values.
#[derive(Copy, Clone, Debug, PartialEq, Default)]
pub struct Double(f64);

impl Double {
    /// Wraps the given [`f64`]-`value` as a value over [`Double`].
    ///
    /// # Errors
    /// The given `value` is [`f64::NAN`].
    pub fn new(value: f64) -> Result<Self, ReadingError> {
        if value.is_nan() {
            return Err(FloatIsNaN.into());
        }

        Ok(Self(value))
    }

    /// Wraps the given [`f64`]-`value`, that is a number, as a value over [`Double`].
    ///
    /// # Panics
    /// The given `value` is [`f64::NAN`].
    pub fn from_number(value: f64) -> Self {
        if value.is_nan() {
            panic!("The provided value is not a number (NaN)!")
        }

        Self(value)
    }

    /// Return this value as an [i64], provided that
    /// this is a finite number without any fractional part
    /// that can fit into an [i64].
    ///
    /// Returns `None` otherwise.
    pub fn as_i64(&self) -> Option<i64> {
        if self.0.round() == self.0 {
            Some(self.0 as i64)
        } else {
            None
        }
    }

    /// Create a [Double] from a [i64].
    pub fn from_i64(value: i64) -> Option<Double> {
        Double::new(value as f64).ok()
    }

    /// Create a [Double] from a [u64].
    pub fn from_u64(value: u64) -> Option<Double> {
        Double::new(value as f64).ok()
    }

    /// Converts this value into [Float].
    pub fn as_float(&self) -> Option<Float> {
        Float::new(self.0 as f32).ok()
    }

    /// Computes the absolute value.
    pub fn abs(self) -> Self {
        Double::new(self.0.abs()).expect("Taking the absolute value cannot result in NaN")
    }

    /// Returns the logarithm of the number with respect to an arbitrary base.
    pub fn log(self, base: Self) -> Option<Self> {
        Double::new(self.0.log(base.0)).ok()
    }

    /// Computes the sine of a number (in radians).
    pub fn sin(self) -> Self {
        Double::new(self.0.sin()).expect("Operation does not result in NaN")
    }

    /// Computes the cosine of a number (in radians).
    pub fn cos(self) -> Self {
        Double::new(self.0.cos()).expect("Operation does not result in NaN")
    }

    /// Computes the tangent of a number (in radians).
    pub fn tan(self) -> Self {
        Double::new(self.0.tan()).expect("Operation does not result in NaN")
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
        Double::from_number(f64::zero())
    }

    fn is_zero(&self) -> bool {
        self.0.is_zero()
    }
}

impl One for Double {
    fn one() -> Self {
        Double::from_number(f64::one())
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
        Double::from_number(iter.map(|f| f.0).sum())
    }
}

impl Product for Double {
    fn product<I>(iter: I) -> Self
    where
        I: Iterator<Item = Self>,
    {
        Double::from_number(iter.map(|f| f.0).product())
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

impl FloorToUsize for Double {
    fn floor_to_usize(self) -> Option<usize> {
        usize::from_f64(self.0.floor())
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
        while value.is_nan() {
            value = f64::arbitrary(g);
        }

        Self::from_number(value)
    }
}

impl RunLengthEncodable for Double {
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
