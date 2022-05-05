use super::{FloatIsNaN, FloorToUsize};
use crate::error::Error;
use num::FromPrimitive;
use num::Zero;
use std::cmp::Ordering;
use std::convert::TryFrom;
use std::fmt;
use std::iter::Sum;
use std::ops::{Add, AddAssign, Div, DivAssign, Mul, MulAssign, Sub, SubAssign};

#[cfg(test)]
use quickcheck::{Arbitrary, Gen};

/// Wrapper for [`f32`] that does not allow [`f32::NAN`] values.
#[derive(Copy, Clone, Debug, PartialEq, Default)]
pub struct Float(f32);

impl Float {
    /// Wraps the given [`f32`]-`value` as a value over [`Float`].
    ///
    /// # Errors
    /// The given `value` is [`f32::NAN`].
    pub fn new(value: f32) -> Result<Float, Error> {
        if value.is_nan() {
            return Err(FloatIsNaN.into());
        }

        Ok(Float(value))
    }

    /// Wraps the given [`f32`]-`value`, that is a number, as a value over [`Float`].
    ///
    /// # Panics
    /// The given `value` is [`f32::NAN`].
    pub fn from_number(value: f32) -> Float {
        if value.is_nan() {
            panic!("The provided value is not a number (NaN)!")
        }

        Float(value)
    }
}

impl Eq for Float {}

impl PartialOrd for Float {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.0.partial_cmp(&other.0)
    }
}

impl Ord for Float {
    fn cmp(&self, other: &Self) -> Ordering {
        self.partial_cmp(other).unwrap()
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
        (&mut self.0).add_assign(rhs.0)
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
        (&mut self.0).sub_assign(rhs.0)
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
        (&mut self.0).mul_assign(rhs.0)
    }
}

impl Div for Float {
    type Output = Float;

    fn div(self, rhs: Self) -> Self::Output {
        Float(self.0.div(rhs.0))
    }
}

impl DivAssign for Float {
    fn div_assign(&mut self, rhs: Self) {
        (&mut self.0).div_assign(rhs.0)
    }
}

impl fmt::Display for Float {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.0, f)
    }
}

impl TryFrom<f32> for Float {
    type Error = Error;

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
        f32::from_usize(value)
            .ok_or(Error::UsizeToFloatingPointValue(value))
            .and_then(Float::new)
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

impl Sum for Float {
    fn sum<I>(iter: I) -> Self
    where
        I: Iterator<Item = Self>,
    {
        Float::from_number(iter.map(|f| f.0).sum())
    }
}

impl FloorToUsize for Float {
    fn floor_to_usize(self) -> Option<usize> {
        usize::from_f32(self.0.floor())
    }
}

#[cfg(test)]
impl Arbitrary for Float {
    fn arbitrary(g: &mut Gen) -> Self {
        let mut value = f32::arbitrary(g);
        while value.is_nan() {
            value = f32::arbitrary(g);
        }

        Self::from_number(value)
    }
}
