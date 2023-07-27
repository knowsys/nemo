use super::run_length_encodable::FloatingStep;
use super::{FloatIsNaN, FloorToUsize, RunLengthEncodable};
use crate::error::{Error, ReadingError};
use num::{Bounded, CheckedMul, FromPrimitive, One, Zero};
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
    pub fn new(value: f64) -> Result<Double, ReadingError> {
        if value.is_nan() {
            return Err(FloatIsNaN.into());
        }

        Ok(Double(value))
    }

    /// Wraps the given [`f64`]-`value`, that is a number, as a value over [`Double`].
    ///
    /// # Panics
    /// The given `value` is [`f64::NAN`].
    pub fn from_number(value: f64) -> Double {
        if value.is_nan() {
            panic!("The provided value is not a number (NaN)!")
        }

        Double(value)
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

impl CheckedMul for Double {
    fn checked_mul(&self, rhs: &Self) -> Option<Self> {
        let prod = self.0 * rhs.0;
        if prod.is_finite() {
            Self::new(prod).ok()
        } else {
            None
        }
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
