use super::FloatIsNaN;
use crate::error::Error;
use std::cmp::Ordering;
use std::convert::TryFrom;
use std::fmt;
use std::ops::{Add, AddAssign};

/// Wrapper for [`f64`] that does not allow [`f64::NAN`] values.
#[derive(Copy, Clone, Debug, PartialEq)]
pub struct Double(f64);

impl Double {
    /// Wraps the given [`f64`]-`value` as a value over [`Double`].
    ///
    /// # Errors
    /// The given `value` is [`f64::NAN`].
    pub fn new(value: f64) -> Result<Double, Error> {
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
        self.0.partial_cmp(&other.0)
    }
}

impl Ord for Double {
    fn cmp(&self, other: &Self) -> Ordering {
        self.partial_cmp(other).unwrap()
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
        (&mut self.0).add_assign(rhs.0)
    }
}

impl fmt::Display for Double {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.0, f)
    }
}

impl TryFrom<f64> for Double {
    type Error = Error;

    fn try_from(value: f64) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl From<Double> for f64 {
    fn from(value: Double) -> Self {
        value.0
    }
}
