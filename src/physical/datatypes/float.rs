use super::float_is_nan::FloatIsNaN;
use std::cmp::Ordering;
use std::convert::TryFrom;
use std::fmt;
use std::ops::{Add, AddAssign};

/// Wrapper for [`f32`] that does not allow [`f32::NAN`] values.
#[derive(Copy, Clone, Debug, PartialEq)]
pub struct Float(f32);

impl Float {
    /// Wraps the given [`f32`]-`value` as a value over [`Float`].
    ///
    /// # Errors
    /// The given `value` is [`f32::NAN`].
    pub fn new(value: f32) -> Result<Float, FloatIsNaN> {
        if value.is_nan() {
            return Err(FloatIsNaN);
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

impl fmt::Display for Float {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.0, f)
    }
}

impl TryFrom<f32> for Float {
    type Error = FloatIsNaN;

    fn try_from(value: f32) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl From<Float> for f32 {
    fn from(value: Float) -> Self {
        value.0
    }
}
