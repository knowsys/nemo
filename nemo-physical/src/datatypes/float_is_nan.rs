use std::fmt;
use std::fmt::Formatter;

/// Error for cases when [f32::NAN`] or [`f64::NAN], values that are not allowed,
/// are being cast to [super::Float`] or [`super::Double], respectively.
#[derive(Clone, Copy, Debug, Ord, PartialOrd, Eq, PartialEq)]
pub struct FloatIsNaN;

impl fmt::Display for FloatIsNaN {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "The floating point types used in this library do not support NaN!"
        )
    }
}

impl std::error::Error for FloatIsNaN {}
