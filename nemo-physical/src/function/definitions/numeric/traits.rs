//! This module defines operations that need to be implemented on number types
//! in order for them to be used in all the supported arithmetic expressions

use num::{
    integer::Roots, traits::CheckedNeg, CheckedAdd, CheckedDiv, CheckedMul, CheckedSub, One, Zero,
};

/// Trait for a type that allows a square root operation
pub trait CheckedSquareRoot {
    /// Compute the square root of the input.
    /// Return `None` if it is not defined.
    fn checked_sqrt(self) -> Option<Self>
    where
        Self: Sized;
}

impl CheckedSquareRoot for usize {
    fn checked_sqrt(self) -> Option<Self> {
        Some(Roots::sqrt(&self))
    }
}
impl CheckedSquareRoot for u8 {
    fn checked_sqrt(self) -> Option<Self> {
        Some(Roots::sqrt(&self))
    }
}

impl CheckedSquareRoot for u32 {
    fn checked_sqrt(self) -> Option<Self> {
        Some(Roots::sqrt(&self))
    }
}

impl CheckedSquareRoot for u64 {
    fn checked_sqrt(self) -> Option<Self> {
        Some(Roots::sqrt(&self))
    }
}

impl CheckedSquareRoot for i8 {
    fn checked_sqrt(self) -> Option<Self>
    where
        Self: Sized,
    {
        if self < 0 {
            return None;
        }

        Some(Roots::sqrt(&self))
    }
}

impl CheckedSquareRoot for i32 {
    fn checked_sqrt(self) -> Option<Self> {
        if self < 0 {
            return None;
        }

        Some(Roots::sqrt(&self))
    }
}

impl CheckedSquareRoot for i64 {
    fn checked_sqrt(self) -> Option<Self>
    where
        Self: Sized,
    {
        if self < 0 {
            return None;
        }

        Some(Roots::sqrt(&self))
    }
}

impl CheckedSquareRoot for f32 {
    fn checked_sqrt(self) -> Option<Self> {
        if self < 0.0 {
            return None;
        }

        Some(num::Float::sqrt(self))
    }
}

impl CheckedSquareRoot for f64 {
    fn checked_sqrt(self) -> Option<Self>
    where
        Self: Sized,
    {
        if self < 0.0 {
            return None;
        }

        Some(num::Float::sqrt(self))
    }
}

/// Trait for a type that allows exponentiation
pub trait CheckedPow {
    /// Raise `self` to the power of some value of the same type.
    /// Returns `None` if the operation is undefined
    /// or if the resulting value is not numeric.
    fn checked_pow(self, exponent: Self) -> Option<Self>
    where
        Self: Sized;
}

impl CheckedPow for usize {
    fn checked_pow(self, exponent: Self) -> Option<Self> {
        num::checked_pow(self, exponent)
    }
}

impl CheckedPow for u8 {
    fn checked_pow(self, exponent: Self) -> Option<Self> {
        num::checked_pow(self, exponent.into())
    }
}

impl CheckedPow for u32 {
    fn checked_pow(self, exponent: Self) -> Option<Self> {
        num::checked_pow(self, exponent.try_into().ok()?)
    }
}

impl CheckedPow for u64 {
    fn checked_pow(self, exponent: Self) -> Option<Self> {
        num::checked_pow(self, exponent.try_into().ok()?)
    }
}

impl CheckedPow for i8 {
    fn checked_pow(self, exponent: Self) -> Option<Self> {
        if exponent < 0 {
            return None;
        }

        num::checked_pow(self, exponent.try_into().ok()?)
    }
}

impl CheckedPow for i32 {
    fn checked_pow(self, exponent: Self) -> Option<Self> {
        if exponent < 0 {
            return None;
        }

        num::checked_pow(self, exponent.try_into().ok()?)
    }
}

impl CheckedPow for i64 {
    fn checked_pow(self, exponent: Self) -> Option<Self> {
        if exponent < 0 {
            return None;
        }

        num::checked_pow(self, exponent.try_into().ok()?)
    }
}

impl CheckedPow for f32 {
    fn checked_pow(self, exponent: Self) -> Option<Self> {
        let result = self.powf(exponent);

        if !result.is_finite() {
            return None;
        }

        Some(result)
    }
}

impl CheckedPow for f64 {
    fn checked_pow(self, exponent: Self) -> Option<Self> {
        let result = self.powf(exponent);

        if !result.is_finite() {
            return None;
        }

        Some(result)
    }
}

/// Specifies the traits that need to be implemented by a type
/// so that can be used to evaluate all the supported arithmetic expressions
pub trait ArithmeticOperations:
    Copy
    + PartialEq
    + PartialOrd
    + Zero
    + One
    + CheckedSub
    + CheckedAdd
    + CheckedMul
    + CheckedDiv
    + CheckedPow
    + CheckedSquareRoot
    + CheckedNeg
{
}

impl<T> ArithmeticOperations for T where
    T: Copy
        + PartialEq
        + PartialOrd
        + Zero
        + One
        + CheckedSub
        + CheckedAdd
        + CheckedMul
        + CheckedDiv
        + CheckedSquareRoot
        + CheckedPow
        + CheckedNeg
{
}
