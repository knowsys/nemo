use std::cmp::Ordering;

use super::{DataTypeName, Double, Float};

/// Implicitlly castable values are those which essentially use the same number representation format
/// but where the range of representable values of one is a subset of the range of values of the other.
/// In this case only overflow or underflow errors are possible.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum ImplicitCastError {
    /// Value is larger than the largest possible value in the target type.
    Overflow,
    /// Value is smaller than the smallest possible value in the target type.
    Underflow,
    /// Values are incompatible with each other.
    NonCastable,
}

/// Types implementing this trait can potentially be obtained by casting the templated type
pub trait ImplicitCastFrom<T>: Sized {
    /// Try to cast the given value into the type of the object implementing this trait.
    fn cast_from(value: T) -> Result<Self, ImplicitCastError>;
}

/// Types implementing this trait can potentially be casted into the templated type.
pub trait ImplicitCastInto<T>: Sized {
    /// Try to cast the object implementing this trait into the templated type.
    fn cast_into(self) -> Result<T, ImplicitCastError>;
}

// Casting from one type to the same
// This should never produce an error
macro_rules! implicit_cast_id {
    ($type:ty) => {
        impl ImplicitCastFrom<$type> for $type {
            fn cast_from(value: $type) -> Result<$type, ImplicitCastError> {
                Ok(value)
            }
        }

        impl ImplicitCastInto<$type> for $type {
            fn cast_into(self) -> Result<$type, ImplicitCastError> {
                Ok(self)
            }
        }
    };
}

implicit_cast_id!(u8);
implicit_cast_id!(u16);
implicit_cast_id!(u32);
implicit_cast_id!(u64);
implicit_cast_id!(usize);
implicit_cast_id!(i8);
implicit_cast_id!(i16);
implicit_cast_id!(i32);
implicit_cast_id!(i64);
implicit_cast_id!(isize);
implicit_cast_id!(Float);
implicit_cast_id!(Double);

// Casting smaller type into larger type
// This should never produce an error

macro_rules! implicit_cast_small_to_large {
    ($type_small:ty, $type_large:ty) => {
        impl ImplicitCastFrom<$type_small> for $type_large {
            fn cast_from(value: $type_small) -> Result<$type_large, ImplicitCastError> {
                Ok(<$type_large>::from(value))
            }
        }

        impl ImplicitCastInto<$type_large> for $type_small {
            fn cast_into(self) -> Result<$type_large, ImplicitCastError> {
                Ok(self.into())
            }
        }
    };
}

implicit_cast_small_to_large!(u8, u16);
implicit_cast_small_to_large!(u8, u32);
implicit_cast_small_to_large!(u8, u64);
implicit_cast_small_to_large!(u8, usize);

implicit_cast_small_to_large!(u16, u32);
implicit_cast_small_to_large!(u16, u64);
implicit_cast_small_to_large!(u16, usize);

implicit_cast_small_to_large!(u32, u64);

implicit_cast_small_to_large!(i8, i16);
implicit_cast_small_to_large!(i8, i32);
implicit_cast_small_to_large!(i8, i64);

implicit_cast_small_to_large!(i16, i32);
implicit_cast_small_to_large!(i16, i64);

implicit_cast_small_to_large!(i32, i64);

// Casting larger unsigned type into smaller signed type
// This either succeeds or there is an overflow error
macro_rules! implicit_cast_unsigned_large_to_small {
    ($type_large:ty, $type_small:ty) => {
        impl ImplicitCastFrom<$type_large> for $type_small {
            fn cast_from(value: $type_large) -> Result<$type_small, ImplicitCastError> {
                match <$type_small>::try_from(value) {
                    Ok(v) => Ok(v),
                    Err(_) => Err(ImplicitCastError::Overflow),
                }
            }
        }

        impl ImplicitCastInto<$type_small> for $type_large {
            fn cast_into(self) -> Result<$type_small, ImplicitCastError> {
                match self.try_into() {
                    Ok(v) => Ok(v),
                    Err(_) => Err(ImplicitCastError::Overflow),
                }
            }
        }
    };
}

implicit_cast_unsigned_large_to_small!(u64, u32);
implicit_cast_unsigned_large_to_small!(u64, u16);
implicit_cast_unsigned_large_to_small!(u64, u8);

implicit_cast_unsigned_large_to_small!(u32, u16);
implicit_cast_unsigned_large_to_small!(u32, u8);

implicit_cast_unsigned_large_to_small!(u16, u8);

// The assumption here is that usize is at least as big as u32
// Since we do not know it size a priori we cannot use implicit_cast_small_to_large! here
implicit_cast_unsigned_large_to_small!(usize, u32);
implicit_cast_unsigned_large_to_small!(usize, u64);
implicit_cast_unsigned_large_to_small!(u32, usize);
implicit_cast_unsigned_large_to_small!(u64, usize);

// Casting larger signed type into smaller signed type
// Here values can be bigger or smaller than possibly representable by the target type
macro_rules! implicit_cast_signed_large_to_small {
    ($type_large:ty, $type_small:ty) => {
        impl ImplicitCastFrom<$type_large> for $type_small {
            fn cast_from(value: $type_large) -> Result<$type_small, ImplicitCastError> {
                match <$type_small>::try_from(value) {
                    Ok(v) => Ok(v),
                    Err(_) => {
                        if value > 0 {
                            Err(ImplicitCastError::Overflow)
                        } else {
                            Err(ImplicitCastError::Underflow)
                        }
                    }
                }
            }
        }

        impl ImplicitCastInto<$type_small> for $type_large {
            fn cast_into(self) -> Result<$type_small, ImplicitCastError> {
                match self.try_into() {
                    Ok(v) => Ok(v),
                    Err(_) => {
                        if self > 0 {
                            Err(ImplicitCastError::Overflow)
                        } else {
                            Err(ImplicitCastError::Underflow)
                        }
                    }
                }
            }
        }
    };
}

implicit_cast_signed_large_to_small!(i64, i32);
implicit_cast_signed_large_to_small!(i64, i16);
implicit_cast_signed_large_to_small!(i64, i8);

implicit_cast_signed_large_to_small!(i32, i16);
implicit_cast_signed_large_to_small!(i32, i8);

implicit_cast_signed_large_to_small!(i16, i8);

// The assumption here is that usize is at least as big as u32
// Since we do not know it size a priori we cannot use implicit_cast_small_to_large! here
implicit_cast_signed_large_to_small!(isize, i32);
implicit_cast_signed_large_to_small!(isize, i64);
implicit_cast_signed_large_to_small!(i32, isize);
implicit_cast_signed_large_to_small!(i64, isize);

// Casting incompatible types
macro_rules! implicit_cast_incompatible {
    ($type_a:ty, $type_b:ty) => {
        impl ImplicitCastFrom<$type_a> for $type_b {
            fn cast_from(_value: $type_a) -> Result<$type_b, ImplicitCastError> {
                Err(ImplicitCastError::NonCastable)
            }
        }

        impl ImplicitCastFrom<$type_b> for $type_a {
            fn cast_from(_value: $type_b) -> Result<$type_a, ImplicitCastError> {
                Err(ImplicitCastError::NonCastable)
            }
        }

        impl ImplicitCastInto<$type_a> for $type_b {
            fn cast_into(self) -> Result<$type_a, ImplicitCastError> {
                Err(ImplicitCastError::NonCastable)
            }
        }

        impl ImplicitCastInto<$type_b> for $type_a {
            fn cast_into(self) -> Result<$type_b, ImplicitCastError> {
                Err(ImplicitCastError::NonCastable)
            }
        }
    };
}

implicit_cast_incompatible!(u8, i8);
implicit_cast_incompatible!(u8, i16);
implicit_cast_incompatible!(u8, i32);
implicit_cast_incompatible!(u8, i64);
implicit_cast_incompatible!(u8, isize);
implicit_cast_incompatible!(u8, Float);
implicit_cast_incompatible!(u8, Double);

implicit_cast_incompatible!(u16, i8);
implicit_cast_incompatible!(u16, i16);
implicit_cast_incompatible!(u16, i32);
implicit_cast_incompatible!(u16, i64);
implicit_cast_incompatible!(u16, isize);
implicit_cast_incompatible!(u16, Float);
implicit_cast_incompatible!(u16, Double);

implicit_cast_incompatible!(u32, i8);
implicit_cast_incompatible!(u32, i16);
implicit_cast_incompatible!(u32, i32);
implicit_cast_incompatible!(u32, i64);
implicit_cast_incompatible!(u32, isize);
implicit_cast_incompatible!(u32, Float);
implicit_cast_incompatible!(u32, Double);

implicit_cast_incompatible!(u64, i8);
implicit_cast_incompatible!(u64, i16);
implicit_cast_incompatible!(u64, i32);
implicit_cast_incompatible!(u64, i64);
implicit_cast_incompatible!(u64, isize);
implicit_cast_incompatible!(u64, Float);
implicit_cast_incompatible!(u64, Double);

implicit_cast_incompatible!(usize, i8);
implicit_cast_incompatible!(usize, i16);
implicit_cast_incompatible!(usize, i32);
implicit_cast_incompatible!(usize, i64);
implicit_cast_incompatible!(usize, isize);
implicit_cast_incompatible!(usize, Float);
implicit_cast_incompatible!(usize, Double);

implicit_cast_incompatible!(i8, Float);
implicit_cast_incompatible!(i8, Double);

implicit_cast_incompatible!(i16, Float);
implicit_cast_incompatible!(i16, Double);

implicit_cast_incompatible!(i32, Float);
implicit_cast_incompatible!(i32, Double);

implicit_cast_incompatible!(i64, Float);
implicit_cast_incompatible!(i64, Double);

implicit_cast_incompatible!(isize, Float);
implicit_cast_incompatible!(isize, Double);

implicit_cast_incompatible!(Float, Double);

/// Represents which data types can be implicitly cast into another.
/// E.g. U32 <= U64.
impl PartialOrd for DataTypeName {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match self {
            DataTypeName::U32 => match other {
                DataTypeName::U32 => Some(Ordering::Equal),
                DataTypeName::U64 => Some(Ordering::Less),
                DataTypeName::Float => None,
                DataTypeName::Double => None,
            },
            DataTypeName::U64 => match other {
                DataTypeName::U32 => Some(Ordering::Greater),
                DataTypeName::U64 => Some(Ordering::Equal),
                DataTypeName::Float => None,
                DataTypeName::Double => None,
            },
            DataTypeName::Float => match other {
                DataTypeName::Float => Some(Ordering::Equal),
                _ => None,
            },
            DataTypeName::Double => match other {
                DataTypeName::Double => Some(Ordering::Equal),
                _ => None,
            },
        }
    }
}
