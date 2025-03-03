//! This module defines [FloorToUsize],
//! a useful trait for [super::RunLengthEncodable] values.

use crate::storagevalues::{double::Double, float::Float};

use num::FromPrimitive;

/// Trait representing datatypes that can be rounded down to usize
pub(crate) trait FloorToUsize {
    /// round down to nearest integer and convert to usize if possible
    fn floor_to_usize(self) -> Option<usize>;
}

impl FloorToUsize for usize {
    fn floor_to_usize(self) -> Option<usize> {
        Some(self)
    }
}

impl FloorToUsize for u64 {
    fn floor_to_usize(self) -> Option<usize> {
        self.try_into().ok()
    }
}

impl FloorToUsize for u32 {
    fn floor_to_usize(self) -> Option<usize> {
        self.try_into().ok()
    }
}

impl FloorToUsize for u16 {
    fn floor_to_usize(self) -> Option<usize> {
        Some(self.into())
    }
}

impl FloorToUsize for u8 {
    fn floor_to_usize(self) -> Option<usize> {
        Some(self.into())
    }
}

impl FloorToUsize for i64 {
    fn floor_to_usize(self) -> Option<usize> {
        self.try_into().ok()
    }
}

impl FloorToUsize for i32 {
    fn floor_to_usize(self) -> Option<usize> {
        self.try_into().ok()
    }
}

impl FloorToUsize for i16 {
    fn floor_to_usize(self) -> Option<usize> {
        self.try_into().ok()
    }
}

impl FloorToUsize for i8 {
    fn floor_to_usize(self) -> Option<usize> {
        self.try_into().ok()
    }
}

impl FloorToUsize for Float {
    fn floor_to_usize(self) -> Option<usize> {
        usize::from_f32(f32::from(self).floor())
    }
}

impl FloorToUsize for Double {
    fn floor_to_usize(self) -> Option<usize> {
        usize::from_f64(f64::from(self).floor())
    }
}
