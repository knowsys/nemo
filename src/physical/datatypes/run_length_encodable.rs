use std::{
    fmt::Debug,
    ops::{Add, Sub},
};

use bytesize::ByteSize;
use num::Zero;

use crate::physical::management::ByteSized;

/// Data, which can be run length compressed with increments of [`RunLengthEncodable::Step`]
pub trait RunLengthEncodable: Zero {
    /// Increment / Decrement values in run length encoded elements
    type Step: Debug + Copy + Eq + ByteSized;

    /// Increment / Decrement value by calculating curr - prev
    fn diff_step(prev: Self, curr: Self) -> Option<Self::Step>;

    /// Increment value corresponding to 0
    fn zero_step() -> Self::Step {
        Self::diff_step(Self::zero(), Self::zero())
            .expect("default implementation for Stepable::zero_step")
    }

    /// if Step is non-negative (i.e. an 'increment') return its corresponding Value
    fn get_step_increment(_step: Self::Step) -> Option<Self>;

    /// offset self by inc * times
    fn offset(self, inc: Self::Step, times: usize) -> Self;
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
/// Step value for unsigned integer types (u32, u64, usize)
pub struct UintStep(i16);

impl From<i16> for UintStep {
    fn from(value: i16) -> Self {
        UintStep(value)
    }
}

impl UintStep {
    fn offset<T>(t: T, inc: Self, times: usize) -> T
    where
        T: Add<T, Output = T> + TryFrom<usize> + Sub<T, Output = T>,
        <T as TryFrom<usize>>::Error: Debug,
    {
        if inc.0 >= 0 {
            t + T::try_from(inc.0 as usize * times).expect("step multiplication overflow")
        } else {
            // convert to i32, because abs might overflow on 2s-complement architectures
            let abs_offset = (inc.0 as i32).abs();
            t - T::try_from(abs_offset as usize * times).expect("step multiplication overflow")
        }
    }
}

impl ByteSized for UintStep {
    fn size_bytes(&self) -> ByteSize {
        bytesize::ByteSize::b(std::mem::size_of::<UintStep>() as u64)
    }
}

impl RunLengthEncodable for u32 {
    type Step = UintStep;

    fn diff_step(prev: Self, curr: Self) -> Option<Self::Step> {
        Some(UintStep(i16::try_from(curr as i64 - prev as i64).ok()?))
    }

    fn zero_step() -> Self::Step {
        UintStep(0)
    }

    fn get_step_increment(step: Self::Step) -> Option<Self> {
        Self::try_from(step.0).ok()
    }

    fn offset(self, inc: Self::Step, times: usize) -> Self {
        Self::Step::offset(self, inc, times)
    }
}

impl RunLengthEncodable for u64 {
    type Step = UintStep;

    fn diff_step(prev: Self, curr: Self) -> Option<Self::Step> {
        Some(UintStep(i16::try_from(curr as i128 - prev as i128).ok()?))
    }

    fn zero_step() -> Self::Step {
        UintStep(0)
    }

    fn get_step_increment(step: Self::Step) -> Option<Self> {
        Self::try_from(step.0).ok()
    }

    fn offset(self, inc: Self::Step, times: usize) -> Self {
        Self::Step::offset(self, inc, times)
    }
}

impl RunLengthEncodable for usize {
    type Step = UintStep;

    fn diff_step(prev: Self, curr: Self) -> Option<Self::Step> {
        Some(UintStep(i16::try_from(curr as i128 - prev as i128).ok()?))
    }

    fn zero_step() -> Self::Step {
        UintStep(0)
    }

    fn get_step_increment(step: Self::Step) -> Option<Self> {
        Self::try_from(step.0).ok()
    }

    fn offset(self, inc: Self::Step, times: usize) -> Self {
        Self::Step::offset(self, inc, times)
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
/// Step type for small unsigned types like u8
pub struct SmallUintStep(i8);

impl ByteSized for SmallUintStep {
    fn size_bytes(&self) -> ByteSize {
        bytesize::ByteSize::b(std::mem::size_of::<SmallUintStep>() as u64)
    }
}

impl RunLengthEncodable for u8 {
    type Step = SmallUintStep;

    fn diff_step(prev: Self, curr: Self) -> Option<Self::Step> {
        Some(SmallUintStep(i8::try_from(curr as i16 - prev as i16).ok()?))
    }

    fn get_step_increment(step: Self::Step) -> Option<Self> {
        Self::try_from(step.0).ok()
    }

    fn offset(self, inc: Self::Step, times: usize) -> Self {
        u8::try_from(self as i16 + inc.0 as i16 * times as i16).expect("multiplication overflow")
    }
}

#[derive(Debug, PartialEq, Eq, Copy, Clone)]
/// Zero-sized marker type for floating-point run-length encoding
/// (only 0-increment is supported)
pub struct FloatingStep;

impl ByteSized for FloatingStep {
    fn size_bytes(&self) -> ByteSize {
        ByteSize::b(0)
    }
}
