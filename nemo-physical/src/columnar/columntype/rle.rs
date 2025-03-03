//! This module defines the trait [RunLengthEncodable],
//! which should be implemented by all types that can be stored
//! within an [crate::columnar::column::rle::ColumnRle].

use std::{
    fmt::Debug,
    ops::{Add, Div, Mul, Sub},
};

use num::Zero;

use crate::{
    management::bytesized::ByteSized,
    storagevalues::{double::Double, float::Float},
};

use super::floor_to_usize::FloorToUsize;

/// Data, which can be run length compressed with increments of [RunLengthEncodable::Step]
pub(crate) trait RunLengthEncodable:
    Zero
    + FloorToUsize
    + Add<Output = Self>
    + Sub<Output = Self>
    + Mul<Output = Self>
    + Div<Output = Self>
{
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
pub(crate) struct IntStep(i16);

impl From<i16> for IntStep {
    fn from(value: i16) -> Self {
        IntStep(value)
    }
}

impl IntStep {
    fn offset_unsigned<T>(t: T, inc: Self, times: usize) -> T
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

    fn offset_signed<T>(t: T, inc: Self, times: usize) -> T
    where
        T: Add<T, Output = T> + From<i16> + TryFrom<usize> + Mul<T, Output = T>,
        <T as TryFrom<usize>>::Error: Debug,
    {
        t + T::from(inc.0) * T::try_from(times).expect("step multiplication overflow")
    }
}

impl ByteSized for IntStep {
    fn size_bytes(&self) -> u64 {
        size_of::<IntStep>() as u64
    }
}

impl RunLengthEncodable for u32 {
    type Step = IntStep;

    fn diff_step(prev: Self, curr: Self) -> Option<Self::Step> {
        Some(IntStep(i16::try_from(curr as i64 - prev as i64).ok()?))
    }

    fn zero_step() -> Self::Step {
        IntStep(0)
    }

    fn get_step_increment(step: Self::Step) -> Option<Self> {
        Self::try_from(step.0).ok()
    }

    fn offset(self, inc: Self::Step, times: usize) -> Self {
        Self::Step::offset_unsigned(self, inc, times)
    }
}

impl RunLengthEncodable for u64 {
    type Step = IntStep;

    fn diff_step(prev: Self, curr: Self) -> Option<Self::Step> {
        Some(IntStep(i16::try_from(curr as i128 - prev as i128).ok()?))
    }

    fn zero_step() -> Self::Step {
        IntStep(0)
    }

    fn get_step_increment(step: Self::Step) -> Option<Self> {
        Self::try_from(step.0).ok()
    }

    fn offset(self, inc: Self::Step, times: usize) -> Self {
        Self::Step::offset_unsigned(self, inc, times)
    }
}

impl RunLengthEncodable for usize {
    type Step = IntStep;

    fn diff_step(prev: Self, curr: Self) -> Option<Self::Step> {
        Some(IntStep(i16::try_from(curr as i128 - prev as i128).ok()?))
    }

    fn zero_step() -> Self::Step {
        IntStep(0)
    }

    fn get_step_increment(step: Self::Step) -> Option<Self> {
        Self::try_from(step.0).ok()
    }

    fn offset(self, inc: Self::Step, times: usize) -> Self {
        Self::Step::offset_unsigned(self, inc, times)
    }
}

impl RunLengthEncodable for i32 {
    type Step = IntStep;

    fn diff_step(prev: Self, curr: Self) -> Option<Self::Step> {
        Some(IntStep(i16::try_from(curr.checked_sub(prev)?).ok()?))
    }

    fn get_step_increment(step: Self::Step) -> Option<Self> {
        Some(step.0 as i32)
    }

    fn offset(self, inc: Self::Step, times: usize) -> Self {
        Self::Step::offset_signed(self, inc, times)
    }
}

impl RunLengthEncodable for i64 {
    type Step = IntStep;

    fn diff_step(prev: Self, curr: Self) -> Option<Self::Step> {
        Some(IntStep(i16::try_from(curr.checked_sub(prev)?).ok()?))
    }

    fn get_step_increment(step: Self::Step) -> Option<Self> {
        Some(step.0 as i64)
    }

    fn offset(self, inc: Self::Step, times: usize) -> Self {
        Self::Step::offset_signed(self, inc, times)
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
/// Step type for small unsigned types like u8
pub(crate) struct SmallIntStep(i8);

impl ByteSized for SmallIntStep {
    fn size_bytes(&self) -> u64 {
        size_of::<SmallIntStep>() as u64
    }
}

impl RunLengthEncodable for u8 {
    type Step = SmallIntStep;

    fn diff_step(prev: Self, curr: Self) -> Option<Self::Step> {
        Some(SmallIntStep(i8::try_from(curr as i16 - prev as i16).ok()?))
    }

    fn get_step_increment(step: Self::Step) -> Option<Self> {
        Self::try_from(step.0).ok()
    }

    fn offset(self, inc: Self::Step, times: usize) -> Self {
        let times =
            u8::try_from(times).expect("rle-element should not be greater than value range");
        u8::try_from(self as i16 + inc.0 as i16 * times as i16).expect("multiplication overflow")
    }
}

impl RunLengthEncodable for i8 {
    type Step = SmallIntStep;

    fn diff_step(prev: Self, curr: Self) -> Option<Self::Step> {
        Some(SmallIntStep(curr.checked_sub(prev)?))
    }

    fn get_step_increment(step: Self::Step) -> Option<Self> {
        Some(step.0)
    }

    fn offset(self, inc: Self::Step, times: usize) -> Self {
        let times =
            i8::try_from(times).expect("rle-element should not be greater than value range");
        self + inc.0 * times
    }
}

impl RunLengthEncodable for Float {
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

#[derive(Debug, PartialEq, Eq, Copy, Clone)]
/// Zero-sized marker type for floating-point run-length encoding
/// (only 0-increment is supported)
pub(crate) struct FloatingStep;

impl ByteSized for FloatingStep {
    fn size_bytes(&self) -> u64 {
        0
    }
}
