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

#[derive(Debug, Copy, Clone)]
/// Step value for unsigned integer types (u32, u64, usize)
pub enum UintStep {
    /// Increment by the saved value
    Increment(u16),
    /// Decrement by the saved value
    Decrement(u16),
}

impl UintStep {
    fn offset<T>(t: T, inc: Self, times: usize) -> T
    where
        T: Add<T, Output = T> + TryFrom<usize> + Sub<T, Output = T>,
        <T as TryFrom<usize>>::Error: Debug,
    {
        match inc {
            UintStep::Increment(i) => {
                t + T::try_from(i as usize * times).expect("step multiplication overflow")
            }
            UintStep::Decrement(d) => {
                t - T::try_from(d as usize * times).expect("step multiplication overflow")
            }
        }
    }
}

impl ByteSized for UintStep {
    fn size_bytes(&self) -> ByteSize {
        bytesize::ByteSize::b(std::mem::size_of::<UintStep>() as u64)
    }
}

impl PartialEq for UintStep {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Increment(l0), Self::Increment(r0)) => l0 == r0,
            (Self::Decrement(l0), Self::Decrement(r0)) => l0 == r0,
            (Self::Increment(0), Self::Decrement(0)) => true,
            (Self::Decrement(0), Self::Increment(0)) => true,
            _ => false,
        }
    }
}

impl Eq for UintStep {}

impl RunLengthEncodable for u32 {
    type Step = UintStep;

    fn diff_step(prev: Self, curr: Self) -> Option<Self::Step> {
        if prev <= curr {
            let inc = u16::try_from(curr - prev).ok()?;
            Some(UintStep::Increment(inc))
        } else {
            let dec = u16::try_from(prev - curr).ok()?;
            Some(UintStep::Decrement(dec))
        }
    }

    fn zero_step() -> Self::Step {
        UintStep::Increment(0)
    }

    fn get_step_increment(step: Self::Step) -> Option<Self> {
        match step {
            UintStep::Increment(i) => Some(i.into()),
            UintStep::Decrement(_) => None,
        }
    }

    fn offset(self, inc: Self::Step, times: usize) -> Self {
        Self::Step::offset(self, inc, times)
    }
}

impl RunLengthEncodable for u64 {
    type Step = UintStep;

    fn diff_step(prev: Self, curr: Self) -> Option<Self::Step> {
        if prev <= curr {
            let inc = u16::try_from(curr - prev).ok()?;
            Some(UintStep::Increment(inc))
        } else {
            let dec = u16::try_from(prev - curr).ok()?;
            Some(UintStep::Decrement(dec))
        }
    }

    fn zero_step() -> Self::Step {
        UintStep::Increment(0)
    }

    fn get_step_increment(step: Self::Step) -> Option<Self> {
        match step {
            UintStep::Increment(i) => Some(i.into()),
            UintStep::Decrement(_) => None,
        }
    }

    fn offset(self, inc: Self::Step, times: usize) -> Self {
        Self::Step::offset(self, inc, times)
    }
}

impl RunLengthEncodable for usize {
    type Step = UintStep;

    fn diff_step(prev: Self, curr: Self) -> Option<Self::Step> {
        if prev <= curr {
            let inc = u16::try_from(curr - prev).ok()?;
            Some(UintStep::Increment(inc))
        } else {
            let dec = u16::try_from(prev - curr).ok()?;
            Some(UintStep::Decrement(dec))
        }
    }

    fn zero_step() -> Self::Step {
        UintStep::Increment(0)
    }

    fn get_step_increment(step: Self::Step) -> Option<Self> {
        match step {
            UintStep::Increment(i) => Some(i.into()),
            UintStep::Decrement(_) => None,
        }
    }

    fn offset(self, inc: Self::Step, times: usize) -> Self {
        Self::Step::offset(self, inc, times)
    }
}

#[derive(Debug, Copy, Clone)]
/// Step type for small unsigned types like u8
pub enum SmallUintStep {
    /// Increment by the saved value
    Increment(u8),
    /// Decrement by the saved value
    Decrement(u8),
}

impl ByteSized for SmallUintStep {
    fn size_bytes(&self) -> ByteSize {
        bytesize::ByteSize::b(std::mem::size_of::<SmallUintStep>() as u64)
    }
}

impl PartialEq for SmallUintStep {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Increment(l0), Self::Increment(r0)) => l0 == r0,
            (Self::Decrement(l0), Self::Decrement(r0)) => l0 == r0,
            (Self::Increment(0), Self::Decrement(0)) => true,
            (Self::Decrement(0), Self::Increment(0)) => true,
            _ => false,
        }
    }
}

impl Eq for SmallUintStep {}

impl RunLengthEncodable for u8 {
    type Step = SmallUintStep;

    fn diff_step(prev: Self, curr: Self) -> Option<Self::Step> {
        if prev <= curr {
            Some(SmallUintStep::Increment(curr - prev))
        } else {
            Some(SmallUintStep::Decrement(prev - curr))
        }
    }

    fn get_step_increment(step: Self::Step) -> Option<Self> {
        match step {
            SmallUintStep::Increment(inc) => Some(inc),
            SmallUintStep::Decrement(_) => None,
        }
    }

    fn offset(self, inc: Self::Step, times: usize) -> Self {
        match inc {
            SmallUintStep::Increment(i) => {
                self + (i * u8::try_from(times).expect("times to large"))
            }
            SmallUintStep::Decrement(d) => {
                self - (d * u8::try_from(times).expect("times to large"))
            }
        }
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
