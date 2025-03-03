use num::{One, Zero};
use std::{
    iter::{Product, Sum},
    ops::{Add, Mul, Sub},
};

/// Trait representing rings (summarizes various arithmetic traits for brevity)
pub(crate) trait Ring:
    Add<Output = Self> + Sub<Output = Self> + Mul<Output = Self> + Sum + Product + Zero + One
{
}

impl<T> Ring for T where
    T: Add<Output = Self> + Sub<Output = Self> + Mul<Output = Self> + Sum + Product + Zero + One
{
}
