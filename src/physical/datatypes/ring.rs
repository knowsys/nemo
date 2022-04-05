use num::Zero;
use std::{
    iter::Sum,
    ops::{Add, Mul, Sub},
};

/// Trait representing commutative rings (summarizes various arithmetic traits for brevity)
pub trait Ring: Add<Output = Self> + Sub<Output = Self> + Mul<Output = Self> + Sum + Zero {}

impl<T> Ring for T where T: Add<Output = Self> + Sub<Output = Self> + Mul<Output = Self> + Sum + Zero
{}
