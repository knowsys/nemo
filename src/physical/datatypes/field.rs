use super::Ring;
use std::ops::Div;

/// Trait representing fields (summarizes various arithmetic traits for brevity)
pub trait Field: Ring + Div<Output = Self> {}

impl<T> Field for T where T: Ring + Div<Output = Self> {}
