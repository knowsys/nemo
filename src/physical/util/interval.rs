//! Module defining an interval type

/// Bound of an interval.
#[derive(Debug, Clone)]
pub enum IntervalBound<T: Clone> {
    /// Bounded and includes the provided value.
    Inclusive(T),
    /// Bounded but does not include the provided value.
    Exclusive(T),
    /// Unbounded in this direction.
    Unbounded,
}

impl<T: Clone> IntervalBound<T> {
    /// If the interval is bounded, return the bound
    pub fn bound(&self) -> Option<T> {
        match self {
            IntervalBound::Inclusive(bound) | IntervalBound::Exclusive(bound) => {
                Some(bound.clone())
            }
            IntervalBound::Unbounded => None,
        }
    }

    /// Return whether the interval bound is inclusive.
    pub fn inclusive(&self) -> bool {
        matches!(self, IntervalBound::Inclusive(_))
    }

    /// Return whether the interval bound is exclusive.
    pub fn exclusive(&self) -> bool {
        matches!(self, IntervalBound::Exclusive(_))
    }

    /// Return whether the interval is unbounded.
    pub fn unbounded(&self) -> bool {
        matches!(self, IntervalBound::Unbounded)
    }
}

impl<T: Ord + Clone> IntervalBound<T> {
    /// Return true iff the given value meets the requirement of `self` is used as a lower bound.
    pub fn above(&self, value: &T) -> bool {
        match self {
            IntervalBound::Inclusive(bound) => value >= bound,
            IntervalBound::Exclusive(bound) => value > bound,
            IntervalBound::Unbounded => true,
        }
    }

    /// Return true iff the given value meets the requirement of `self` is used as an upper bound.
    pub fn below(&self, value: &T) -> bool {
        match self {
            IntervalBound::Inclusive(bound) => value <= bound,
            IntervalBound::Exclusive(bound) => value < bound,
            IntervalBound::Unbounded => true,
        }
    }
}

/// Represents an interval between (up to) two values.
#[derive(Debug, Clone)]
pub struct Interval<T: Clone> {
    /// The lower bound.
    pub lower: IntervalBound<T>,
    /// The upper bound.
    pub upper: IntervalBound<T>,
}

impl<T: Clone> Interval<T> {
    /// Create a new [`Interval`].
    pub fn new(lower: IntervalBound<T>, upper: IntervalBound<T>) -> Self {
        Self { lower, upper }
    }

    /// Create a new [`Interval`] which covers exactly one element.
    pub fn single(value: T) -> Self {
        Self {
            lower: IntervalBound::Inclusive(value.clone()),
            upper: IntervalBound::Inclusive(value),
        }
    }
}

impl<T: Ord + Clone> Interval<T> {
    /// Return true iff the given value is within the ranges of this interval.
    pub fn contains(&self, value: &T) -> bool {
        self.lower.above(&value) && self.upper.below(&value)
    }
}
