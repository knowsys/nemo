//! This module implements [BitSet].

use std::ops::{BitAnd, BitOr, BitXor, Not, Shl, Shr, Sub};

use num::One;

/// Trait that defines an object on which boolean operations can be performed
pub(crate) trait BooleanOperations:
    Default
    + Clone
    + Copy
    + Sized
    + Not<Output = Self>
    + BitAnd<Output = Self>
    + BitOr<Output = Self>
    + BitXor<Output = Self>
    + Shl<usize, Output = Self>
    + Shr<usize, Output = Self>
    + Sub<Output = Self>
    + One
    + PartialEq
    + Eq
{
}

impl BooleanOperations for u64 {}
impl BooleanOperations for u32 {}
impl BooleanOperations for usize {}

/// A set implemented as a bit field
#[derive(Debug, Copy, Clone, Default, PartialEq, Eq)]
pub(crate) struct BitSet<T: BooleanOperations>(T);

impl<T> BitSet<T>
where
    T: BooleanOperations,
{
    /// Create a [BitSet] with no entries.
    pub(crate) fn empty() -> Self {
        Self::default()
    }

    /// Create a [BitSet] where every element is included.
    pub(crate) fn full(elements: usize) -> Self {
        let mut result = T::one() << elements;
        result = result - T::one();

        Self(result)
    }

    /// Create a [BitSet] with a single set bit.
    pub(crate) fn single(index: usize) -> Self {
        Self(T::one() << index)
    }

    /// Set a single bit to some value
    pub(crate) fn set(&mut self, index: usize, value: bool) {
        if value {
            let mask = Self::single(index);
            *self = self.union(mask);
        } else {
            let mask = Self(!(T::one() << index));
            *self = self.intersection(mask);
        }
    }

    /// Get the value of a spefic bit
    pub(crate) fn get(&self, index: usize) -> bool {
        self.intersection(Self::single(index)) != Self::default()
    }

    /// Compute the intersection of two [BitSet]s.
    pub(crate) fn intersection(&self, other: Self) -> Self {
        Self(self.0 & other.0)
    }

    /// Compute the union of two [BitSet]s.
    pub(crate) fn union(&self, other: Self) -> Self {
        Self(self.0 | other.0)
    }
}

#[cfg(test)]
mod test {
    use super::BitSet;

    type TestBitSet = BitSet<usize>;

    #[test]
    fn bitset_create() {
        let empty = TestBitSet::empty();
        assert_eq!(empty.0, 0);

        let second = TestBitSet::single(2);
        assert_eq!(second.0, 4);

        let full = TestBitSet::full(5);
        assert_eq!(full.0, 31);
    }

    #[test]
    fn bitset_set() {
        let mut bits = TestBitSet::single(1);

        bits.set(3, true);
        bits.set(0, true);
        bits.set(5, true);
        bits.set(1, false);
        bits.set(3, false);

        assert_eq!(bits.0, 33)
    }

    #[test]
    fn bitset_get() {
        let bits = TestBitSet::full(3);

        assert_eq!(bits.get(0), true);
        assert_eq!(bits.get(1), true);
        assert_eq!(bits.get(2), true);
        assert_eq!(bits.get(3), false);
        assert_eq!(bits.get(4), false);
    }

    #[test]
    fn bitset_intersection() {
        let mut bits_a = TestBitSet::empty();
        bits_a.set(0, true);
        bits_a.set(2, true);
        bits_a.set(3, true);
        bits_a.set(6, true);

        let mut bits_b = TestBitSet::empty();
        bits_b.set(0, true);
        bits_b.set(1, true);
        bits_b.set(3, true);
        bits_b.set(4, true);

        let mut bits_expected = TestBitSet::empty();
        bits_expected.set(0, true);
        bits_expected.set(3, true);

        let bits_result = bits_a.intersection(bits_b);

        assert_eq!(bits_result, bits_expected);
    }
}
