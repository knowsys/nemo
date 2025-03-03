//! This module defines [StorageType],
//! which is an enum containing a variant for each supported storage type.

use std::{fmt::Display, hash::Hash};

use crate::util::bitset::BitSet;

/// Number of storage types
pub(crate) const NUM_STORAGE_TYPES: usize = 5;

/// Descriptors to refer to the possible data types at runtime.
#[derive(Clone, Copy, Debug, Eq, PartialEq, PartialOrd, Ord, Hash)]
pub(crate) enum StorageType {
    /// Data type [u32], used to store dictionary ids that fit into 32bits. This type always refers to an entry in a
    /// dictionary, rather than to the literal numerical integer value.
    Id32,
    /// Data type [u64], used to store dictionary ids. This type always refers to an entry in a
    /// dictionary, rather than to the literal numerical integer value.
    Id64,
    /// Data type [i64]. This type always refers to a literal integer value, not to an id in some dictionary.
    Int64,
    /// Data type [super::float::Float]
    Float,
    /// Data type [super::double::Double]
    Double,
}

/// A list of [StorageType]s,
/// in the order they appear in the enum.
pub(crate) const STORAGE_TYPES: &[StorageType] = &[
    StorageType::Id32,
    StorageType::Id64,
    StorageType::Int64,
    StorageType::Float,
    StorageType::Double,
];

impl StorageType {
    /// Returns a number that corresponds to the position of that [StorageType]
    /// in the defining enum.
    pub(crate) fn order(&self) -> usize {
        match self {
            StorageType::Id32 => 0,
            StorageType::Id64 => 1,
            StorageType::Int64 => 2,
            StorageType::Float => 3,
            StorageType::Double => 4,
        }
    }

    /// Return a [StorageType] from a number which points to a position
    /// in the defining enum.
    pub(crate) fn from_order(index: usize) -> Self {
        match index {
            0 => StorageType::Id32,
            1 => StorageType::Id64,
            2 => StorageType::Int64,
            3 => StorageType::Float,
            4 => StorageType::Double,
            _ => unreachable!("There is no storage type name for this index"),
        }
    }

    /// Return a [StorageTypeBitSet]
    /// with the bit set to 1 which corresponds to this [StorageType].
    pub(crate) fn bitset(&self) -> StorageTypeBitSet {
        StorageTypeBitSet::from(BitSet::single(self.order()))
    }
}

impl Display for StorageType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StorageType::Id32 => write!(f, "Id32"),
            StorageType::Id64 => write!(f, "Id64"),
            StorageType::Int64 => write!(f, "Int64"),
            StorageType::Float => write!(f, "Float"),
            StorageType::Double => write!(f, "Double"),
        }
    }
}

/// [BitSet] where each bit represents a [StorageType].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct StorageTypeBitSet(BitSet<usize>);

impl StorageTypeBitSet {
    /// Create a [StorageTypeBitSet] which contains no entries.
    pub(crate) fn empty() -> Self {
        Self(BitSet::empty())
    }

    /// Create a [StorageTypeBitSet] which contains every [StorageType].
    pub(crate) fn full() -> Self {
        Self(BitSet::full(NUM_STORAGE_TYPES))
    }

    /// Only contains the storage types included both `self` and `other`.
    pub(crate) fn intersection(&self, other: Self) -> Self {
        Self(self.0.intersection(other.0))
    }

    /// Contains the storage types included in `self` or `other` (or both).
    pub(crate) fn union(&self, other: Self) -> Self {
        Self(self.0.union(other.0))
    }

    /// Return a list of [StorageType] that are contained in this [BitSet].
    pub(crate) fn storage_types(&self) -> Vec<StorageType> {
        let mut result = Vec::with_capacity(NUM_STORAGE_TYPES);
        for index in 0..NUM_STORAGE_TYPES {
            if self.0.get(index) {
                result.push(StorageType::from_order(index))
            }
        }
        result
    }

    /// Returns `true` if there is exactly one possible type.
    pub(crate) fn is_unique(&self) -> bool {
        self.0.is_single()
    }

    /// Returns `true` if set contains the given type.
    pub(crate) fn contains(&self, storage_type: &StorageType) -> bool {
        self.0.subset(storage_type.bitset().0)
    }
}

impl From<BitSet<usize>> for StorageTypeBitSet {
    fn from(value: BitSet<usize>) -> Self {
        Self(value)
    }
}
