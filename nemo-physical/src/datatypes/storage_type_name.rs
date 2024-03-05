use std::{fmt::Display, hash::Hash};

use crate::util::bitset::BitSet;

/// Number of storage types
pub(crate) const NUM_STORAGETYPES: usize = 5;

/// Descriptors to refer to the possible data types at runtime.
#[derive(Clone, Copy, Debug, Eq, PartialEq, PartialOrd, Ord, Hash)]
pub(crate) enum StorageTypeName {
    /// Data type [`u32`], used to store dictionary ids that fit into 32bits. This type always refers to an entry in a
    /// dictionary, rather than to the literal numerical integer value.
    Id32,
    /// Data type [`u64`], used to store dictionary ids. This type always refers to an entry in a
    /// dictionary, rather than to the literal numerical integer value.
    Id64,
    /// Data type [`i64`]. This type always refers to a literal integer value, not to an id in some dictionary.
    Int64,
    /// Data type [`super::float::Float`]
    Float,
    /// Data type [`super::double::Double`]
    Double,
}

/// A list of [StorageTypeName],
/// in the order they appear in the enum.
pub(crate) const STORAFE_TYPES: &[StorageTypeName] = &[
    StorageTypeName::Id32,
    StorageTypeName::Id64,
    StorageTypeName::Int64,
    StorageTypeName::Float,
    StorageTypeName::Double,
];

impl StorageTypeName {
    /// Returns a number that corresponds to the position of that [StorageTypeName]
    /// in the defining enum.
    pub(crate) fn order(&self) -> usize {
        match self {
            StorageTypeName::Id32 => 0,
            StorageTypeName::Id64 => 1,
            StorageTypeName::Int64 => 2,
            StorageTypeName::Float => 3,
            StorageTypeName::Double => 4,
        }
    }

    /// Return a [StorageTypeName] from a number which points to a position
    /// in the defining enum.
    pub(crate) fn from_order(index: usize) -> Self {
        match index {
            0 => StorageTypeName::Id32,
            1 => StorageTypeName::Id64,
            2 => StorageTypeName::Int64,
            3 => StorageTypeName::Float,
            4 => StorageTypeName::Double,
            _ => unreachable!("There is no storage type name for this index"),
        }
    }

    /// Return a [StorageTypeBitSet]
    /// with the bit set to 1 which corresponds to this [StorageTypeName].
    pub(crate) fn bitset(&self) -> StorageTypeBitSet {
        StorageTypeBitSet::from(BitSet::single(self.order()))
    }
}

impl Display for StorageTypeName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StorageTypeName::Id32 => write!(f, "Id32"),
            StorageTypeName::Id64 => write!(f, "Id64"),
            StorageTypeName::Int64 => write!(f, "Int64"),
            StorageTypeName::Float => write!(f, "Float"),
            StorageTypeName::Double => write!(f, "Double"),
        }
    }
}

/// [BitSet] where each bit represents a [StorageTypeName].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct StorageTypeBitSet(BitSet<usize>);

impl StorageTypeBitSet {
    /// Create a [StorageTypeBitSet] which contains no entries.
    pub(crate) fn empty() -> Self {
        Self(BitSet::empty())
    }

    /// Create a [StorageTypeBitSet] which contains every [StorageTypeName].
    pub(crate) fn full() -> Self {
        Self(BitSet::full(NUM_STORAGETYPES))
    }

    /// Only contains the storage types included both `self` and `other`.
    pub(crate) fn intersection(&self, other: Self) -> Self {
        Self(self.0.intersection(other.0))
    }

    /// Contains the storage types included in `self` or `other` (or both).
    pub(crate) fn union(&self, other: Self) -> Self {
        Self(self.0.union(other.0))
    }

    /// Return a list of [StorageTypeName] that are contained in this [BitSet].
    pub(crate) fn storage_types(&self) -> Vec<StorageTypeName> {
        let mut result = Vec::with_capacity(NUM_STORAGETYPES);
        for index in 0..NUM_STORAGETYPES {
            if self.0.get(index) {
                result.push(StorageTypeName::from_order(index))
            }
        }
        result
    }
}

impl From<BitSet<usize>> for StorageTypeBitSet {
    fn from(value: BitSet<usize>) -> Self {
        Self(value)
    }
}
