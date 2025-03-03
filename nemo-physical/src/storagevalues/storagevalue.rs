//! This module defines the trait [StorageValue]
//! and provides implementations for all appropriate types.

use std::fmt::Debug;

use crate::columnar::columntype::ColumnType;

use super::{double::Double, float::Float, storagetype::StorageType};

/// Trait implemented by all types that appear in the data column of a
/// [crate::columnar::column::Column].
pub(crate) trait StorageValue: Debug + Copy + ColumnType + Into<StorageValueT> {}
impl<T> StorageValue for T where T: Debug + Copy + ColumnType + Into<StorageValueT> {}

/// Enum for values of all supported storage types.
/// This should not be used to represent large numbers of values,
/// due to the overhead for each value, but it can be a convenient
/// option to interface with unknown values.
///
/// Ord and PartialOrd assume U32 < U64 < I64 < Float < Double.
/// More information at <https://doc.rust-lang.org/std/cmp/trait.PartialOrd.html#derivable>
#[derive(Clone, Copy, Debug, PartialEq, Eq, Ord, PartialOrd)]
pub(crate) enum StorageValueT {
    /// A value of type [StorageType::Id32]. Such values always refer to an entry in a
    /// dictionary, rather than to the literal numerical integer value.
    Id32(u32),
    /// A value of type [StorageType::Id64]. Such values always refer to an entry in a
    /// dictionary, rather than to the literal numerical integer value.
    Id64(u64),
    /// A value of type [StorageType::Int64]. Such values always refer to a literal
    /// numerical integer value rather than to an entry in a dictionary.
    Int64(i64),
    /// A value of type [StorageType::Float].
    Float(Float),
    /// A value of type [StorageType::Double].
    Double(Double),
}

impl StorageValueT {
    /// Returns the type of the VecT as StorageType
    pub(crate) fn storage_type(&self) -> StorageType {
        match self {
            Self::Id32(_) => StorageType::Id32,
            Self::Id64(_) => StorageType::Id64,
            Self::Int64(_) => StorageType::Int64,
            Self::Float(_) => StorageType::Float,
            Self::Double(_) => StorageType::Double,
        }
    }
}

macro_rules! storage_value_from {
    ($variant:ident => $dst:ty) => {
        impl TryFrom<StorageValueT> for $dst {
            type Error = ();

            fn try_from(value: StorageValueT) -> Result<Self, Self::Error> {
                match value {
                    StorageValueT::$variant(val) => Ok(val),
                    _ => Err(()),
                }
            }
        }

        impl From<$dst> for StorageValueT {
            fn from(value: $dst) -> StorageValueT {
                StorageValueT::$variant(value)
            }
        }
    };
}

storage_value_from!(Id32 => u32);
storage_value_from!(Id64 => u64);
storage_value_from!(Int64 => i64);
storage_value_from!(Float => Float);
storage_value_from!(Double => Double);

#[cfg(test)]
mod tests {
    use super::StorageValueT;
    use super::{Double, Float};

    #[test]
    fn storagevaluet_comparison() {
        assert!(StorageValueT::Id32(11) < StorageValueT::Id64(10));
        assert!(StorageValueT::Id32(11) < StorageValueT::Id64(11));
        assert!(StorageValueT::Id32(11) < StorageValueT::Id64(12));

        assert!(StorageValueT::Id64(11) < StorageValueT::Int64(10));
        assert!(StorageValueT::Id64(11) < StorageValueT::Int64(11));
        assert!(StorageValueT::Id64(11) < StorageValueT::Int64(12));

        let f10 = Float::new(10.0).unwrap();
        let f11 = Float::new(11.0).unwrap();
        let f12 = Float::new(12.0).unwrap();

        assert!(StorageValueT::Int64(11) < StorageValueT::Float(f10));
        assert!(StorageValueT::Int64(11) < StorageValueT::Float(f11));
        assert!(StorageValueT::Int64(11) < StorageValueT::Float(f12));

        let d10 = Double::new(10.0).unwrap();
        let d11 = Double::new(11.0).unwrap();
        let d12 = Double::new(12.0).unwrap();

        assert!(StorageValueT::Float(f11) < StorageValueT::Double(d10));
        assert!(StorageValueT::Float(f11) < StorageValueT::Double(d11));
        assert!(StorageValueT::Float(f11) < StorageValueT::Double(d12));
    }
}
