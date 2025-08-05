use delegate::delegate;

use crate::{datavalues::AnyDataValue, dictionary::meta_dv_dict::MetaDvDictionary};

use super::{double::Double, float::Float, into_datavalue::IntoDataValue, StorageTypeName};

/// Enum for values of all supported basic types.
/// This should not be used to represent large numbers of values,
/// due to the overhead for each value, but it can be a convenient
/// option to interface with unknown values.
///
/// Ord and PartialOrd assume U32 < U64 < I64 < Float < Double.
/// More information at <https://doc.rust-lang.org/std/cmp/trait.PartialOrd.html#derivable>
#[derive(Clone, Copy, Debug, PartialEq, Eq, Ord, PartialOrd)]
pub enum StorageValueT {
    /// A value of type [StorageTypeName::Id32]. Such values always refer to an entry in a
    /// dictionary, rather than to the literal numerical integer value.
    Id32(u32),
    /// A value of type [StorageTypeName::Id64]. Such values always refer to an entry in a
    /// dictionary, rather than to the literal numerical integer value.
    Id64(u64),
    /// A value of type [StorageTypeName::Int64]. Such values always refer to a literal
    /// numerical integer value rather than to an entry in a dictionary.
    Int64(i64),
    /// A value of type [StorageTypeName::Float].
    Float(Float),
    /// A value of type [StorageTypeName::Double].
    Double(Double),
}

impl StorageValueT {
    /// Returns the type of the VecT as StorageTypeName
    pub(crate) fn get_type(&self) -> StorageTypeName {
        match self {
            Self::Id32(_) => StorageTypeName::Id32,
            Self::Id64(_) => StorageTypeName::Id64,
            Self::Int64(_) => StorageTypeName::Int64,
            Self::Float(_) => StorageTypeName::Float,
            Self::Double(_) => StorageTypeName::Double,
        }
    }
}

macro_rules! storage_value_try_from {
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
    };
}

storage_value_try_from!(Id32 => u32);
storage_value_try_from!(Id64 => u64);
storage_value_try_from!(Int64 => i64);
storage_value_try_from!(Float => Float);
storage_value_try_from!(Double => Double);

impl std::fmt::Display for StorageValueT {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Id32(val) => write!(f, "{val}"),
            Self::Id64(val) => write!(f, "{val}"),
            Self::Int64(val) => write!(f, "{val}"),
            Self::Float(val) => write!(f, "{val}"),
            Self::Double(val) => write!(f, "{val}"),
        }
    }
}

impl From<u32> for StorageValueT {
    fn from(value: u32) -> Self {
        StorageValueT::Id32(value)
    }
}

impl From<u64> for StorageValueT {
    fn from(value: u64) -> Self {
        StorageValueT::Id64(value)
    }
}

impl From<i64> for StorageValueT {
    fn from(value: i64) -> Self {
        StorageValueT::Int64(value)
    }
}

impl From<Float> for StorageValueT {
    fn from(value: Float) -> Self {
        StorageValueT::Float(value)
    }
}

impl From<Double> for StorageValueT {
    fn from(value: Double) -> Self {
        StorageValueT::Double(value)
    }
}

impl IntoDataValue for StorageValueT {
    delegate! {
        to match self {
            Self::Id32(value) => value,
            Self::Id64(value) => value,
            Self::Int64(value) => value,
            Self::Float(value) => value,
            Self::Double(value) => value,
        } {
            fn into_datavalue(self, dictionary: &MetaDvDictionary) -> Option<AnyDataValue>;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::{double::Double, float::Float};
    use super::StorageValueT;
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
