use crate::physical::dictionary::Dictionary;
use crate::physical::management::database::Dict;

use super::double::Double;
use super::float::Float;
use super::{DataTypeName, StorageValueT};

/// Enum for values that pass the barrier of the physical layer.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DataValueT {
    /// Case String
    String(String),

    // StorageValueT from here on
    //
    /// Case u32
    U32(u32),
    /// Case u64
    U64(u64),
    /// Case Float
    Float(Float),
    /// Case Double
    Double(Double),
}

impl DataValueT {
    /// Returns the type of the DataValueT as DataTypeName
    pub fn get_type(&self) -> DataTypeName {
        match self {
            Self::String(_) => DataTypeName::String,
            Self::U32(_) => DataTypeName::U32,
            Self::U64(_) => DataTypeName::U64,
            Self::Float(_) => DataTypeName::Float,
            Self::Double(_) => DataTypeName::Double,
        }
    }

    /// Get the appropriate [`StorageValueT`]` for the given [`DataValueT`]
    pub fn to_storage_value(&self, dict: &mut Dict) -> StorageValueT {
        match self {
            Self::String(val) => StorageValueT::U64(dict.add(val.clone()).try_into().unwrap()), // dictionary indices
            Self::U32(val) => StorageValueT::U32(*val),
            Self::U64(val) => StorageValueT::U64(*val),
            Self::Float(val) => StorageValueT::Float(*val),
            Self::Double(val) => StorageValueT::Double(*val),
        }
    }
}

impl std::fmt::Display for DataValueT {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::String(val) => write!(f, "{val}"),
            Self::U32(val) => write!(f, "{val}"),
            Self::U64(val) => write!(f, "{val}"),
            Self::Float(val) => write!(f, "{val}"),
            Self::Double(val) => write!(f, "{val}"),
        }
    }
}
