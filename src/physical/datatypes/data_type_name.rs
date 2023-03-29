use std::fmt::Display;

use super::StorageTypeName;

/// Descriptors to refer to the possible data types that pass the barrier of the physical layer.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DataTypeName {
    /// String Datatype
    String,

    // StorageTypeName from here on
    //
    /// Data type [`u32`].
    U32,
    /// Data type [`u64`].
    U64,
    /// Data type [`super::float::Float`]
    Float,
    /// Data type [`super::double::Double`]
    Double,
}

impl DataTypeName {
    /// Get the appropriate [`StorageTypeName`]` for the given [`DataTypeName`]
    pub fn to_storage_type_name(&self) -> StorageTypeName {
        match self {
            Self::String => StorageTypeName::U64, // dictionary indices
            Self::U32 => StorageTypeName::U32,
            Self::U64 => StorageTypeName::U64,
            Self::Float => StorageTypeName::Float,
            Self::Double => StorageTypeName::Double,
        }
    }
}

impl Display for DataTypeName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::String => write!(f, "String"),
            Self::U32 => write!(f, "U32"),
            Self::U64 => write!(f, "U64"),
            Self::Float => write!(f, "Float"),
            Self::Double => write!(f, "Double"),
        }
    }
}
