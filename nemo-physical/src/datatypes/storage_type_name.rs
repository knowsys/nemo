use std::fmt::Display;

use crate::error::Error;

use super::StorageValueT;

/// Descriptors to refer to the possible data types at runtime.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StorageTypeName {
    /// Data type [`u32`].
    U32,
    /// Data type [`u64`].
    U64,
    /// Data type [`i64`]
    I64,
    /// Data type [`super::float::Float`]
    Float,
    /// Data type [`super::double::Double`]
    Double,
}

impl StorageTypeName {
    /// Parses a string, based on the name of the Datatype
    pub fn parse(&self, string: &str) -> Result<StorageValueT, Error> {
        Ok(match self {
            StorageTypeName::U32 => StorageValueT::U32(string.parse()?),
            StorageTypeName::U64 => StorageValueT::U64(string.parse()?),
            StorageTypeName::I64 => StorageValueT::I64(string.parse()?),
            StorageTypeName::Float => StorageValueT::Float(super::Float::new(string.parse()?)?),
            StorageTypeName::Double => StorageValueT::Double(super::Double::new(string.parse()?)?),
        })
    }
}

impl Display for StorageTypeName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StorageTypeName::U32 => write!(f, "U32"),
            StorageTypeName::U64 => write!(f, "U64"),
            StorageTypeName::I64 => write!(f, "I64"),
            StorageTypeName::Float => write!(f, "Float"),
            StorageTypeName::Double => write!(f, "Double"),
        }
    }
}
