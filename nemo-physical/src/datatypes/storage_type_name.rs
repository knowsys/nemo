use std::fmt::Display;

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
