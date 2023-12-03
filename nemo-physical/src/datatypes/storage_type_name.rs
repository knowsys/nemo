use std::fmt::Display;

/// Number of storage types
pub const NUM_STORAGETYPES: usize = 5;

/// Descriptors to refer to the possible data types at runtime.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StorageTypeName {
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
