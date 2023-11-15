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

/// Number of available storage types
const NUM_STORAGE_TYPES: usize = 5;

/// Order of storage types
const STORAGE_TYPE_ORDER: [StorageTypeName; NUM_STORAGE_TYPES] = [
    StorageTypeName::U64,
    StorageTypeName::I64,
    StorageTypeName::Double,
    StorageTypeName::U32,
    StorageTypeName::Float,
];

impl Ord for StorageTypeName {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        let index_self = STORAGE_TYPE_ORDER
            .iter()
            .position(|s| self == s)
            .expect("Array contians every storage type");

        let index_other = STORAGE_TYPE_ORDER
            .iter()
            .position(|&s| self == other)
            .expect("Array contians every storage type");

        index_self.cmp(&index_other)
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
