use std::cmp::Ordering;

use crate::generate_datatype_forwarder;

use super::double::Double;
use super::float::Float;
use super::StorageTypeName;

/// Enum for values of all supported basic types.
/// This should not be used to represent large numbers of values,
/// due to the overhead for each value, but it can be a convenient
/// option to interface with unknown values.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StorageValueT {
    /// Case u32
    U32(u32),
    /// Case u64
    U64(u64),
    /// Case i64
    I64(i64),
    /// Case Float
    Float(Float),
    /// Case Double
    Double(Double),
}

impl StorageValueT {
    /// Compares its value with another given [`StorageValueT`]
    pub fn compare(&self, other: &Self) -> Option<Ordering> {
        match self {
            StorageValueT::U32(val) => (*other).try_into().map(|otherval| val.cmp(&otherval)).ok(),
            StorageValueT::U64(val) => (*other).try_into().map(|otherval| val.cmp(&otherval)).ok(),
            StorageValueT::I64(val) => (*other).try_into().map(|otherval| val.cmp(&otherval)).ok(),
            StorageValueT::Float(val) => {
                (*other).try_into().map(|otherval| val.cmp(&otherval)).ok()
            }
            StorageValueT::Double(val) => {
                (*other).try_into().map(|otherval| val.cmp(&otherval)).ok()
            }
        }
    }

    /// Returns the type of the VecT as StorageTypeName
    pub fn get_type(&self) -> StorageTypeName {
        match self {
            Self::U32(_) => StorageTypeName::U32,
            Self::U64(_) => StorageTypeName::U64,
            Self::I64(_) => StorageTypeName::I64,
            Self::Float(_) => StorageTypeName::Float,
            Self::Double(_) => StorageTypeName::Double,
        }
    }
}

macro_rules! storage_value_try_into {
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

storage_value_try_into!(U32 => u32);
storage_value_try_into!(U64 => u64);
storage_value_try_into!(I64 => i64);
storage_value_try_into!(Float => Float);
storage_value_try_into!(Double => Double);

impl std::fmt::Display for StorageValueT {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::U32(val) => write!(f, "{val}"),
            Self::U64(val) => write!(f, "{val}"),
            Self::I64(val) => write!(f, "{val}"),
            Self::Float(val) => write!(f, "{val}"),
            Self::Double(val) => write!(f, "{val}"),
        }
    }
}

/// Enum for vectors of different supported input types
#[derive(Debug)]
pub enum VecT {
    /// Case Vec\<u32\>
    U32(Vec<u32>),
    /// Case Vec\<u64\>
    U64(Vec<u64>),
    /// Case Vec\<i64\>
    I64(Vec<i64>),
    /// Case Vec\<Float\>
    Float(Vec<Float>),
    /// Case Vec\<Double\>
    Double(Vec<Double>),
}

impl Default for VecT {
    fn default() -> Self {
        Self::U64(Vec::<u64>::default())
    }
}

generate_datatype_forwarder!(forward_to_vec);

impl VecT {
    /// Creates a new empty VecT for the given StorageTypeName
    pub fn new(dtn: StorageTypeName) -> Self {
        match dtn {
            StorageTypeName::U32 => Self::U32(Vec::new()),
            StorageTypeName::U64 => Self::U64(Vec::new()),
            StorageTypeName::I64 => Self::I64(Vec::new()),
            StorageTypeName::Float => Self::Float(Vec::new()),
            StorageTypeName::Double => Self::Double(Vec::new()),
        }
    }

    /// Returns the type of the VecT as StorageTypeName
    pub fn get_type(&self) -> StorageTypeName {
        match self {
            Self::U32(_) => StorageTypeName::U32,
            Self::U64(_) => StorageTypeName::U64,
            Self::I64(_) => StorageTypeName::I64,
            Self::Float(_) => StorageTypeName::Float,
            Self::Double(_) => StorageTypeName::Double,
        }
    }

    /// Removes the last element in the corresponding vector
    pub fn pop(&mut self) {
        forward_to_vec!(self, pop;)
    }

    /// Get the value at the given index as StorageValueT
    pub fn get(&self, index: usize) -> Option<StorageValueT> {
        match self {
            VecT::U32(vec) => vec.get(index).copied().map(StorageValueT::U32),
            VecT::U64(vec) => vec.get(index).copied().map(StorageValueT::U64),
            VecT::I64(vec) => vec.get(index).copied().map(StorageValueT::I64),
            VecT::Float(vec) => vec.get(index).copied().map(StorageValueT::Float),
            VecT::Double(vec) => vec.get(index).copied().map(StorageValueT::Double),
        }
    }

    /// Inserts the Value to the corresponding Vector if the datatypes are compatible
    /// Note that it is not checked if the [StorageValueT] has the right enum-variant
    pub(crate) fn push(&mut self, value: StorageValueT) {
        match self {
            VecT::U32(vec) => {
                vec.push(value.try_into().expect(
                    "expecting VecT::U32 and StorageValueT::U32, but StorageValueT does not match",
                ))
            }
            VecT::U64(vec) => {
                vec.push(value.try_into().expect(
                    "expecting VecT::U64 and StorageValueT::U64, but StorageValueT does not match",
                ))
            }
            VecT::I64(vec) => {
                vec.push(value.try_into().expect(
                    "expecting VecT::I64 and StorageValueT::I64, but StorageValueT does not match",
                ))
            }
            VecT::Float(vec) => vec.push(value.try_into().expect(
                "expecting VecT::Float and StorageValueT::Float, but StorageValueT does not match",
            )),
            VecT::Double(vec) => vec.push(value.try_into().expect(
                "expecting VecT::Double and StorageValueT::Double, but StorageValueT does not match",
            )),
        };
    }

    /// Returns the lengths of the contained Vector
    pub fn len(&self) -> usize {
        forward_to_vec!(self, len)
    }

    /// Returns whether the vector is empty, or not
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Compares two values at the given index-points with each other
    pub fn compare_idx(&self, idx_a: usize, idx_b: usize) -> Option<Ordering> {
        match self {
            VecT::U32(vec) => vec
                .get(idx_a)
                .and_then(|&val_a| vec.get(idx_b).map(|val_b| val_a.cmp(val_b))),
            VecT::U64(vec) => vec
                .get(idx_a)
                .and_then(|&val_a| vec.get(idx_b).map(|val_b| val_a.cmp(val_b))),
            VecT::I64(vec) => vec
                .get(idx_a)
                .and_then(|&val_a| vec.get(idx_b).map(|val_b| val_a.cmp(val_b))),
            VecT::Float(vec) => vec
                .get(idx_a)
                .and_then(|&val_a| vec.get(idx_b).map(|val_b| val_a.cmp(val_b))),
            VecT::Double(vec) => vec
                .get(idx_a)
                .and_then(|&val_a| vec.get(idx_b).map(|val_b| val_a.cmp(val_b))),
        }
    }
}
