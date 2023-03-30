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
    /// Case Float
    Float(Float),
    /// Case Double
    Double(Double),
}

impl StorageValueT {
    /// Returns either [`Option<u32>`], answering whether the [`StorageValueT`] is of this datatype
    pub fn as_u32(&self) -> Option<u32> {
        match *self {
            StorageValueT::U32(val) => Some(val),
            _ => None,
        }
    }
    /// Returns either [`Option<u64>`], answering whether the [`StorageValueT`] is of this datatype
    pub fn as_u64(&self) -> Option<u64> {
        match *self {
            StorageValueT::U64(val) => Some(val),
            _ => None,
        }
    }

    /// Returns either [`Option<Float>`], answering whether [`StorageValueT`] is of this datatype
    pub fn as_float(&self) -> Option<Float> {
        match *self {
            StorageValueT::Float(val) => Some(val),
            _ => None,
        }
    }

    /// Returns either [`Option<Double>`], answering whether the [`StorageValueT`] is of this datatype
    pub fn as_double(&self) -> Option<Double> {
        match *self {
            StorageValueT::Double(val) => Some(val),
            _ => None,
        }
    }

    /// Compares its value with another given [`StorageValueT`]
    pub fn compare(&self, other: &Self) -> Option<Ordering> {
        match self {
            StorageValueT::U32(val) => other.as_u32().map(|otherval| val.cmp(&otherval)),
            StorageValueT::U64(val) => other.as_u64().map(|otherval| val.cmp(&otherval)),
            StorageValueT::Float(val) => other.as_float().map(|otherval| val.cmp(&otherval)),
            StorageValueT::Double(val) => other.as_double().map(|otherval| val.cmp(&otherval)),
        }
    }

    /// Returns the type of the VecT as StorageTypeName
    pub fn get_type(&self) -> StorageTypeName {
        match self {
            Self::U32(_) => StorageTypeName::U32,
            Self::U64(_) => StorageTypeName::U64,
            Self::Float(_) => StorageTypeName::Float,
            Self::Double(_) => StorageTypeName::Double,
        }
    }
}

impl std::fmt::Display for StorageValueT {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::U32(val) => write!(f, "{val}"),
            Self::U64(val) => write!(f, "{val}"),
            Self::Float(val) => write!(f, "{val}"),
            Self::Double(val) => write!(f, "{val}"),
        }
    }
}

/// Enum for vectors of different supported input types
#[derive(Debug)]
pub enum VecT {
    /// Case Vec<u32>
    U32(Vec<u32>),
    /// Case Vec<u64>
    U64(Vec<u64>),
    /// Case Vec<Float>
    Float(Vec<Float>),
    /// Case Vec<Double>
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
            StorageTypeName::Float => Self::Float(Vec::new()),
            StorageTypeName::Double => Self::Double(Vec::new()),
        }
    }

    /// Returns the type of the VecT as StorageTypeName
    pub fn get_type(&self) -> StorageTypeName {
        match self {
            Self::U32(_) => StorageTypeName::U32,
            Self::U64(_) => StorageTypeName::U64,
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
            VecT::Float(vec) => vec.get(index).copied().map(StorageValueT::Float),
            VecT::Double(vec) => vec.get(index).copied().map(StorageValueT::Double),
        }
    }

    /// Inserts the Value to the corresponding Vector if the datatypes are compatible
    /// Note that it is not checked if the [StorageValueT] has the right enum-variant
    pub(crate) fn push(&mut self, value: &StorageValueT) {
        match self {
            VecT::U32(vec) => {
                vec.push(value.as_u32().expect(
                    "expecting VecT::U32 and StorageValueT::U32, but StorageValueT does not match",
                ))
            }
            VecT::U64(vec) => {
                vec.push(value.as_u64().expect(
                    "expecting VecT::U64 and StorageValueT::U64, but StorageValueT does not match",
                ))
            }
            VecT::Float(vec) => vec.push(value.as_float().expect(
                "expecting VecT::Float and StorageValueT::Float, but StorageValueT does not match",
            )),
            VecT::Double(vec) => vec.push(value.as_double().expect(
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
            VecT::Float(vec) => vec
                .get(idx_a)
                .and_then(|&val_a| vec.get(idx_b).map(|val_b| val_a.cmp(val_b))),
            VecT::Double(vec) => vec
                .get(idx_a)
                .and_then(|&val_a| vec.get(idx_b).map(|val_b| val_a.cmp(val_b))),
        }
    }
}
