use std::cmp::Ordering;

use crate::generate_datatype_forwarder;

use super::double::Double;
use super::float::Float;
use super::DataTypeName;

/// Enum for values of all supported basic types.
/// This should not be used to represent large numbers of values,
/// due to the overhead for each value, but it can be a convenient
/// option to interface with unknown values.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DataValueT {
    /// Case u64
    U64(u64),
    /// Case Float
    Float(Float),
    /// Case Double
    Double(Double),
}

impl DataValueT {
    /// Returns either [`Option<u64>`], answering whether the [`DataValueT`] is of this datatype
    pub fn as_u64(&self) -> Option<u64> {
        match *self {
            DataValueT::U64(val) => Some(val),
            _ => None,
        }
    }

    /// Returns either [`Option<Float>`], answering whether [`DataValueT`] is of this datatype
    pub fn as_float(&self) -> Option<Float> {
        match *self {
            DataValueT::Float(val) => Some(val),
            _ => None,
        }
    }

    /// Returns either [`Option<Double>`], answering whether the [`DataValueT`] is of this datatype
    pub fn as_double(&self) -> Option<Double> {
        match *self {
            DataValueT::Double(val) => Some(val),
            _ => None,
        }
    }

    /// Compares its value with another given [`DataValueT`]
    pub fn compare(&self, other: &Self) -> Option<Ordering> {
        match self {
            DataValueT::U64(val) => other.as_u64().map(|otherval| val.cmp(&otherval)),
            DataValueT::Float(val) => other.as_float().map(|otherval| val.cmp(&otherval)),
            DataValueT::Double(val) => other.as_double().map(|otherval| val.cmp(&otherval)),
        }
    }

    /// Returns the type of the VecT as DataTypeName
    pub fn get_type(&self) -> DataTypeName {
        match self {
            Self::U64(_) => DataTypeName::U64,
            Self::Float(_) => DataTypeName::Float,
            Self::Double(_) => DataTypeName::Double,
        }
    }
}

impl std::fmt::Display for DataValueT {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::U64(val) => write!(f, "{}", val),
            Self::Float(val) => write!(f, "{}", val),
            Self::Double(val) => write!(f, "{}", val),
        }
    }
}

/// Enum for vectors of different supported input types
#[derive(Debug)]
pub enum VecT {
    /// Case Vec<u64>
    U64(Vec<u64>),
    /// Case Vec<Float>
    Float(Vec<Float>),
    /// Case Vec<Double>
    Double(Vec<Double>),
}

generate_datatype_forwarder!(forward_to_vec);

impl VecT {
    /// Creates a new empty VecT for the given DataTypeName
    pub fn new(dtn: DataTypeName) -> Self {
        match dtn {
            DataTypeName::U64 => Self::U64(Vec::new()),
            DataTypeName::Float => Self::Float(Vec::new()),
            DataTypeName::Double => Self::Double(Vec::new()),
        }
    }

    /// Returns the type of the VecT as DataTypeName
    pub fn get_type(&self) -> DataTypeName {
        match self {
            Self::U64(_) => DataTypeName::U64,
            Self::Float(_) => DataTypeName::Float,
            Self::Double(_) => DataTypeName::Double,
        }
    }

    /// Removes the last element in the corresponding vector
    pub fn pop(&mut self) {
        forward_to_vec!(self, pop;)
    }

    /// Get the value at the given index as DataValueT
    pub fn get(&self, index: usize) -> Option<DataValueT> {
        match self {
            VecT::U64(vec) => vec.get(index).copied().map(DataValueT::U64),
            VecT::Float(vec) => vec.get(index).copied().map(DataValueT::Float),
            VecT::Double(vec) => vec.get(index).copied().map(DataValueT::Double),
        }
    }

    /// Inserts the Value to the corresponding Vector if the datatypes are compatible
    /// Note that it is not checked if the [DataValueT] has the right enum-variant
    pub(crate) fn push(&mut self, value: &DataValueT) {
        match self {
            VecT::U64(vec) => {
                vec.push(value.as_u64().expect(
                    "expecting VecT::U64 and DataValueT::U64, but DataValueT does not match",
                ))
            }
            VecT::Float(vec) => vec.push(value.as_float().expect(
                "expecting VecT::Float and DataValueT::Float, but DataValueT does not match",
            )),
            VecT::Double(vec) => vec.push(value.as_double().expect(
                "expecting VecT::Double and DataValueT::Double, but DataValueT does not match",
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
