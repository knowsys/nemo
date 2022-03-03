use super::double::Double;
use super::float::Float;

/// Enum for values of all supported basic types.
/// This should not be used to represent large numbers of values,
/// due to the overhead for each value, but it can be a convenient
/// option to interface with unknown values.
#[derive(Clone, Copy, Debug)]
pub enum DataValueT {
    /// Case u64
    U64(u64),
    /// Case Float
    Float(Float),
    /// Case Double
    Double(Double),
}

impl DataValueT {
    /// Returns either `Option<u64>` or `None`
    pub fn as_u64(&self) -> Option<u64> {
        match *self {
            DataValueT::U64(val) => Some(val),
            _ => None,
        }
    }

    /// Returns either `Option<Float>` or `None`
    pub fn as_float(&self) -> Option<Float> {
        match *self {
            DataValueT::Float(val) => Some(val),
            _ => None,
        }
    }

    /// Returns either `Option<Double>` or `None`
    pub fn as_double(&self) -> Option<Double> {
        match *self {
            DataValueT::Double(val) => Some(val),
            _ => None,
        }
    }
}

/// Enum for vectors of different supported input types
#[derive(Debug)]
pub enum VecT {
    /// Case Vec<u64>
    VecU64(Vec<u64>),
    /// Case Vec<Float>
    VecFloat(Vec<Float>),
    /// Case Vec<Double>
    VecDouble(Vec<Double>),
}

impl VecT {
    /// Removes the last element in the corresponding vector
    pub fn pop(&mut self) {
        match self {
            VecT::VecU64(v) => {
                v.pop();
            }
            VecT::VecFloat(v) => {
                v.pop();
            }
            VecT::VecDouble(v) => {
                v.pop();
            }
        }
    }

    /// Inserts the Value to the corresponding Vector if the datatypes are compatible
    /// Note that it is not checked if the [DataValueT] has the right enum-variant
    pub(crate) fn push(&mut self, value: &DataValueT) {
        match self {
            VecT::VecU64(vec) => vec.push(value.as_u64().unwrap()),
            VecT::VecFloat(vec) => vec.push(value.as_float().unwrap()),
            VecT::VecDouble(vec) => vec.push(value.as_double().unwrap()),
        };
    }

    /// Returns the lengths of the contained Vector
    pub fn len(&self) -> usize {
        match self {
            VecT::VecU64(vec) => vec.len(),
            VecT::VecFloat(vec) => vec.len(),
            VecT::VecDouble(vec) => vec.len(),
        }
    }

    /// Returns whether the vector is empty, or not
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}
