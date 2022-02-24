use super::double::Double;
use super::float::Float;

/// Enum for values of all supported basic types.
/// This should not be used to represent large numbers of values,
/// due to the overhead for each value, but it can be a convenient
/// option to interface with unknown values.
#[derive(Clone, Copy, Debug)]
pub enum DataValueT {
    /// Case u64
    DataValueU64(u64),
    /// Case Float
    DataValueFloat(Float),
    /// Case Double
    DataValuDouble(Double),
}

impl DataValueT {
    /// Returns either `Option<u64>` or `None`
    pub fn as_u64(&self) -> Option<u64> {
        match *self {
            DataValueT::DataValueU64(val) => Some(val),
            _ => None,
        }
    }

    /// Returns either `Option<Float>` or `None`
    pub fn as_float(&self) -> Option<Float> {
        match *self {
            DataValueT::DataValueFloat(val) => Some(val),
            _ => None,
        }
    }

    /// Returns either `Option<Double>` or `None`
    pub fn as_double(&self) -> Option<Double> {
        match *self {
            DataValueT::DataValuDouble(val) => Some(val),
            _ => None,
        }
    }
}
