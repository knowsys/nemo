use crate::error::Error;

use super::DataValueT;

/// Descriptors to refer to the possible data types at runtime.
#[derive(Clone, Copy, Debug, Ord, PartialOrd, Eq, PartialEq)]
pub enum DataTypeName {
    /// Data type [`u64`].
    U64,
    /// Data type [`super::float::Float`]
    Float,
    /// Data type [`super::double::Double`]
    Double,
}

impl DataTypeName {
    /// Parses a string, based on the name of the Datatype
    pub fn parse(&self, string: &str) -> Result<DataValueT, Error> {
        Ok(match self {
            DataTypeName::U64 => DataValueT::U64(string.parse::<u64>()?),
            DataTypeName::Float => DataValueT::Float(super::Float::new(string.parse::<f32>()?)?),
            DataTypeName::Double => DataValueT::Double(super::Double::new(string.parse::<f64>()?)?),
        })
    }
}
