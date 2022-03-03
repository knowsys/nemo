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
        match self {
            DataTypeName::U64 => match string.parse::<u64>() {
                Ok(val) => Ok(DataValueT::U64(val)),
                Err(err) => Err(Error::Parser(Box::new(err))),
            },
            DataTypeName::Float => match string.parse::<f32>() {
                Ok(val) => Ok(DataValueT::Float(super::Float::new(val)?)),
                Err(err) => Err(Error::Parser(Box::new(err))),
            },
            DataTypeName::Double => match string.parse::<f64>() {
                Ok(val) => Ok(DataValueT::Double(super::Double::new(val)?)),
                Err(err) => Err(Error::Parser(Box::new(err))),
            },
        }
    }
}
