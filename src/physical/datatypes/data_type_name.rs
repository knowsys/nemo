use std::fmt::Display;

use crate::error::Error;

use super::DataValueT;

/// Descriptors to refer to the possible data types at runtime.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DataTypeName {
    /// Data type [`u32`].
    U32,
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
            DataTypeName::U32 => DataValueT::U32(string.parse::<u32>()?),
            DataTypeName::U64 => DataValueT::U64(string.parse::<u64>()?),
            DataTypeName::Float => DataValueT::Float(string.parse::<super::Float>()?),
            DataTypeName::Double => DataValueT::Double(string.parse::<super::Double>()?),
        })
    }
}

impl Display for DataTypeName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DataTypeName::U32 => write!(f, "U32"),
            DataTypeName::U64 => write!(f, "U64"),
            DataTypeName::Float => write!(f, "Float"),
            DataTypeName::Double => write!(f, "Double"),
        }
    }
}

/// Types implementing this trait have a function to get their corresponding DataTypeName enum Variant
pub trait HasDataTypeName {
    /// Get the DataTypeName of Self
    fn data_type_name() -> DataTypeName;
}

macro_rules! has_data_type_name {
    ($type:ty, $variant:ident) => {
        impl HasDataTypeName for $type {
            fn data_type_name() -> DataTypeName {
                DataTypeName::$variant
            }
        }
    };
}

has_data_type_name!(u32, U32);
has_data_type_name!(u64, U64);
has_data_type_name!(super::Float, Float);
has_data_type_name!(super::Double, Double);
