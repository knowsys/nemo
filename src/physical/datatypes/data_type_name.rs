use std::fmt::Display;

use crate::{error::Error, physical::dictionary::Dictionary};

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
            DataTypeName::Float => DataValueT::Float(super::Float::new(string.parse::<f32>()?)?),
            DataTypeName::Double => DataValueT::Double(super::Double::new(string.parse::<f64>()?)?),
        })
    }

    /// Puts a string into a Dictionary and returns the index in the according Datatype
    pub fn parse_dict<Dict: Dictionary>(
        &self,
        dictionary: &mut Dict,
        string: &str,
    ) -> Result<DataValueT, Error> {
        Ok(match self {
            DataTypeName::U32 => DataValueT::U32(dictionary.add(string.to_string()).try_into()?),
            DataTypeName::U64 => DataValueT::U64(dictionary.add(string.to_string()).try_into()?),
            DataTypeName::Float => {
                unimplemented!("Float indices for dictionaries do not make sense")
            }
            DataTypeName::Double => {
                unimplemented!("Float indices for dictionaries do not make sense")
            }
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
