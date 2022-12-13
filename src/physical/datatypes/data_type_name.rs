use std::cmp::Ordering;

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

/// Represents which data types can be implicitly cast into another.
/// E.g. U32 <= U64.
impl PartialOrd for DataTypeName {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match self {
            DataTypeName::U32 => match other {
                DataTypeName::U32 => Some(Ordering::Equal),
                DataTypeName::U64 => Some(Ordering::Less),
                DataTypeName::Float => None,
                DataTypeName::Double => None,
            },
            DataTypeName::U64 => match other {
                DataTypeName::U32 => Some(Ordering::Greater),
                DataTypeName::U64 => Some(Ordering::Equal),
                DataTypeName::Float => None,
                DataTypeName::Double => None,
            },
            DataTypeName::Float => match other {
                DataTypeName::Float => Some(Ordering::Equal),
                _ => None,
            },
            DataTypeName::Double => match other {
                DataTypeName::Double => Some(Ordering::Equal),
                _ => None,
            },
        }
    }
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
}
