use std::fmt::write;

use crate::error::Error;

pub trait DataWriter {
    /// Output a string
    fn write_string(&self, string: &str) -> Result<(), Error>;

    /// Output an integer
    /// TODO: Should cover a broader range of integer types than u64
    fn write_integer(&self, integer: u64) -> Result<(), Error>;
}

/// https://www.w3.org/TR/rdf11-concepts/#xsd-datatypes
#[derive(Debug, Copy, Clone)]
pub enum DataType {
    /// Character strings (but not all Unicode character strings)
    String,
    /// true, false
    Boolean,
    /// Arbitrary-precision decimal numbers
    Decimal,
    /// Arbitrary-size integer numbers
    Integer,
    /// 64-bit floating point numbers incl. ±Inf, ±0, NaN
    /// TODO: We do not allow NaN in our Double type
    Double,
    /// 32-bit floating point numbers incl. ±Inf, ±0, NaN
    /// TODO: We do not allow NaN in our Float type
    Float,
    /// 64 bit signed integer: -9223372036854775808…+9223372036854775807
    Long,
    /// 32 bit signed integer: -2147483648…+2147483647
    Int,
    // ...
}

pub trait DataValue {
    /// Return the [`DataType`] of this value
    fn data_type(&self) -> DataType;

    ///
    fn serialize<Writer: DataWriter>(&self, writer: &Writer) -> Result<(), Error>;

    /// Return a string representation of the value
    /// Returns `None` if the underlying value cannot be converted to a string.
    ///
    /// TODO: Should this only work on the String type?
    fn to_string(&self) -> Option<String>;

    /// Return the 64-bit unsigned integer that represents the value
    /// Returns `None` if the underlying value cannot be converted to a u64.
    fn to_u64(&self) -> Option<u64>;

    /// Return the 32-bit unsigned integer that represents the value
    /// Returns `None` if the underlying value cannot be converted to a u64.
    fn to_u32(&self) -> Option<u32>;

    // ...
}

/// String value obtianed from the physical layer
#[derive(Debug)]
pub struct PhysicalString<'a> {
    // TODO: Just guessing
    prefix: &'a str,
    content: &'a str,
}

impl<'a> DataValue for PhysicalString<'a> {
    fn data_type(&self) -> DataType {
        DataType::String
    }

    fn to_string(&self) -> Option<String> {
        let mut string = String::from(self.prefix);
        string.push_str(self.content);

        Some(string)
    }

    fn to_u64(&self) -> Option<u64> {
        None
    }

    fn to_u32(&self) -> Option<u32> {
        None
    }

    fn serialize<Writer: DataWriter>(&self, writer: &Writer) -> Result<(), Error> {
        writer.write_string(self.prefix)?;
        writer.write_string(self.content)
    }
}

/// Representation of the Long data type
#[derive(Debug, Clone, Copy)]
pub struct Long(u64);

impl DataValue for Long {
    fn data_type(&self) -> DataType {
        DataType::Long
    }

    fn to_string(&self) -> Option<String> {
        // TODO: Should this the value as a string instead?
        None
    }

    fn to_u64(&self) -> Option<u64> {
        Some(self.0)
    }

    fn to_u32(&self) -> Option<u32> {
        // TOOD: Or should the user always call `to_u64`and cast themselves?
        Some(self.0.try_into().ok()?)
    }

    fn serialize<Writer: DataWriter>(&self, writer: &Writer) -> Result<(), Error> {
        writer.write_integer(self.0)
    }
}
