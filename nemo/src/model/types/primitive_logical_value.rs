use nemo_physical::datatypes::data_value::DataValueIteratorT;
use nemo_physical::datatypes::Double;

use crate::model::RdfLiteral;

use super::XSD_STRING;

/// Enum for values in the logical layer
#[derive(Debug)]
pub enum PrimitiveLogicalValueT {
    /// Any variant
    Any(RdfLiteral),
    /// String variant
    String(String),
    /// Integer variant
    Integer(i64),
    /// Float64 variant
    Float64(Double),
}

impl From<RdfLiteral> for PrimitiveLogicalValueT {
    fn from(value: RdfLiteral) -> Self {
        Self::Any(value)
    }
}

impl From<String> for PrimitiveLogicalValueT {
    fn from(value: String) -> Self {
        Self::String(value)
    }
}

impl From<i64> for PrimitiveLogicalValueT {
    fn from(value: i64) -> Self {
        Self::Integer(value)
    }
}

impl From<Double> for PrimitiveLogicalValueT {
    fn from(value: Double) -> Self {
        Self::Float64(value)
    }
}

/// Iterator over one kind of possible logical values
#[allow(missing_debug_implementations)]
pub enum PrimitiveLogicalValueIteratorT<'a> {
    /// Any variant
    Any(DefaultAnyIterator<'a>),
    /// String variant
    String(DefaultStringIterator<'a>),
    /// Integer variant
    Integer(DefaultIntegerIterator<'a>),
    /// Float64 variant
    Float64(DefaultFloat64Iterator<'a>),
}

impl<'a> Iterator for PrimitiveLogicalValueIteratorT<'a> {
    type Item = PrimitiveLogicalValueT;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Any(iter) => Some(PrimitiveLogicalValueT::Any(iter.next()?)),
            Self::String(iter) => Some(PrimitiveLogicalValueT::String(iter.next()?)),
            Self::Integer(iter) => Some(PrimitiveLogicalValueT::Integer(iter.next()?)),
            Self::Float64(iter) => Some(PrimitiveLogicalValueT::Float64(iter.next()?)),
        }
    }
}

pub(super) type DefaultAnyIterator<'a> = Box<dyn Iterator<Item = RdfLiteral> + 'a>;
pub(super) type DefaultStringIterator<'a> = Box<dyn Iterator<Item = String> + 'a>;
pub(super) type DefaultIntegerIterator<'a> = Box<dyn Iterator<Item = i64> + 'a>;
pub(super) type DefaultFloat64Iterator<'a> = Box<dyn Iterator<Item = Double> + 'a>;

pub(super) struct AnyOutputMapper<'a> {
    physical_iter: Box<dyn Iterator<Item = String> + 'a>,
}

impl<'a> AnyOutputMapper<'a> {
    pub(super) fn new(phy: DataValueIteratorT<'a>) -> Self {
        match phy {
            DataValueIteratorT::String(physical_iter) => Self { physical_iter },
            _ => unreachable!("If the database representation of the logical types is correct, we never reach this branch.")
        }
    }
}

impl<'a> From<AnyOutputMapper<'a>> for DefaultAnyIterator<'a> {
    fn from(source: AnyOutputMapper<'a>) -> Self {
        Box::new(source.physical_iter.map(|s| {
            // TODO: do correct mapping here
            RdfLiteral::DatatypeValue {
                value: s,
                datatype: XSD_STRING.to_string(),
            }
        }))
    }
}

impl<'a> From<AnyOutputMapper<'a>> for DefaultStringIterator<'a> {
    fn from(source: AnyOutputMapper<'a>) -> Self {
        // TODO: do correct mapping here
        source.physical_iter
    }
}

pub(super) struct StringOutputMapper<'a> {
    physical_iter: Box<dyn Iterator<Item = String> + 'a>,
}

impl<'a> StringOutputMapper<'a> {
    pub(super) fn new(phy: DataValueIteratorT<'a>) -> Self {
        match phy {
            DataValueIteratorT::String(physical_iter) => Self { physical_iter },
            _ => unreachable!("If the database representation of the logical types is correct, we never reach this branch.")
        }
    }
}

impl<'a> From<StringOutputMapper<'a>> for DefaultStringIterator<'a> {
    fn from(source: StringOutputMapper<'a>) -> Self {
        Box::new(source.physical_iter.map(|s| {
            s.get(1..(s.len() - 1))
                .expect("The physical string is wrapped in quotes.")
                .to_string()
        }))
    }
}

pub(super) struct IntegerOutputMapper<'a> {
    physical_iter: Box<dyn Iterator<Item = i64> + 'a>,
}

impl<'a> IntegerOutputMapper<'a> {
    pub(super) fn new(phy: DataValueIteratorT<'a>) -> Self {
        match phy {
            DataValueIteratorT::I64(physical_iter) => Self { physical_iter },
            _ => unreachable!("If the database representation of the logical types is correct, we never reach this branch.")
        }
    }
}

impl<'a> From<IntegerOutputMapper<'a>> for DefaultIntegerIterator<'a> {
    fn from(source: IntegerOutputMapper<'a>) -> Self {
        source.physical_iter
    }
}

impl<'a> From<IntegerOutputMapper<'a>> for DefaultStringIterator<'a> {
    fn from(source: IntegerOutputMapper<'a>) -> Self {
        Box::new(source.physical_iter.map(|i| i.to_string()))
    }
}

pub(super) struct Float64OutputMapper<'a> {
    physical_iter: Box<dyn Iterator<Item = Double> + 'a>,
}

impl<'a> Float64OutputMapper<'a> {
    pub(super) fn new(phy: DataValueIteratorT<'a>) -> Self {
        match phy {
            DataValueIteratorT::Double(physical_iter) => Self { physical_iter },
            _ => unreachable!("If the database representation of the logical types is correct, we never reach this branch.")
        }
    }
}

impl<'a> From<Float64OutputMapper<'a>> for DefaultFloat64Iterator<'a> {
    fn from(source: Float64OutputMapper<'a>) -> Self {
        source.physical_iter
    }
}

impl<'a> From<Float64OutputMapper<'a>> for DefaultStringIterator<'a> {
    fn from(source: Float64OutputMapper<'a>) -> Self {
        Box::new(source.physical_iter.map(|d| d.to_string()))
    }
}
