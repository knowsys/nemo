//! This module provides traits and implementations for structures that represent data values.
//! Data values are conceived on this level as canonical representations of unique (semantic)
//! values across a number of domains (integers, strings, etc.).

/// Module to define the general [DataValue] trait.
pub mod datavalue;
pub use datavalue::DataValue;
pub use datavalue::ValueDomain;
/// Module to define errors that may occur when working with [DataValue]s
pub mod errors;
pub use errors::DataValueCreationError;
/// Module to define [DataValue] implementations for values that can be represented as integers.
pub mod integer_datavalues;
pub use integer_datavalues::LongDataValue;
pub use integer_datavalues::UnsignedLongDataValue;
/// Module to define [DataValue] implementations for values that can be represented as floating point numbers.
pub mod float_datavalues;
pub use float_datavalues::DoubleDataValue;
pub use float_datavalues::FloatDataValue;
/// Module to define [DataValue] implementations for values that can be represented as Unicode strings.
pub mod string_datavalue;
pub use string_datavalue::StringDataValue;
/// Module to define [DataValue] implementations for values that correspond to IRIs.
pub mod iri_datavalue;
pub use iri_datavalue::IriDataValue;
/// Module to define [DataValue] implementations for values that correspond to language-tagged Unicode strings.
pub mod lang_string_datavalue;
pub use lang_string_datavalue::LangStringDataValue;
/// Module to define [DataValue] implementations for generic values that are not covered by any of our specific [`ValueDomain`]s.
pub mod other_datavalue;
pub use other_datavalue::OtherDataValue;
/// Module to define [DataValue] implementations for null values.
pub mod null_datavalue;
pub use null_datavalue::NullDataValue;
/// Module to define [DataValue] implementation for boolean values.
pub mod boolean_datavalue;
pub use boolean_datavalue::BooleanDataValue;
/// Module to define [DataValue] implementation for tuple values.
pub mod tuple_datavalue;
pub use tuple_datavalue::TupleDataValue;
/// Module to define [DataValue] implementations for arbitrary values.
pub mod any_datavalue;
pub use any_datavalue::AnyDataValue;
pub use any_datavalue::AnyDataValueIterator;
