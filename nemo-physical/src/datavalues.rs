//! This module provides traits and implementations for structures that represent data values.
//! Data values are conceived on this level as canonical representations of unique (semantic)
//! values across a number of domains (integers, strings, etc.).

/// Module to define the general [DataValue] trait.
pub mod datavalue;
pub use datavalue::DataValue;
pub use datavalue::ValueDomain;
/// Module to define [DataValue] implementations for values that can be represented as integers.
pub mod integer_datavalues;
pub use integer_datavalues::Long;
pub use integer_datavalues::UnsignedLong;
/// Module to define [DataValue] implementations for values that can be represented as floating point numbers.
pub mod float_datavalues;
pub use float_datavalues::Double;
