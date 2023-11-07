//! This module provides traits and implementations for structures that represent data values.
//! Data values are conceived on this level as canonical representations of unique (semantic)
//! values across a number of domains (integers, strings, etc.).

/// Module to define the general [DataValue] trait.
pub mod datavalue;
pub use datavalue::DataValue;
pub use datavalue::ValueDomain;
/// Module to define a [DataValue] implementation for values that can be represented as an i64.
pub mod long_datavalue;
pub use long_datavalue::Long;

