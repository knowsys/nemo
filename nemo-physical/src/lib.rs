//! The database layer of the Nemo rule engine.
//!
//! This crate implements the in-memory database and operations on it.
//!
//! # Main components
//!
//! Values:
//! - [value] defines the semantic data values.
//! - [storage] defines the physical storage of values in memory.
//! - [dictionary] handles values that do not fit inline.
//!
//! Tables and operations:
//! - [trie] is the immutable columnar table structure.
//! - [management] organizes related tables and their orders.
//! - [plan] describes computations such as joins or filters as operation plans,
//!   and executes them.
//! - [function] compiles builtin expressions into stack programs.
//!
//! Infrastructure:
//! - [parallel] contains functions for the parallel execution of operations.
//! - [tuning] contains constants for performance tuning.
//! - [diagnostics] provides data structures for error reporting.
//! - [stopwatch] provides the clock execution timings are measured with.
//!
//! The primary public entry point is [TableManager](management::TableManager),
//! which owns and maintains the in-memory tables.
//! Computations over them are built as an [OperationPlan](plan::OperationPlan)
//! and executed against it.

pub mod diagnostics;
pub mod dictionary;
mod execution;
pub mod function;
pub mod management;
pub mod parallel;
pub mod plan;
pub mod stopwatch;
pub mod storage;
pub mod trie;
pub mod tuning;
pub mod value;

#[cfg(test)]
mod testing;
