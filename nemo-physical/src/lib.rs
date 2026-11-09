//! The database layer of the Nemo rule engine.
//!
//! This crate implements the in-memory database and operations on it.
//!
//! # Main components
//!
//! Values:
//! - [value] defines the data values and the operations on them.
//! - [storage] defines the storage values, the form in which tables store values in memory.
//! - [dictionary] stores the values that a storage value cannot encode in its payload.
//!
//! Tables and operations:
//! - [trie] is the immutable columnar table structure.
//! - [management] stores the tables, with their rows in the column orders that reads ask for.
//! - [plan] describes computations such as joins or filters as operation plans,
//!   and executes them.
//! - [function] evaluates builtin functions as stack programs.
//!
//! Infrastructure:
//! - [parallel] runs the parallel phases of operations on a worker pool.
//! - [tuning] defines the thresholds that decide how operations run in parallel.
//! - [diagnostics] provides data structures for errors and warnings.
//! - [stopwatch] provides the clock that measures execution times.
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
