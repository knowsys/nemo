//! A fast in-memory rule engine

#![type_length_limit = "5000000000"]
#![deny(
    missing_debug_implementations,
    missing_copy_implementations,
    trivial_casts,
    trivial_numeric_casts
)]
#![warn(
    missing_docs,
    unused_import_braces,
    unused_qualifications,
    unused_extern_crates,
    variant_size_differences
)]
#![feature(iter_intersperse)]
#![feature(iterator_try_reduce)]
#![feature(str_from_raw_parts)]
#![feature(associated_type_defaults)]

/// The crate for underlying physical operations.
pub extern crate nemo_physical;

pub mod api;
pub mod error;
pub mod execution;
pub mod io;
pub mod parser;
pub mod rule_file;
pub mod rule_model;
pub mod syntax;
pub mod util;

pub(crate) mod table_manager;

// we use datavalues and meta from nemo_physical in our API, so re-export it here.
pub use nemo_physical::datavalues;
pub use nemo_physical::meta;

/// Lock for tests that execute programs.
///
/// Executing a program records timings in the global [meta::TimedCode],
/// which does not support measuring the same block on several threads at once,
/// so such tests must not run in parallel.
#[cfg(test)]
pub(crate) static EXECUTION_TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());
