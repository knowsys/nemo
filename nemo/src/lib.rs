//! An experimental project for rule reasoning.

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
#![allow(clippy::single_range_in_vec_init)]
#![feature(macro_metavar_expr)]
#![feature(is_sorted)]
#![feature(assert_matches)]
#![feature(iter_intersperse)]

/// The crate for underlying physical operations.
#[macro_use]
extern crate nemo_physical;

pub mod api;
pub mod error;
pub mod io;

mod builder_proxy;

pub mod execution;
pub mod model;
pub mod types;

mod program_analysis;
mod table_manager;
mod util;

pub use nemo_physical::meta;

// TODO: this is a temporary reexport, as long as the datatype mapping is not fully implemented
pub use nemo_physical::datatypes;
