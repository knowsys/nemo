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
#![feature(macro_metavar_expr)]
#![feature(assert_matches)]
#![feature(iter_intersperse)]
#![feature(str_from_raw_parts)]
#![feature(path_add_extension)]
#![feature(associated_type_defaults)]

/// The crate for underlying physical operations.
pub extern crate nemo_physical;

pub mod api;
pub mod error;
pub mod io;
pub mod parser;
pub mod syntax;

pub mod execution;
pub mod rule_model;
pub mod util;

pub mod chase_model; // TODO: Make private
pub(crate) mod table_manager;

// we use datavalues and meta from nemo_physical in our API, so re-export it here.
pub use nemo_physical::datavalues;
pub use nemo_physical::meta;
