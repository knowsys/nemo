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
#![feature(macro_metavar_expr)]
#![feature(is_sorted)]
#![feature(iter_intersperse)]

pub mod error;
pub mod io;
pub mod logical;
pub mod meta;
pub mod physical;
