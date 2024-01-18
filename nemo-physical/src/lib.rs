//! This crate defines low-level data structures and operations, i.e.,
//! it corresponds to the physical layer of a data processing system.
//! It uses simplified structures and primitive datatypes that may
//! not correspond to the view at a higher (logical) level.

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
    variant_size_differences,
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap
)]
#![feature(macro_metavar_expr)]
#![feature(is_sorted)]
#![feature(iter_intersperse)]

pub mod aggregates;
pub mod columnar;
pub mod datasources;
pub mod datatypes;
pub mod datavalues;
pub mod dictionary;
pub mod error;
pub mod function;
pub mod management;
pub mod meta;
pub mod permutator;
pub mod resource;
pub mod tabular;
pub mod util;
