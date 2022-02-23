//! Error-handling module for the crate

use crate::physical::datatypes::float_is_nan::FloatIsNaN;
use thiserror::Error;

/// Error-Collection for all the possible Errors occurring in this crate
#[derive(Error, Debug)]
pub enum Error {
    /// Float holds a NaN value
    #[error("The floating point types used in this library do not support NaN!")]
    FloatIsNaN(#[from] FloatIsNaN),
    /// Error occurred during parsing
    #[error("Parsing failed")]
    Parser(#[from] Box<dyn std::error::Error>),
}
