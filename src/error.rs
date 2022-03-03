//! Error-handling module for the crate

use crate::physical::datatypes::float_is_nan::FloatIsNaN;
use thiserror::Error;

/// Error-Collection for all the possible Errors occurring in this crate
#[derive(Error, Debug)]
pub enum Error {
    /// Float holds a NaN value
    #[error(transparent)]
    FloatIsNaN(#[from] FloatIsNaN),
    /// Error occurred during parsing of Int values
    #[error(transparent)]
    ParseInt(#[from] std::num::ParseIntError),
    /// Error occurred during parsing of Float values
    #[error(transparent)]
    ParseFloat(#[from] std::num::ParseFloatError),
    /// Error which implies a needed Rollback
    #[error("Rollback due to csv-error")]
    RollBack(usize),
}
