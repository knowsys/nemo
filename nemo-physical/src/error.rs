//! Error-handling module for the crate

use std::path::PathBuf;

use thiserror::Error;

use crate::datatypes::FloatIsNaN;

/// Error-Collection for errors related to reading input tables.
/// Used in the [`TableReader`][crate::table_reader::TableReader] and
/// [`ColumnBuilderProxy`][crate::builder_proxy::ColumnBuilderProxy] interfaces.
#[allow(variant_size_differences)]
#[derive(Error, Debug)]
pub enum ReadingError {
    /// Float holds a NaN value
    #[error(transparent)]
    FloatIsNaN(#[from] FloatIsNaN),
    /// Error occurred during parsing of Int values
    #[error(transparent)]
    ParseInt(#[from] std::num::ParseIntError),
    /// Error occurred during parsing of Float values
    #[error(transparent)]
    ParseFloat(#[from] std::num::ParseFloatError),
    /// Error which occurs when trying to Parse from an Int
    #[error(transparent)]
    FromInt(#[from] std::num::TryFromIntError),
    /// Errors on reading a file
    #[error("Failed to read \"{filename}\": {error}.")]
    IOReading {
        /// Contains the wrapped error
        error: std::io::Error,
        /// Filename which caused the error
        filename: PathBuf,
    },
    /// IO Error
    // TODO: there is currently also an IO Error in nmo_logical. Do we want both?
    #[error(transparent)]
    IO(#[from] std::io::Error),
}

/// Error-Collection for all the possible Errors occurring in this crate
#[derive(Error, Debug)]
pub enum Error {
    /// Permutation shall be sorted, but the input data is of different length
    #[error("The provided data-structures do not have the same length: {0:?}")]
    PermutationSortLen(Vec<usize>),
    /// Permutation shall be applied to a too small amount of data
    #[error("Permutation data length ({0}) is smaller than the sort_vec length ({1}) + the offset of {2}")]
    PermutationApplyWrongLen(usize, usize, usize),
    /// Error when giving invalid execution plan to the database instance
    #[error("The given execution plan is invalid.")]
    InvalidExecutionPlan,
    /// Error when converting integer type to floating point value
    #[error("Usize value `{0}` could not be converted to floating point value")]
    UsizeToFloatingPointValue(usize),
    /// Error when converting integer type to floating point value
    #[error("U32 value `{0}` could not be converted to floating point value")]
    U32ToFloatingPointValue(u32),
    /// Error that happened while reading a Table
    #[error(transparent)]
    ReadingError(#[from] ReadingError),
}