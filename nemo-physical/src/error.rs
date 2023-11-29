//! Error-handling module for the crate

use std::{convert::Infallible, fmt::Display};

use thiserror::Error;

use crate::{datatypes::FloatIsNaN, resource::Resource};

/// Trait that can be used by external libraries extending Nemo to communicate a error during reading
pub trait ExternalReadingError: Display + std::fmt::Debug {}

/// Error-Collection for errors related to reading input tables.
#[allow(variant_size_differences)]
#[derive(Error, Debug)]
pub enum ReadingError {
    /// Error from trying to use a floating point number that is NaN
    #[error(transparent)]
    FloatIsNaN(#[from] FloatIsNaN),
    /// Error occurred during parsing of Int values
    #[error(transparent)]
    ParseInt(#[from] std::num::ParseIntError),
    /// Error occurred during parsing of Float values
    #[error(transparent)]
    ParseFloat(#[from] std::num::ParseFloatError),
    /// Errors on reading a file
    #[error("failed to read \"{filename}\": {error}")]
    IOReading {
        /// Contains the wrapped error
        error: std::io::Error,
        /// Filename which caused the error
        filename: String,
    },
    /// Error when a resource could not be provided by any resource provider
    #[error("resource at \"{resource}\" was not provided by any resource provider")]
    ResourceNotProvided {
        /// Resource which was not provided
        resource: Resource,
    },
    /// A provided resource is not a valid local file:// URI
    #[error(r#"resource "{0}" is not a valid local file:// URI"#)]
    InvalidFileUri(Resource),
    /// Error in Reqwest's HTTP handler
    #[error(transparent)]
    HttpTransfer(#[from] reqwest::Error),
    /// Type conversion error
    #[error("failed to convert value {0} to type {1}.")]
    TypeConversionError(String, String), // Note we cannot access logical types in physical layer
    /// Invalid RdfLiteral
    #[error("invalid Rdf Literal: {0}")]
    InvalidRdfLiteral(String), // Note we cannot access rdf literals in physical layer
    /// Reading error caused by a library which extends nemo
    #[error("reading error caused by a external library extending Nemo: {0}")]
    ExternalReadingError(Box<dyn ExternalReadingError>),
}

/// Error-Collection for all the possible Errors occurring in this crate
#[derive(Error, Debug)]
pub enum Error {
    /// Permutation shall be sorted, but the input data is of different length
    #[error("an invalid number of aggregated variables was provided: {0}")]
    InvalidAggregatedVariableCount(usize),
    /// Permutation shall be sorted, but the input data is of different length
    #[error("the provided data-structures do not have the same length: {0:?}")]
    PermutationSortLen(Vec<usize>),
    /// Permutation shall be applied to a too small amount of data
    #[error("permutation data length ({0}) is smaller than the sort_vec length ({1}) + the offset of {2}")]
    PermutationApplyWrongLen(usize, usize, usize),
    /// Error when giving invalid execution plan to the database instance
    #[error("the given execution plan is invalid.")]
    InvalidExecutionPlan,
    /// Error when converting integer type to floating point value
    #[error("usize value `{0}` could not be converted to floating point value")]
    UsizeToFloatingPointValue(usize),
    /// Error when converting integer type to floating point value
    #[error("u32 value `{0}` could not be converted to floating point value")]
    U32ToFloatingPointValue(u32),
    /// Error that happened while reading a Table
    #[error(transparent)]
    ReadingError(#[from] ReadingError),
    /// Error computing the memory requirements for a stack program
    #[error("the supplied stack program was malformed")]
    MalformedStackProgram,
}

impl From<Infallible> for ReadingError {
    fn from(_value: Infallible) -> Self {
        unreachable!("Infallible can never occur!")
    }
}
