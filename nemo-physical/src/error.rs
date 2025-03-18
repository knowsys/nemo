//! Error-handling module for the crate

use std::{convert::Infallible, fmt::Display};

use thiserror::Error;

use crate::{datavalues::DataValueCreationError, resource::Resource};

/// Trait that can be used by external libraries extending Nemo to communicate a error during reading
pub trait ExternalReadingError: Display + std::fmt::Debug {}

/// The collection of possible causes for a [`ReadingError`]
#[allow(variant_size_differences)]
#[derive(Error, Debug)]
pub enum ReadingErrorKind {
    /// An external error during reading.
    #[error(transparent)]
    ExternalError(#[from] Box<dyn std::error::Error>),
    /// Error from trying to use a floating point number that is NaN
    #[error("floating point values must be finite and not NaN")]
    InvalidFloat,
    /// Error occurred during parsing of Int values
    #[error(transparent)]
    ParseInt(#[from] std::num::ParseIntError),
    /// Error occurred during parsing of Float values
    #[error(transparent)]
    ParseFloat(#[from] std::num::ParseFloatError),
    /// Errors on reading a file
    #[error("failed to read file: {0}")]
    IoReading(#[from] std::io::Error),
    /// Decompression error
    #[error("failed to decompress with {decompression_format}")]
    Decompression {
        /// name of decompression method that was used
        decompression_format: String,
    },
    /// Error when a resource could not be provided by any resource provider
    #[error("resource was not provided by any resource provider")]
    ResourceNotProvided,
    /// Error in Reqwest's HTTP handler
    #[error(transparent)]
    HttpTransfer(#[from] reqwest::Error),
    /// Error when HTTP response has not the expected content type
    #[error("HTTP response failed")]
    HttpWrongContentType,
    /// Type conversion error
    #[error("failed to convert value {0} to type {1}.")]
    TypeConversionError(String, String), // Note we cannot access logical types in physical layer
    /// Invalid RdfLiteral
    #[error("invalid RDF Literal: {0}")]
    InvalidRdfLiteral(String), // Note we cannot access rdf literals in physical layer
    /// Reading error caused by a library which extends nemo
    #[error("reading error caused by a external library extending Nemo: {0}")]
    ExternalReadingError(Box<dyn ExternalReadingError>),
    /// Error during creation of data-values
    #[error(transparent)]
    DataValueCreation(#[from] DataValueCreationError),
}

/// An error that occurred while reading input tables
#[derive(Debug, Error)]
pub struct ReadingError {
    kind: ReadingErrorKind,
    resource: Option<Box<Resource>>,
    predicate: Option<String>,
}

impl ReadingError {
    /// Creates a new [`ReadingError`]
    pub fn new(kind: ReadingErrorKind) -> Self {
        ReadingError {
            kind,
            resource: None,
            predicate: None,
        }
    }

    /// Create a new [`ReadingError`] of kind [`ReadingErrorKind::ExternalError`]
    pub fn new_external(error: Box<dyn std::error::Error>) -> Self {
        Self::new(error.into())
    }

    /// Set the resource which was being read while the error occurred
    pub fn with_resource(mut self, resource: Resource) -> Self {
        self.resource = Some(Box::new(resource));
        self
    }

    /// Set the predicate which was being processed while the error occurred
    pub fn with_predicate(mut self, predicate: String) -> Self {
        self.predicate = Some(predicate);
        self
    }
}

impl<T> From<T> for ReadingError
where
    ReadingErrorKind: From<T>,
{
    fn from(value: T) -> Self {
        ReadingError::new(ReadingErrorKind::from(value))
    }
}

impl Display for ReadingError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Display::fmt(&self.kind, f)?;

        if let Some(resource) = &self.resource {
            f.write_fmt(format_args!("\nwhile reading at `{resource}`"))?
        }

        if let Some(predicate) = &self.predicate {
            f.write_fmt(format_args!("\nwhile processing table for `{predicate}`"))?
        }

        Ok(())
    }
}

/// Error-Collection for all the possible Errors occurring in this crate
#[derive(Error, Debug)]
pub enum Error {
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
