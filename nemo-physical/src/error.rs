//! Error-handling module for the crate

use std::{convert::Infallible, fmt::Display};

use thiserror::Error;

use crate::{datatypes::FloatIsNaN, table_reader::Resource};

/// Trait that can be used by external libraries extending Nemo to communicate a error during reading
pub trait ExternalReadingError: Display + std::fmt::Debug {}

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
        filename: String,
    },
    /// Error when a resource could not be provided by any resource provider
    #[error("Resource at \"{resource}\" was not provided by any resource provider")]
    ResourceNotProvided {
        /// Resource which was not provided
        resource: Resource,
    },
    /// A provided resource is not a valid local file:// URI
    #[error(r#"Resource "{0}" is not a valid local file:// URI"#)]
    InvalidFileUri(Resource),
    /// Error in Rio's Turtle parser
    #[error(transparent)]
    RioTurtle(#[from] rio_turtle::TurtleError),
    /// Error in Rio's RDF/XML parser
    #[error(transparent)]
    RioXML(#[from] rio_xml::RdfXmlError),
    /// Error in Requwest's HTTP handler
    #[error(transparent)]
    HTTPTransfer(#[from] reqwest::Error),
    /// Error when converting from Rio since we do not support RDF Star
    #[error("Failed to convert nested RDF triple. We do not support RDF Star.")]
    RdfStarUnsupported,
    /// Type conversion error
    #[error("Failed to convert value {0} to type {1}.")]
    TypeConversionError(String, String), // Note we cannot access logical types in physical layer
    /// Invalid RdfLiteral
    #[error("Invalid Rdf Literal: {0}")]
    InvalidRdfLiteral(String), // Note we cannot access rdf literals in physical layer
    /// Reading error caused by a library which extends nemo
    #[error("Reading error caused by a external library extending Nemo: {0}")]
    ExternalReadingError(Box<dyn ExternalReadingError>),
    /// Unable to determine RDF format.
    #[error("Could not determine which RDF parser to use for resource {0}")]
    UnknownRDFFormatVariant(Resource),
    /// Could not read the dsv headers
    #[error("cannot read headers in csv/dsv file")]
    DSVMissingHeaders,
    /// Missing column in dsv file
    #[error("missing column {0} in csv/dsv file")]
    DSVMissingColumn(String),
    /// Mismatch between import specification and output type
    #[error("import specification does not match predicate arity")]
    ImportArityMismatch,
}

/// Error-Collection for all the possible Errors occurring in this crate
#[derive(Error, Debug)]
pub enum Error {
    /// Permutation shall be sorted, but the input data is of different length
    #[error("An invalid number of aggregated variables was provided: {0}")]
    InvalidAggregatedVariableCount(usize),
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
    /// Error computing the memory requirements for a stack program
    #[error("The supplied stack program was malformed")]
    MalformedStackProgram,
}

impl From<Infallible> for ReadingError {
    fn from(_value: Infallible) -> Self {
        unreachable!("Infallible can never occur!")
    }
}
