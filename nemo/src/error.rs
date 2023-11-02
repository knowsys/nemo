//! Error-handling module for the crate

use std::path::PathBuf;

use thiserror::Error;

use crate::{
    execution::selection_strategy::strategy::SelectionStrategyError, io::parser::LocatedParseError,
    model::types::error::TypeError, program_analysis::analysis::RuleAnalysisError,
};

pub use nemo_physical::error::ReadingError;

/// Error-Collection for all the possible Errors occurring in this crate
#[allow(variant_size_differences)]
#[derive(Error, Debug)]
pub enum Error {
    /// Currently tracing doesn't work for all language features
    #[error(
        "Tracing is currently not supported for some rules with arithmetic operations in the head."
    )]
    TraceUnsupportedFeature(),
    /// Error which implies a needed Rollback
    #[error("Rollback due to csv-error")]
    Rollback(usize),
    /// Build selection strategy errror
    #[error(transparent)]
    SelectionStrategyError(#[from] SelectionStrategyError),
    /// Error when converting floating type to integer point value
    #[error("Floating type could not be converted to integer value")]
    FloatingPointToInteger,
    /// Error if no input rule files are specified
    #[error("No inputs were specified")]
    NoInput,
    /// Error if the user asked for an unimplemented feature
    #[error("Multiple file support is not yet implemented")]
    MultipleFilesNotImplemented,
    /// Rule analysis errors
    #[error(transparent)]
    RuleAnalysisError(#[from] RuleAnalysisError),
    /// Parse errors
    #[error(transparent)]
    ParseError(#[from] LocatedParseError),
    /// Type errors
    #[error(transparent)]
    TypeError(#[from] TypeError),
    /// IO Error
    #[error(transparent)]
    IO(#[from] std::io::Error),
    /// File exists and should not be overwritten
    #[error("File \"{filename}\" exists and would be overwritten!\nConsider using the `--overwrite-results` option, setting a different `--output` directory, or deleting \"{filename}\".")]
    IOExists {
        /// Contains the wrapped error
        error: std::io::Error,
        /// Filename which caused the error
        filename: PathBuf,
    },
    /// Error during a Write operation
    #[error("Failed to write \"{filename}\": {error}")]
    IOWriting {
        /// Underlying IO error
        error: std::io::Error,
        /// Name of the file that could not be written
        filename: String,
    },
    /// CSV serialization/deserialization error
    #[error(transparent)]
    CsvError(#[from] csv::Error),
    /// Error in the physical layer
    #[error(transparent)]
    PhysicalError(#[from] nemo_physical::error::Error),
    /// Error when trying to lookup unary operations
    #[error("The unary operation {operation} is unknown.")]
    UnknonwUnaryOpertation {
        /// The operation causing the failure
        operation: String,
    },
}

impl From<ReadingError> for Error {
    fn from(value: ReadingError) -> Self {
        Self::PhysicalError(value.into())
    }
}
