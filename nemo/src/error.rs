//! Error-handling module for the crate

use std::path::PathBuf;

use nemo_physical::datavalues::DataValueCreationError;
use thiserror::Error;

use crate::{
    execution::selection_strategy::strategy::SelectionStrategyError,
    io::{formats::import_export::ImportExportError, parser::LocatedParseError},
    program_analysis::analysis::RuleAnalysisError,
};

pub use nemo_physical::error::ReadingError;

/// Error-Collection for all the possible Errors occurring in this crate
#[allow(variant_size_differences)]
#[derive(Error, Debug)]
pub enum Error {
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
    /// IO Error
    #[error(transparent)]
    IO(#[from] std::io::Error),
    /// File exists and should not be overwritten
    #[error(r#"File "{path}" exists and would be overwritten!\nConsider using the `--overwrite-results` option, setting a different `--output` directory, or deleting "{path}"."#)]
    IOExists {
        /// Contains the wrapped error
        error: std::io::Error,
        /// Filename which caused the error
        path: PathBuf,
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
    UnknownUnaryOpertation {
        /// The operation causing the failure
        operation: String,
    },
    /// Error while serializing data to a file
    #[error("Error while serializing data to {filename}.")]
    SerializationError {
        /// Name of the file where data could not have been serialized into
        filename: String,
    },
    /// Error related to handling of file formats
    #[error(transparent)]
    FileFormatError(#[from] ImportExportError),
    /// Error related to the creation of data values
    #[error(transparent)]
    DataValueCreationError(#[from] DataValueCreationError),
}

impl From<ReadingError> for Error {
    fn from(value: ReadingError) -> Self {
        Self::PhysicalError(value.into())
    }
}
