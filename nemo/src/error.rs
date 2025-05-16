//! Error-handling module for the crate

use std::path::PathBuf;

use nemo_physical::datavalues::DataValueCreationError;
use thiserror::Error;

use crate::execution::{
    selection_strategy::strategy::SelectionStrategyError, tracing::error::TracingError,
};

pub use nemo_physical::error::ReadingError;

pub mod context;
pub mod report;
pub mod rich;

/// Error-Collection for all the possible Errors occurring in this crate
#[allow(variant_size_differences)]
#[derive(Error, Debug)]
pub enum Error {
    /// Build selection strategy errror
    #[error(transparent)]
    SelectionStrategyError(#[from] SelectionStrategyError),
    /// Error occurred during parsing
    #[error("error while parsing program")]
    ProgramParseError,
    /// Error occurred during tracing
    #[error(transparent)]
    TracingError(#[from] TracingError),
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
    /// Error related to the creation of data values
    #[error(transparent)]
    DataValueCreationError(#[from] DataValueCreationError),
}

impl From<ReadingError> for Error {
    fn from(value: ReadingError) -> Self {
        Self::PhysicalError(value.into())
    }
}
