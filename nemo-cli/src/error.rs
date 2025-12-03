//! This module defines all the errors that can occur while executing nemo-cli.

use thiserror::Error;

/// Error that occur during execution of Nemo's CLI app
#[derive(Error, Debug)]
pub enum CliError {
    /// Error if no input rule files are specified
    #[error("no input file was given")]
    NoInput,
    /// Error if the user asked for an unimplemented feature
    #[error("multiple rule files are currently unsupported")]
    MultipleFilesNotImplemented,
    /// Error while serializing data to a file
    #[error("Error while serializing data to {filename}.")]
    SerializationError {
        /// Name of the file where data could not have been serialized into
        filename: String,
    },
    /// Error while parsing fact for tracing
    #[error("unable to parse fact: {fact}")]
    TracingInvalidFact {
        /// Incorrectly formatted fact
        fact: String,
    },
    /// Error while parsing tracing input file
    #[error("unable to parse tracing input: {error}")]
    TracingInvalidJsonInput {
        /// Error from json parsing
        error: String,
    },
    /// Invalid paramater
    #[error("invalid parameter: {parameter}")]
    InvalidParameter {
        /// Invalid paramater
        parameter: String,
    },
    /// Error while parsing a rule file
    #[error("unable to parse program `{filename}`")]
    ProgramParsing {
        /// Filename of the rule file
        filename: String,
    },
    /// Error resulting from io operations
    #[error(transparent)]
    IoError(#[from] std::io::Error),
    /// Error originating from nemo
    #[error(transparent)]
    NemoError(#[from] nemo::error::Error),
}
