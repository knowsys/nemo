//! Error-handling module for the crate

use thiserror::Error;

use crate::{
    io::parser::ParseError,
    logical::program_analysis::analysis::RuleAnalysisError,
    logical::types::{LogicalTypeEnum, TypeError},
    physical::datatypes::float_is_nan::FloatIsNaN,
};

/// Error-Collection for all the possible Errors occurring in this crate
#[allow(variant_size_differences)]
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
    /// Error which occurs when trying to Parse from an Int
    #[error(transparent)]
    FromInt(#[from] std::num::TryFromIntError),
    /// Error which implies a needed Rollback
    #[error("Rollback due to csv-error")]
    Rollback(usize),
    /// Permutation shall be sorted, but the input data is of different length
    #[error("The provided data-structures do not have the same length: {0:?}")]
    PermutationSortLen(Vec<usize>),
    /// Permutation shall be applied to a too small amount of data
    #[error("Permutation data length ({0}) is smaller than the sort_vec length ({1}) + the offset of {2}")]
    PermutationApplyWrongLen(usize, usize, usize),
    /// Error when converting integer type to floating point value
    #[error("Usize value `{0}` could not be converted to floating point value")]
    UsizeToFloatingPointValue(usize),
    /// Error when converting integer type to floating point value
    #[error("U32 value `{0}` could not be converted to floating point value")]
    U32ToFloatingPointValue(u32),
    /// Error when converting floating type to integer point value
    #[error("Floating type could not be converted to integer value")]
    FloatingPointToInteger,
    /// Rule analysis errors
    #[error(transparent)]
    RuleAnalysisError(#[from] RuleAnalysisError),
    /// Parse errors
    #[error(transparent)]
    ParseError(#[from] ParseError),
    /// Type errors
    #[error(transparent)]
    TypeError(#[from] TypeError),
    /// Error when giving invalid execution plan to the database instance
    #[error("The given execution plan is invalid.")]
    InvalidExecutionPlan,
    /// Parser could not parse whole Program-file, but should have read all of it.
    #[error("Parser could not parse the whole input file")]
    ProgramParse,
    /// IO Error
    #[error(transparent)]
    IO(#[from] std::io::Error),
    /// File exists and should not be overwritten
    #[error("File \"{filename}\" exists and would be overwritten!\nConsider using the `--overwrite-results` option, setting a different `--output` directory, or deleting \"{filename}\".")]
    IOExists {
        /// Contains the wrapped error
        error: std::io::Error,
        /// Filename which caused the error
        filename: String,
    },
    /// Errors on reading a file
    #[error("Failed to read \"{filename}\": {error}.")]
    IOReading {
        /// Contains the wrapped error
        error: std::io::Error,
        /// Filename which caused the error
        filename: String,
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
    /// Unknown logical type name in program.
    #[error("A predicate declaration used an unknown type ({0}). The known types are: {1:?}")]
    ParseUnknownType(String, Vec<LogicalTypeEnum>),
}
