use std::path::PathBuf;

use nemo_physical::datavalues::AnyDataValue;
use thiserror::Error;

use crate::rule_model::components::import_export::file_formats::FileFormat;

/// Errors related to the creation and usage of [ImportExportHandler]s.
#[derive(Debug, Error)]
pub enum ImportExportError {
    /// Format is not supported for reading.
    #[error(r#"Format "{0}" cannot be read"#)]
    UnsupportedRead(FileFormat),
    /// Format is not supported for writing.
    #[error(r#"Format "{0}" cannot be written"#)]
    UnsupportedWrite(FileFormat),
    /// A required attribute is missing.
    #[error(r#"Missing required attribute "{0}""#)]
    MissingAttribute(String),
    /// A given attribute is not valid for the format.
    #[error(r#"Unknown attribute "{0}""#)]
    UnknownAttribute(String),
    /// File format name is not known.
    #[error(r#"Unknown file format "{0}""#)]
    UnknownFileFormat(String),
    /// Attribute value is invalid.
    #[error(r#"Invalid attribute value "{value}" for attribute "{attribute}": {description}"#)]
    InvalidAttributeValue {
        /// The given value.
        value: AnyDataValue,
        /// The attribute the value was given for.
        attribute: AnyDataValue,
        /// A description of why the value was invalid.
        description: String,
    },
    /// Value format is unsupported for this format.
    #[error(r#"Unsupported value format "{value_format}" for format {format}"#)]
    InvalidValueFormat {
        /// The given value format.
        value_format: String,
        /// The file format.
        format: FileFormat,
    },
    /// Arity is unsupported for this format.
    #[error(r#"import produces tuples of arity {arity}, but it should be arity {expected}"#)]
    InvalidArity {
        /// The given arity.
        arity: usize,
        /// The expected arity.
        expected: usize,
    },
    /// Arity is unsupported for this format, exact value is required.
    #[error(r#"unsupported arity "{arity}" for format {format}, must be {required}"#)]
    InvalidArityExact {
        /// The given arity.
        arity: usize,
        /// The required arity.
        required: usize,
        /// The file format.
        format: FileFormat,
    },
    /// Format does not support complex types
    #[error(r"Format {format} does not support complex types")]
    UnsupportedComplexTypes {
        /// The file format.
        format: FileFormat,
    },
    /// File could not be read
    #[error(r#"File "{path}" could not be read."#)]
    IoError {
        /// Contains the wrapped error
        error: std::io::Error,
        /// Path that could not be read
        path: PathBuf,
    },
}
