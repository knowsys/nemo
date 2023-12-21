//! This module defines errors that are relevant when dealing with data values.

use std::num::{ParseFloatError, ParseIntError};

use thiserror::Error;

/// Potential errors encountered when trying to construct [`DataValue`]s.
#[allow(variant_size_differences)]
#[derive(Error, Debug)]
pub enum DataValueCreationError {
    /// Error for floating point numbers that are not finite
    #[error("floating point number must represent a finite value (no infinity, no NaN)")]
    NonFiniteFloat,
    /// Error for problems with parsing floating point numbers
    #[error("floating point number could not be parsed")]
    FloatNotParsed(#[from] ParseFloatError),
    /// Error for problems with parsing integer numbers
    #[error("integer number could not be parsed")]
    IntegerNotParsed(#[from] ParseIntError),
    /// Error for problems with integer ranges
    #[error("integer number {value} is not in supported range [{min},{max}] for datatype {datatype_name}")]
    IntegerRange {
        /// Smallest value that would have been allowed
        min: i64,
        /// Largest value that would have been allowed
        max: i64,
        /// Actually given value
        value: i64,
        /// String that hints at the datatype that was used, the source of the range constraints
        datatype_name: String,
    },
    /// Error for problems with parsing boolean literals
    #[error("boolean value '{lexical_value}' could not be parsed")]
    BooleanNotParsed {
        /// Lexical value that failed to parse
        lexical_value: String,
    },
    /// Generic error for incorrect values given in lexical (string-based) form
    #[error("lexical value '{lexical_value}' is not valid for datatype '{datatype_iri}'")]
    InvalidLexicalValue {
        /// Lexical value that failed to parse
        lexical_value: String,
        /// Datatype IRI for which parsing failed
        datatype_iri: String,
    },
    /// Generic error for issues that should not arise when using the public API (and should maybe never arise
    /// if the crate works as intended)
    #[error("internal error when trying to create a datavalue: {0}")]
    InternalError(Box<dyn std::error::Error>),
}

impl PartialEq for DataValueCreationError {
    // Note: We cannot derive this with the boxed errors inside, but it is still convenient to have eq for the "normal" errors.
    fn eq(&self, other: &Self) -> bool {
        use DataValueCreationError::*;
        match (self, other) {
            (NonFiniteFloat, NonFiniteFloat) => true,
            (FloatNotParsed(a), FloatNotParsed(b)) => a == b,
            (IntegerNotParsed(a), IntegerNotParsed(b)) => a == b,
            (
                IntegerRange {
                    min: min_a,
                    max: max_a,
                    value: v_a,
                    datatype_name: d_a,
                },
                IntegerRange {
                    min: min_b,
                    max: max_b,
                    value: v_b,
                    datatype_name: d_b,
                },
            ) => min_a == min_b && max_a == max_b && v_a == v_b && d_a == d_b,
            (
                InvalidLexicalValue {
                    datatype_iri: dt_a,
                    lexical_value: v_a,
                },
                InvalidLexicalValue {
                    datatype_iri: dt_b,
                    lexical_value: v_b,
                },
            ) => dt_a == dt_b && v_a == v_b,
            _ => false,
        }
    }
}

/// Conceivable internal errors that we distinguish. These should not surface in
/// normal operation.
#[derive(Error, Debug)]
pub(crate) enum InternalDataValueCreationError {
    /// Error when retrieving a value from the dictionary
    #[error("could not recover DataValue from dictionary: id {0} not found")]
    DictionaryIdNotFound(usize),
    /// Error trying to convert a 32bit float to DataValue
    #[error("single precision floats are not supported in DataValues")]
    SinglePrecisionFloat,
}
