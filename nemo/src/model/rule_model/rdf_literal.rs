use nemo_physical::{datatypes::Double, error::ReadingError};
use thiserror::Error;

use crate::model::types::primitive_logical_value::LogicalString;

use super::{Constant, NumericLiteral};

/// XSD type for string
pub const XSD_STRING: &str = "http://www.w3.org/2001/XMLSchema#string";
/// XSD type for double
pub const XSD_DOUBLE: &str = "http://www.w3.org/2001/XMLSchema#double";
/// XSD type for decimal
pub const XSD_DECIMAL: &str = "http://www.w3.org/2001/XMLSchema#decimal";
/// XSD type for integer
pub const XSD_INTEGER: &str = "http://www.w3.org/2001/XMLSchema#integer";

/// An RDF literal.
#[derive(Debug, Eq, PartialEq, Hash, Clone, PartialOrd, Ord)]
pub enum RdfLiteral {
    /// A language string.
    LanguageString {
        /// The literal value.
        value: String,
        /// The language tag.
        tag: String,
    },
    /// A literal with a datatype.
    DatatypeValue {
        /// The literal value.
        value: String,
        /// The datatype IRI.
        datatype: String,
    },
}

impl std::fmt::Display for RdfLiteral {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RdfLiteral::LanguageString { value, tag } => write!(f, "\"{value}\"@{tag}"),
            RdfLiteral::DatatypeValue { value, datatype } => write!(f, "\"{value}\"^^<{datatype}>"),
        }
    }
}

/// An [`InvalidRdfLiteral`]
#[derive(Debug, Error, PartialEq)]
#[error("The literal \"{}\" is not valid.", .literal)]
pub struct InvalidRdfLiteral {
    literal: RdfLiteral,
}

impl InvalidRdfLiteral {
    /// Create new `InvalidRdfLiteral` error
    pub fn new(literal: RdfLiteral) -> Self {
        Self { literal }
    }
}

impl From<InvalidRdfLiteral> for ReadingError {
    fn from(value: InvalidRdfLiteral) -> Self {
        Self::InvalidRdfLiteral(value.literal.to_string())
    }
}

impl TryFrom<RdfLiteral> for Constant {
    type Error = InvalidRdfLiteral;

    fn try_from(literal: RdfLiteral) -> Result<Self, Self::Error> {
        match literal {
            RdfLiteral::LanguageString { .. } => Ok(Self::RdfLiteral(literal)),
            RdfLiteral::DatatypeValue {
                ref value,
                ref datatype,
            } => match datatype.as_str() {
                XSD_STRING => Ok(Constant::StringLiteral(
                    LogicalString::from(value.to_string()).into(),
                )),
                XSD_INTEGER => {
                    let trimmed = value.strip_prefix(['-', '+']).unwrap_or(value);

                    if !trimmed.chars().all(|c| c.is_ascii_digit()) {
                        Err(InvalidRdfLiteral::new(literal.clone()))
                    } else {
                        Ok(value
                            .parse()
                            .map(|v| Self::NumericLiteral(NumericLiteral::Integer(v)))
                            .unwrap_or(Self::RdfLiteral(literal)))
                    }
                }
                XSD_DECIMAL => match value.rsplit_once('.') {
                    Some((a, b)) => {
                        let trimmed_a = a.strip_prefix(['-', '+']).unwrap_or(a);
                        let is_valid = trimmed_a.chars().all(|c| c.is_ascii_digit())
                            && b.chars().all(|c| c.is_ascii_digit());

                        if !is_valid {
                            Err(InvalidRdfLiteral::new(literal.clone()))
                        } else {
                            Ok(a.parse()
                                .ok()
                                .and_then(|a| Some((a, b.parse().ok()?)))
                                .map(|(a, b)| Self::NumericLiteral(NumericLiteral::Decimal(a, b)))
                                .unwrap_or(Self::RdfLiteral(literal)))
                        }
                    }
                    None => {
                        let trimmed = value.strip_prefix(['-', '+']).unwrap_or(value);
                        let is_valid = trimmed.chars().all(|c| c.is_ascii_digit());

                        if !is_valid {
                            Err(InvalidRdfLiteral::new(literal.clone()))
                        } else {
                            Ok(value
                                .parse()
                                .ok()
                                .map(|v| Self::NumericLiteral(NumericLiteral::Decimal(v, 0)))
                                .unwrap_or(Self::RdfLiteral(literal)))
                        }
                    }
                },
                XSD_DOUBLE => Ok(value
                    .parse()
                    .ok()
                    .and_then(|f64| Double::new(f64).ok())
                    .map(|d| Self::NumericLiteral(NumericLiteral::Double(d)))
                    .unwrap_or(Self::RdfLiteral(literal))),
                _ => Ok(Self::RdfLiteral(literal)),
            },
        }
    }
}
