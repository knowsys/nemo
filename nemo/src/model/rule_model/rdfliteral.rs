use nemo_physical::error::ReadingError;
use thiserror::Error;

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
