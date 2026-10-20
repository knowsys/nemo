//! Why a text is not a query an import can ask.

use std::fmt;

/// Why a text is not a query an import can ask.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QueryError {
    /// The base IRI the query is read against does not parse.
    Base {
        /// The base as given.
        base: String,
        /// The parser's message.
        reason: String,
    },
    /// The text does not parse as a SPARQL query.
    Syntax {
        /// The parser's message.
        reason: String,
    },
    /// The query is not a `SELECT`.
    ///
    /// Only a `SELECT` has a projection, which is the imported table.
    NotSelect,
}

impl fmt::Display for QueryError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            QueryError::Base { base, reason } => {
                write!(f, "`{base}` is not a valid base IRI: {reason}")
            }
            QueryError::Syntax { reason } => f.write_str(reason),
            QueryError::NotSelect => f.write_str("only SELECT queries can be imported"),
        }
    }
}

impl std::error::Error for QueryError {}
