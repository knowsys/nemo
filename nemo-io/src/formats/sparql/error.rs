//! The errors of [SparqlQuery::parse](super::SparqlQuery::parse).

use std::fmt;

/// The reason that [SparqlQuery::parse](super::SparqlQuery::parse) rejects a text.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QueryError {
    /// The base IRI that the query is resolved against does not parse.
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
    /// Only a `SELECT` has a projection, which is the result table.
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
