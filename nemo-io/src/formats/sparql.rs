//! SPARQL endpoints as sources: each `SELECT` query has one *result table*, its projection.
//!
//! A [SparqlQuery] is a validated query, which builds the [request](SparqlQuery::request) for
//! its solutions.
//! A query can also be asked with values for some of its projected variables,
//! which it sends as `VALUES` blocks ([SparqlQuery::with_values]).
//! A [SparqlReader] reads the endpoint's responses, which are in SPARQL TSV.
//! [compose] merges several queries into one.

pub mod compose;

mod algebra;
mod cut_off;
mod error;
mod query;
mod response;
mod turtle;
mod values;

/// The query algebra crate, for composing queries from expressions and patterns.
pub use spargebra;

pub use self::{
    algebra::QueryShape,
    error::QueryError,
    query::{DEFAULT_QUERY, SparqlQuery},
    response::SparqlReader,
    values::ValuesBlock,
};
