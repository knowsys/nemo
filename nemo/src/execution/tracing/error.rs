//! This module defines [TracingError].

use thiserror::Error;

/// Error that can occur while tracing.
#[derive(Debug, Error, Clone)]
pub enum TracingError {
    /// Error when tracing rules with count and sum aggregate
    #[error("tracing is only supported for #min and #max aggregation")]
    UnsupportedFeatureNonMinMaxAggregation,
    /// Error when tracing over aggregates that involve arithmetic
    #[error("tracing not supported for aggregates combined with arithmetic")]
    UnsupportedFeatureComplexAggregates,
    /// No fact with the given ID exists
    #[error("No fact with {id} exists for predicate {predicate}.")]
    InvalidFactId {
        /// Prediacte name of the fact in question
        predicate: String,
        /// Claimed fact index in the predicate table
        id: usize,
    },
    /// No fact matching the query exists
    #[error("No fact found for predicate {predicate} with query {query}.")]
    EmptyFactQuery {
        /// Prediacte name of the fact in question
        predicate: String,
        /// The query for the arguments of the fact that did not match
        query: String,
    },
    /// Invalid fact supplied given as input to tracing
    #[error("Invalid tracing fact: {fact}.")]
    InvalidFact {
        /// The invalid fact
        fact: String,
    },
}
