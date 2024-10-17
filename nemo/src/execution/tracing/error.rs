//! This module defines [TracingError].

use thiserror::Error;

/// Error that can occur while tracing.
#[derive(Debug, Error, Copy, Clone)]
pub enum TracingError {
    /// Error when tracing rules with count and sum aggregate
    #[error("tracing is only supported for #min and #max aggregation")]
    UnsupportedFeatureNonMinMaxAggregation,
    /// Error when tracing over aggregates that involve arithmetic
    #[error("tracing not supported for aggregates combined with arithmetic")]
    UnsupportedFeatureComplexAggregates,
}
