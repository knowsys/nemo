//! Specifications: descriptions of plan operations, their inputs and outputs,
//! and how they compute them.

mod aggregate;
mod aggregate_function;
mod computed;
mod expression;
mod filter;

pub use self::{
    aggregate::AggregateColumnSpec, aggregate_function::AggregateFunction,
    computed::ComputedColumnSpec, expression::Expression, filter::FilterSpec,
};
