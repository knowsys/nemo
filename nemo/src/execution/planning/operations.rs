//! This module defines intermediate datastructures
//! that are used to generate execution plans for the physical layer.

pub mod aggregation;
pub mod duplicates;
pub mod filter;
pub mod function;
pub mod function_filter_negation;
pub mod import;
pub mod join;
pub mod join_cartesian;
pub mod join_imports;
pub mod join_seminaive;
pub mod negation;
pub mod projection_head;
pub mod restricted_frontier;
pub mod restricted_head;
pub mod restricted_null;
pub mod union;
