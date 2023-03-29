//! This module collects data structures and operations on individual columns.

/// Module for defining [`ColumnBuilderAdaptive`] and [`ColumnBuilderAdaptiveT`]
pub mod adaptive_column_builder;
/// Adaptive Builder to create [`VecT`] columns, based on streamed data
pub mod proxy_builder;

pub mod column_types;
pub mod operations;
pub mod traits;
