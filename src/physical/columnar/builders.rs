//! This module collects objects which are able to build a column data structure

/// Module for defining [`ColumnBuilder`]
pub mod columnbuilder;
pub use columnbuilder::ColumnBuilder;

/// Module for defining [`ColumnBuilderAdaptive`]
pub mod columnbuilder_adaptive;
pub use columnbuilder_adaptive::ColumnBuilderAdaptive;
pub use columnbuilder_adaptive::ColumnBuilderAdaptiveT;
