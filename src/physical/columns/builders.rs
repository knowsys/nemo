//! This module collects objects which are able to build a column data structure

/// Module for defining [`ColumnBuilder`]
pub mod colbuilder;
pub use colbuilder::ColumnBuilder;

/// Module for defining [`AdaptiveColumnBuilder`]
pub mod colbuilder_adaptive;
pub use colbuilder_adaptive::AdaptiveColumnBuilder;
pub use colbuilder_adaptive::AdaptiveColumnBuilderT;
