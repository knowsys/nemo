//! This module collects objects which are able to build a column data structure

/// Module for defining [`ColBuilder`]
pub mod colbuilder;
pub use colbuilder::ColBuilder;

/// Module for defining [`ColBuilderAdaptive`]
pub mod colbuilder_adaptive;
pub use colbuilder_adaptive::ColBuilderAdaptive;
pub use colbuilder_adaptive::ColBuilderAdaptiveT;
