//! This module collects objects which are able to build a column data structure

/// Module for defining [`ColBuilder`]
pub mod columnbuilder;
pub use columnbuilder::ColBuilder;

/// Module for defining [`ColBuilderAdaptive`]
pub mod columnbuilder_adaptive;
pub use columnbuilder_adaptive::ColBuilderAdaptive;
pub use columnbuilder_adaptive::ColBuilderAdaptiveT;
