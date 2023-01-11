//! This module collects data structures and functionality surrounding
//! the management of a collection of tables

/// Module for defining [`DatabaseInstance`]
pub mod database;
pub use database::DatabaseInstance;

/// Module for defining [`Sized`]
pub mod bytesized;
pub use bytesized::ByteSized;

/// Module for defining [`ExecutionPlan`]
pub mod execution_plan;
pub use execution_plan::ExecutionPlan;

/// Module containing functionality for type analysis
pub mod type_analysis;
