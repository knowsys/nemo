//! This module collects data structures and functionality surrounding
//! the management of a collection of tables

/// Module for defining [`DatabaseInstance`]
pub mod database;
pub use database::DatabaseInstance;

/// Module for defining [`Sized`]
pub mod bytesized;
pub use bytesized::ByteSized;
