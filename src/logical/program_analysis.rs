//! Functionality for collecting useful information about a existential rule program befire its execution.

/// Computes useful information of a program before its execution
pub mod analysis;

/// Transformation of a program into an normalized form
pub mod normalization;

/// Functionality for computing promising variable orders from a program
pub mod variable_order;
