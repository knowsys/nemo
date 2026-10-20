//! Compilation: a program's statements into the forms the engine executes.
//!
//! The modules hold the translation into compiled rules,
//! the optimization passes over imports and rules, the type analysis,
//! and the rules' join orders and strata.

pub(super) mod compiled;
pub(super) mod import_plan;
pub(super) mod layout;
pub(super) mod optimize;
pub(super) mod order;
pub(super) mod strata;
pub(super) mod translation;
pub(super) mod types;
