//! Compilation: turning a program's statements into the [compiled program](compiled).
//!
//! - [translation] turns the statements into stored rows and compiled rules.
//! - [import_plan] and [layout] describe what the loads of imported predicates leave out.
//! - [optimize] has the optimization passes, and [types] the type analysis.
//! - [order] and [strata] choose the rules' join orders and strata.

pub(super) mod compiled;
pub(super) mod import_plan;
pub(super) mod layout;
pub(super) mod optimize;
pub(super) mod order;
pub(super) mod strata;
pub(super) mod translation;
pub(super) mod types;
