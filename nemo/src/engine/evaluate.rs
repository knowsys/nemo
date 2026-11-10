//! Evaluation of the compiled program to its fixpoint.
//!
//! The modules:
//! - [reason] runs the semi-naive loop over the strata and their rules.
//! - [application] performs one rule application as an operation plan.
//! - [body] builds the plan of a rule body, wherever a body is evaluated.
//! - [extensional] records which revision blocks of a table hold input facts rather than
//!   derived ones.
//! - [contexts] keeps the evaluation context of each run.
//! - `fetch` applies the rules of bound SPARQL imports, which query an endpoint instead of
//!   joining.

pub(super) mod application;
pub(super) mod body;
pub(super) mod contexts;
pub(super) mod extensional;
pub(super) mod fetch;
pub(super) mod reason;
