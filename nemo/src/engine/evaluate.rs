//! Evaluation of the compiled program to its fixpoint.
//!
//! The modules are:
//!
//! - `reason`: the semi-naive loop over the strata and their rules,
//! - `application`: one rule application as an operation plan,
//! - `body`: the plan of a rule body, shared with the explanations,
//! - `extensional`: which revision blocks of a table hold input facts rather than derived ones,
//! - `contexts`: the evaluation context of each reasoning run,
//! - `fetch`: the rules of bound SPARQL imports, which query an endpoint instead of joining.

pub(super) mod application;
pub(super) mod body;
pub(super) mod contexts;
pub(super) mod extensional;
#[cfg(feature = "sparql")]
pub(super) mod fetch;
pub(super) mod reason;
