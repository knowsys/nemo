//! What a [check](crate::engine::Engine::check) of a program reports:
//! its diagnostics, and the types that its analysis inferred.
//!
//! It also contains the rewrite through which a check reads the parameters without a value.

mod inferred_types;
mod report;
mod rule_types;
mod unvalued;

pub use self::{inferred_types::InferredTypes, report::CheckReport, rule_types::RuleTypes};

pub(in crate::engine) use self::unvalued::rewrite_unvalued;
