//! What a [check](crate::engine::Engine::check) of a program reports:
//! its diagnostics, and the types its analysis inferred.

mod inferred_types;
mod open_parameters;
mod report;
mod rule_types;

pub use self::{inferred_types::InferredTypes, report::CheckReport, rule_types::RuleTypes};

pub(in crate::engine) use self::open_parameters::open_parameters;
