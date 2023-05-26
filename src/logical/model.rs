//! The rule models.

/// Defines the rule model variant used during the chase computation.
pub(crate) mod chase_model;

/// Defines the "official" rule model that we expose to the outside
/// world. Allows faithful serialisation of parsed rule programs.
pub mod rule_model;

/// Forward everything to the rule model.
pub use rule_model::*;
