//! The rule models.

/// Defines the rule model variant used during the chase computation.
pub mod chase_model;

/// Defines the "official" rule model that we expose to the outside
/// world. Allows faithful serialization of parsed rule programs.
pub mod rule_model;

/// Forward everything to the rule model.
pub use rule_model::*;

use std::collections::HashMap;

/// Map from variables to terms
pub type VariableAssignment = HashMap<Variable, Term>;
