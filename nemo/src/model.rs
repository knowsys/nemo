//! The rule models.

/// Defines the rule model variant used during the chase computation.
pub mod chase_model;

/// Defines the "official" rule model that we expose to the outside
/// world. Allows faithful serialisation of parsed rule programs.
///
/// TODO: Will this become the AST-Model?
pub mod rule_model;

/// TODO: Should this be the "official" rule model that we expose to the outside world?
pub mod object_model;

pub mod types;

/// Forward everything to the rule model.
pub use rule_model::*;
pub use types::complex_types::*;
pub use types::primitive_types::PrimitiveType;

use std::collections::HashMap;

/// Map from variables to terms
pub type VariableAssignment = HashMap<Variable, Term>;
