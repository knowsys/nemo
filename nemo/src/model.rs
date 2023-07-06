//! The rule models.

/// Defines the rule model variant used during the chase computation.
pub mod chase_model;

/// Defines the "official" rule model that we expose to the outside
/// world. Allows faithful serialisation of parsed rule programs.
pub mod rule_model;

pub mod types;

/// Forward everything to the rule model.
pub use rule_model::*;
pub use types::complex_types::*;
pub use types::primitive_types::PrimitiveType;
