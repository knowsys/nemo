//! This will contain all program components

use std::fmt::{Debug, Display};

use super::{manager::ProgramComponentId, origin::Origin};

pub mod rule;

/// Trait implemented by objects that are part of the logical rule model of the nemo language.
pub trait ProgramComponent: Debug + Display + Sized {
    type ValidationResult = ();

    // /// Return the [ProgramComponentKind] of this component.
    // fn kind(&self) -> ProgramComponentKind;

    /// Return the [Origin] of this component.
    fn origin(&self) -> &Origin;

    /// Set the [Origin] of this component.
    fn set_origin(self, origin: Origin) -> Self;

    /// Return the [ProgramComponentId] of this component
    ///
    fn id(&self) -> ProgramComponentId;

    /// Set the [ProgramComponentId] of this component
    /// Should this be publice?
    /// Should this just be mut?
    /// Should these just be crate public?
    fn set_id(self, id: ProgramComponentId) -> Self;

    // /// Validate this component.
    // ///
    // /// Errors will be appended to the given [ValidationErrorBuilder].
    // /// Returns `Some(())` if successful and `None` otherwise.
    // fn validate(&self, builder: &mut ValidationErrorBuilder) -> Option<Self::ValidationResult>;
}
