//! This module defines the logical components that make up a program.

pub mod atom;
pub mod base;
pub mod fact;
pub mod import_export;
pub mod literal;
pub mod output;
pub mod rule;
pub mod term;

use std::fmt::{Debug, Display};

use super::{error::ProgramConstructionError, origin::Origin};

/// Trait implemented by objects that are part of the logical rule model of the nemo language.
pub trait ProgramComponent: Debug + Display + Clone + PartialEq + Eq {
    /// Construct this object from a string.
    fn parse(_string: &str) -> Result<Self, ProgramConstructionError>
    where
        Self: Sized;

    /// Return the [Origin] of this component.
    fn origin(&self) -> &Origin;

    /// Set the [Origin] of this component.
    fn set_origin(self, origin: Origin) -> Self
    where
        Self: Sized;

    /// Validate this component
    fn validate(&self) -> Result<(), ProgramConstructionError>
    where
        Self: Sized;
}
