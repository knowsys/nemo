//! This module defines the logical components that make up a program.

#![allow(missing_docs)]

pub mod atom;
pub mod rule;

use std::{fmt::Debug, fmt::Display};

use enum_assoc::Assoc;

use super::{origin::Origin, pipeline::id::ProgramComponentId};

/// Types of [ProgramComponent]s
#[derive(Assoc, Debug, Copy, Clone, Eq, PartialEq, Hash)]
#[func(pub fn name(&self) -> &'static str)]
pub enum ProgramComponentKind {
    /// Variable
    #[assoc(name = "variable")]
    Variable,
    /// Rule
    #[assoc(name = "rule")]
    Rule,
    /// Atom
    #[assoc(name = "atom")]
    Atom,
}

/// TODO: This is placeholder for improved validation error type
#[derive(Debug, Clone, Copy)]
pub struct NewValidationError(usize);

pub trait ProgramComponent: Debug + Display + Sized {
    /// Return the [ProgramComponentKind] of this component.
    fn kind(&self) -> ProgramComponentKind;

    /// Return the [Origin] of this component.
    fn origin(&self) -> &Origin;

    /// Return the [ProgramComponentId] associated with this component.
    fn id(&self) -> ProgramComponentId;

    /// Validate this component.
    fn validate(&self) -> Result<(), NewValidationError>;
}

/// Trait that defines a helper method that is useful
/// when combining [Origin]s
pub(crate) trait EffectiveOrigin: ProgramComponent {
    ///
    fn effective_origin(&self) -> Origin {
        if self.id().is_assigned() {
            Origin::Reference(self.id())
        } else {
            self.origin().clone()
        }
    }
}

impl<Component: ProgramComponent> EffectiveOrigin for Component {}
