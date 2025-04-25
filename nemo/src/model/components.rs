//! This module defines the logical components that make up a program.

#![allow(missing_docs)]

pub mod atom;
pub mod program;
pub mod rule;

use std::{fmt::Debug, fmt::Display};

use atom::Atom;
use enum_assoc::Assoc;
use rule::Rule;

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

/// Trait implemented by objects that are part
/// of the logical rule model of the nemo language.
pub trait ProgramComponent: Debug + Display {
    /// Return the [ProgramComponentKind] of this component.
    fn kind(&self) -> ProgramComponentKind;

    /// Return the [Origin] of this component.
    fn origin(&self) -> &Origin;

    /// Return the [ProgramComponentId] associated with this component.
    fn id(&self) -> ProgramComponentId;

    /// Set the [Origin] of this component.
    fn set_origin(&mut self, origin: Origin);

    /// Set the [ProgramComponentId] of this component.
    fn set_id(&mut self, id: ProgramComponentId);

    /// Validate this component.
    fn validate(&self) -> Result<(), NewValidationError>;
}

/// Trait that defines a helper method that is useful
/// when combining [Origin]s
pub(crate) trait EffectiveOrigin: ProgramComponent {
    /// If the component is assigned to a [super::pipeline::ProgramPipeline],
    /// the [Origin] will be a reference using its [ProgramComponentId].
    /// Otherwise, returns the [Origin] of the component.
    fn effective_origin(&self) -> Origin {
        if self.id().is_assigned() {
            Origin::Reference(self.id())
        } else {
            self.origin().clone()
        }
    }
}

impl<Component: ProgramComponent> EffectiveOrigin for Component {}

/// Trait implemented by [ProgramComponent]s
/// to allow an iteration over their sub components.
pub(crate) trait IterableProgramComponent {
    /// Return an iterator over all [Variable]s contained within this program component.
    fn components<'a>(&'a self) -> Box<dyn Iterator<Item = &'a dyn ProgramComponent> + 'a>;

    /// Return a mutable iterator over all [Variable]s contained within this program component.
    fn components_mut<'a>(
        &'a mut self,
    ) -> Box<dyn Iterator<Item = &'a mut dyn ProgramComponent> + 'a>;

    /// Return an iterator over all [Rule]s contained in this [ProgramComponent].
    fn rules(&self) -> impl Iterator<Item = &Rule> {
        Option::<Rule>::None.iter()
    }

    /// Return an iterator over all [Atom]s contained in this [ProgramComponent].
    fn atoms(&self) -> impl Iterator<Item = &Atom> {
        Option::<Atom>::None.iter()
    }
}
