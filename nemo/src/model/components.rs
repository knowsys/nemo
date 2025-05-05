//! This module defines the logical components that make up a program.

#![allow(missing_docs)]

pub mod atom;
pub mod program;
pub mod rule;

use std::{fmt::Debug, fmt::Display};

use atom::Atom;
use enum_assoc::Assoc;
use program::Program;
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
    /// Program
    #[assoc(name = "program")]
    Program,
}

/// TODO: This is placeholder for improved validation error type
#[derive(Debug, Clone, Copy)]
pub struct NewValidationError(usize);

/// Trait that collects common methods of all components
/// of Nemo's logical rule model
pub trait ComponentBehavior {
    /// Return the [ProgramComponentKind] of this component.
    fn kind(&self) -> ProgramComponentKind;

    /// Validate this component.
    fn validate(&self) -> Result<(), NewValidationError>;

    fn boxed_clone(&self) -> Box<dyn ProgramComponent>;
}

/// Trait that collects methods for identifying and tracking the origins of  
/// components of Nemo's logical rule model
pub trait ComponentIdentity {
    /// Return the [ProgramComponentId] associated with this component.
    fn id(&self) -> ProgramComponentId;

    /// Set the [ProgramComponentId] of this component.
    fn set_id(&mut self, id: ProgramComponentId);

    /// Return the [Origin] of this component.
    fn origin(&self) -> &Origin;

    /// Set the [Origin] of this component.
    fn set_origin(&mut self, origin: Origin);
}

/// Trait that allows to iterate over children
/// of components of Nemo's logical rule model
pub trait IterableComponent {
    /// Return an iterator over all [Variable]s contained within this program component.
    fn children<'a>(&'a self) -> Box<dyn Iterator<Item = &'a dyn ProgramComponent> + 'a>;

    /// Return a mutable iterator over all [Variable]s contained within this program component.
    fn children_mut<'a>(
        &'a mut self,
    ) -> Box<dyn Iterator<Item = &'a mut dyn ProgramComponent> + 'a>;
}

/// Trait that defines a helper method that is useful
/// when combining [Origin]s
pub(crate) trait EffectiveOrigin: ComponentIdentity + ComponentBehavior {
    /// If the component is assigned to a [super::pipeline::ProgramPipeline],
    /// the [Origin] will be a reference using its [ProgramComponentId].
    /// Otherwise, returns the [Origin] of the component.
    fn effective_origin(&self) -> Origin {
        if self.id().is_assigned() {
            Origin::Reference(self.id())
        } else {
            Origin::Component(self.boxed_clone())
        }
    }
}

impl<Component: ComponentIdentity + ComponentBehavior> EffectiveOrigin for Component {}

/// Fallible version of [std::convert::AsRef]
pub trait TryAsRef<T> {
    /// Try to convert `&self` into `&T`.
    ///
    /// Returns `None` if unsuccessful.
    fn try_as_ref(&self) -> Option<&T>;
}

/// Trait implemented by components of Nemo's logical rule model
/// that allows conversion between them
pub trait ComponentCast: TryAsRef<Rule> + TryAsRef<Atom> + TryAsRef<Program> {}
impl<Component> ComponentCast for Component where
    Component: TryAsRef<Rule> + TryAsRef<Atom> + TryAsRef<Program>
{
}

impl<ComponentLeft: ComponentBehavior, ComponentRight: ComponentBehavior> TryAsRef<ComponentLeft>
    for ComponentRight
{
    default fn try_as_ref(&self) -> Option<&ComponentLeft> {
        None
    }
}

impl<Component: ComponentBehavior> TryAsRef<Component> for Component {
    fn try_as_ref(&self) -> Option<&Component> {
        Some(self)
    }
}

/// Trait implemented by objects that are part
/// of the logical rule model of the Nemo language.
pub trait ProgramComponent:
    Debug + Display + ComponentBehavior + ComponentIdentity + ComponentCast + IterableComponent
{
}
impl<Component> ProgramComponent for Component where
    Component:
        Debug + Display + ComponentBehavior + ComponentIdentity + ComponentCast + IterableComponent
{
}

pub(crate) fn component_iterator<'a, Component, Iter>(
    iterator: Iter,
) -> impl Iterator<Item = &'a dyn ProgramComponent>
where
    Component: ProgramComponent + 'a,
    Iter: Iterator<Item = &'a Component>,
{
    iterator.map(|element| {
        let element: &dyn ProgramComponent = element;
        element
    })
}

pub(crate) fn component_iterator_mut<'a, Component, Iter>(
    iterator: Iter,
) -> impl Iterator<Item = &'a mut dyn ProgramComponent>
where
    Component: ProgramComponent + 'a,
    Iter: Iterator<Item = &'a mut Component>,
{
    iterator.map(|element| {
        let element: &mut dyn ProgramComponent = element;
        element
    })
}

impl Clone for Box<dyn ProgramComponent> {
    fn clone(&self) -> Self {
        self.boxed_clone()
    }
}
