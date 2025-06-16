//! This module defines the logical components that make up a program.

#![allow(missing_docs)]

#[macro_use]
pub mod atom;
pub mod fact;
pub mod import_export;
pub mod literal;
pub mod output;
pub mod parameter;
pub mod rule;
pub mod statement;
pub mod symbols;
pub mod tag;
pub mod term;

use std::fmt::{Debug, Display};

use atom::Atom;
use enum_assoc::Assoc;
use import_export::{
    attribute::ImportExportAttribute, specification::ImportExportSpec, ExportDirective,
    ImportDirective,
};
use literal::Literal;
use output::Output;
use parameter::ParameterDeclaration;
use rule::Rule;
use term::{
    aggregate::Aggregate,
    function::FunctionTerm,
    map::Map,
    primitive::{
        ground::GroundTerm,
        variable::{
            existential::ExistentialVariable, global::GlobalVariable, universal::UniversalVariable,
            Variable,
        },
        Primitive,
    },
    tuple::Tuple,
    Term,
};

use crate::rule_model::{components::statement::Statement, programs::program::Program};

use super::{
    error::ValidationReport, origin::Origin, pipeline::id::ProgramComponentId, util::TryAsRef,
};

/// Types of [ProgramComponent]s
#[derive(Assoc, Debug, Copy, Clone, Eq, PartialEq, Hash)]
#[func(pub fn name(&self) -> &'static str)]
pub enum ProgramComponentKind {
    /// Variable
    #[assoc(name = "variable")]
    Variable,
    /// Plain String
    #[assoc(name = "string")]
    PlainString,
    /// Attribute
    #[assoc(name = "attribute")]
    Attribute,
    /// Language tagged string
    #[assoc(name = "language tagged string")]
    LanguageTaggedString,
    /// Iri
    #[assoc(name = "iri")]
    Iri,
    /// Single precision point number
    #[assoc(name = "float")]
    Float,
    /// Double precision floating point number
    #[assoc(name = "double")]
    Double,
    /// Integer value
    #[assoc(name = "integer")]
    Integer,
    /// Boolean
    #[assoc(name = "boolean")]
    Boolean,
    /// Null
    #[assoc(name = "null")]
    Null,
    /// Other
    #[assoc(name = "data value")]
    Other,
    /// Map
    #[assoc(name = "map")]
    Map,
    /// Tuple
    #[assoc(name = "tuple")]
    Tuple,
    /// Operation
    #[assoc(name = "operation")]
    Operation,
    /// Function term
    #[assoc(name = "function")]
    FunctionTerm,
    /// Aggregation term
    #[assoc(name = "aggregation")]
    Aggregation,
    /// Atom
    #[assoc(name = "atom")]
    Atom,
    /// Literal
    #[assoc(name = "literal")]
    Literal,
    /// Rule
    #[assoc(name = "rule")]
    Rule,
    /// Fact
    #[assoc(name = "fact")]
    Fact,
    /// Import
    #[assoc(name = "import")]
    Import,
    /// Export
    #[assoc(name = "export")]
    Export,
    /// Output
    #[assoc(name = "output")]
    Output,
    /// Parameter declaration directive
    #[assoc(name = "parameter")]
    ParameterDeclaration,
    /// Program
    #[assoc(name = "program")]
    Program,
    /// One of the given kinds:
    #[assoc(name = "oneof")]
    OneOf(&'static [ProgramComponentKind]),
}

/// Trait that collects common methods of all components
/// of Nemo's logical rule model
pub trait ComponentBehavior {
    /// Return the [ProgramComponentKind] of this component.
    fn kind(&self) -> ProgramComponentKind;

    /// Clone this component into a boxed trait object.
    fn boxed_clone(&self) -> Box<dyn ProgramComponent>;

    /// Check if this component is well-formed.
    fn validate(&self) -> Result<(), ValidationReport>;
}

/// Trait that contains methods for tracking the source of a component
pub trait ComponentSource {
    type Source;

    /// Return the source of this component.
    fn origin(&self) -> Self::Source;

    /// Set the source of this component.
    fn set_origin(&mut self, origin: Self::Source);
}

/// Trait that collects methods for identifying and tracking the origins of  
/// components of Nemo's logical rule model
pub trait ComponentIdentity {
    /// Return the [ProgramComponentId] associated with this component.
    fn id(&self) -> ProgramComponentId;

    /// Set the [ProgramComponentId] of this component.
    fn set_id(&mut self, id: ProgramComponentId);
}

/// Trait that allows to iterate over children
/// of components of Nemo's logical rule model
pub trait IterableComponent {
    /// Return an iterator over all [Variable]s contained within this program component.
    fn children<'a>(&'a self) -> Box<dyn Iterator<Item = &'a dyn ProgramComponent> + 'a> {
        Box::new(std::iter::empty())
    }

    /// Return a mutable iterator over all [Variable]s contained within this program component.
    fn children_mut<'a>(
        &'a mut self,
    ) -> Box<dyn Iterator<Item = &'a mut dyn ProgramComponent> + 'a> {
        Box::new(std::iter::empty())
    }
}

/// Trait that allows duplicate [ProgramComponent]s
/// while recording their [Origin]
pub trait ComponentDuplicate:
    ComponentSource<Source = Origin> + ComponentIdentity + ComponentBehavior
{
    /// If the component is assigned to a [super::pipeline::ProgramPipeline],
    /// the [Origin] will be a reference using its [ProgramComponentId].
    /// Otherwise, returns the [Origin] of the component.
    fn duplicated_origin(&self) -> Origin {
        if self.id().is_assigned() {
            Origin::Reference(self.id())
        } else {
            Origin::Component(self.boxed_clone())
        }
    }

    /// Duplicate a [ProgramComponent]
    /// while keeping track of the [Origin].
    fn duplicate(self) -> Self
    where
        Self: Clone,
    {
        let origin = self.duplicated_origin();
        let mut cloned = self.clone();
        cloned.set_origin(origin);

        cloned
    }
}

impl<Component: ComponentSource<Source = Origin> + ComponentIdentity + ComponentBehavior>
    ComponentDuplicate for Component
{
}

/// Trait implemented by components of Nemo's logical rule model
/// that allows conversion between them
pub trait ComponentCast:
    TryAsRef<ExistentialVariable>
    + TryAsRef<UniversalVariable>
    + TryAsRef<GlobalVariable>
    + TryAsRef<Variable>
    + TryAsRef<GroundTerm>
    + TryAsRef<Aggregate>
    + TryAsRef<Term>
    + TryAsRef<Tuple>
    + TryAsRef<Map>
    + TryAsRef<FunctionTerm>
    + TryAsRef<Primitive>
    + TryAsRef<Rule>
    + TryAsRef<Literal>
    + TryAsRef<Output>
    + TryAsRef<ImportDirective>
    + TryAsRef<ExportDirective>
    + TryAsRef<Atom>
    + TryAsRef<Program>
    + TryAsRef<ImportExportAttribute>
    + TryAsRef<ImportExportSpec>
    + TryAsRef<ParameterDeclaration>
    + TryAsRef<Statement>
{
}
impl<Component> ComponentCast for Component where
    Component: TryAsRef<ExistentialVariable>
        + TryAsRef<UniversalVariable>
        + TryAsRef<GlobalVariable>
        + TryAsRef<Variable>
        + TryAsRef<GroundTerm>
        + TryAsRef<Aggregate>
        + TryAsRef<Term>
        + TryAsRef<Tuple>
        + TryAsRef<Map>
        + TryAsRef<FunctionTerm>
        + TryAsRef<Primitive>
        + TryAsRef<Rule>
        + TryAsRef<Literal>
        + TryAsRef<Output>
        + TryAsRef<ImportDirective>
        + TryAsRef<ExportDirective>
        + TryAsRef<Atom>
        + TryAsRef<Program>
        + TryAsRef<ImportExportAttribute>
        + TryAsRef<ImportExportSpec>
        + TryAsRef<ParameterDeclaration>
        + TryAsRef<Statement>
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
    Debug
    + Display
    + ComponentBehavior
    + ComponentIdentity
    + ComponentSource<Source = Origin>
    + ComponentCast
    + IterableComponent
    + Send
    + Sync
{
}
impl<Component> ProgramComponent for Component where
    Component: Debug
        + Display
        + ComponentBehavior
        + ComponentIdentity
        + ComponentSource<Source = Origin>
        + ComponentCast
        + IterableComponent
        + Sync
        + Send
{
}

/// Helper function that converts an iterator over
/// references to concrete [ProgramComponent]s
/// into an iterator over trait object references.
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

/// Helper function that converts an iterator over
/// mutable references to concrete [ProgramComponent]s
/// into an iterator over mutable trait object references.
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

/// Trait implemented by program components that allow iterating over [Variable]s
pub trait IterableVariables {
    /// Return an iterator over all [Variable]s contained within this program component.
    fn variables<'a>(&'a self) -> Box<dyn Iterator<Item = &'a Variable> + 'a>;

    /// Return a mutable iterator over all [Variable]s contained within this program component.
    fn variables_mut<'a>(&'a mut self) -> Box<dyn Iterator<Item = &'a mut Variable> + 'a>;
}

/// Trait implemented by program components that allow iterating over [Primitive] terms
pub trait IterablePrimitives {
    type TermType = Term;

    /// Return an iterator over all [Primitive] terms contained within this program component.
    fn primitive_terms<'a>(&'a self) -> Box<dyn Iterator<Item = &'a Primitive> + 'a>;

    /// Return a mutable iterator over all [Primitive] terms contained within this program component.
    fn primitive_terms_mut<'a>(
        &'a mut self,
    ) -> Box<dyn Iterator<Item = &'a mut Self::TermType> + 'a>;
}
