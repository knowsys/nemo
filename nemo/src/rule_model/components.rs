//! This module defines the logical components that make up a program.

#![allow(missing_docs)]

#[macro_use]
pub mod atom;
pub mod datatype;
pub mod fact;
pub mod import_export;
pub mod literal;
pub mod output;
pub mod parameter;
pub mod rule;
pub mod tag;
pub mod term;

use std::fmt::{Debug, Display};

use enum_assoc::Assoc;
use term::{
    primitive::{variable::Variable, Primitive},
    Term,
};

use super::{error::ValidationErrorBuilder, origin::Origin};

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
    /// One of the given kinds:
    #[assoc(name = "oneof")]
    OneOf(&'static [ProgramComponentKind]),
}

/// Trait implemented by objects that are part of the logical rule model of the nemo language.
pub trait ProgramComponent: Debug + Display + Sized {
    type ValidationResult = ();

    /// Return the [ProgramComponentKind] of this component.
    fn kind(&self) -> ProgramComponentKind;

    /// Return the [Origin] of this component.
    fn origin(&self) -> &Origin;

    /// Set the [Origin] of this component.
    fn set_origin(self, origin: Origin) -> Self;

    /// Validate this component.
    ///
    /// Errors will be appended to the given [ValidationErrorBuilder].
    /// Returns `Some(())` if successful and `None` otherwise.
    fn validate(&self, builder: &mut ValidationErrorBuilder) -> Option<Self::ValidationResult>;
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
