//! This module defines the logical components that make up a program.

#[macro_use]

pub mod atom;
pub mod datatype;
pub mod fact;
pub mod import_export;
pub mod literal;
pub mod output;
pub mod rule;
pub mod tag;
pub mod term;

use std::fmt::{Debug, Display};

use enum_assoc::Assoc;
use term::primitive::variable::Variable;

use super::{
    error::{ValidationError, ValidationErrorBuilder},
    origin::Origin,
};

/// TODO: Think whether this is needed
/// Types of [ProgramComponent]s
#[derive(Assoc, Debug, Copy, Clone, Eq, PartialEq)]
#[func(pub fn name(&self) -> &'static str)]
pub enum ProgramComponentKind {
    /// Variable
    #[assoc(name = "variable")]
    Variable,
    /// Plain String
    #[assoc(name = "string")]
    PlainString,
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
    /// Program
    #[assoc(name = "program")]
    Program,
}

/// Trait implemented by objects that are part of the logical rule model of the nemo language.
pub trait ProgramComponent: Debug + Display {
    /// Return the [ProgramComponentKind] of this component.
    fn kind(&self) -> ProgramComponentKind;

    /// Construct this object from a string.
    fn parse(_string: &str) -> Result<Self, ValidationError>
    where
        Self: Sized;

    /// Return the [Origin] of this component.
    fn origin(&self) -> &Origin;

    /// Set the [Origin] of this component.
    fn set_origin(self, origin: Origin) -> Self
    where
        Self: Sized;

    /// Validate this component
    fn validate(&self, builder: &mut ValidationErrorBuilder) -> Result<(), ()>
    where
        Self: Sized;
}

/// Trait implemented by program components that allow iterating over [Variable]s
pub trait IterableVariables {
    /// Return an iterator over all [Variable]s contained within this program component.
    fn variables<'a>(&'a self) -> Box<dyn Iterator<Item = &'a Variable> + 'a>;

    /// Return a mutable iterator over all [Variable]s contained within this program component.
    fn variables_mut<'a>(&'a mut self) -> Box<dyn Iterator<Item = &'a mut Variable> + 'a>;
}
