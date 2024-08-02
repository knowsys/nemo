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

use term::primitive::variable::Variable;

use super::{
    error::{ValidationError, ValidationErrorBuilder},
    origin::Origin,
};

/// TODO: Think whether this is needed
/// Types of [ProgramComponent]s
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum ProgramComponentKind {
    /// Variable
    Variable,
    /// Primitive groun term
    PrimitiveGround,
    /// Map
    Map,
    /// Tuple
    Tuple,
    /// Operation
    Operation,
    /// Function term
    FunctionTerm,
    /// Atom
    Atom,
}

/// Trait implemented by objects that are part of the logical rule model of the nemo language.
pub trait ProgramComponent: Debug + Display {
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
