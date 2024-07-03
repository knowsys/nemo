//! This module defines [Term].

pub mod aggregate;
pub mod function;
pub mod map;
pub mod operation;
pub mod primitive;
pub mod tuple;

use std::fmt::{Debug, Display};

use function::FunctionTerm;
use map::Map;
use nemo_physical::datavalues::AnyDataValue;
use operation::Operation;
use primitive::{ground::GroundTerm, variable::Variable, Primitive};
use tuple::Tuple;

use crate::rule_model::{error::ProgramConstructionError, origin::Origin};

use super::ProgramComponent;

/// Name of a term
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Identifier(String);

impl Identifier {
    /// Create a new [Identifier].
    pub fn new(name: String) -> Self {
        Self(name)
    }

    /// Validate term name.
    pub fn is_valid(&self) -> bool {
        !self.0.is_empty()
    }
}

/// TODO
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd)]
pub enum Term {
    /// Unstructured, primitive term
    Primitive(Primitive),
    /// Abstract function over a list of terms
    FunctionTerm(FunctionTerm),
    /// Map of terms
    Map(Map),
    /// Operation applied to a list of terms
    Operation(Operation),
    /// Tuple
    Tuple(Tuple),
}

impl Term {
    /// Create a universal variable term.
    pub fn universal_variable(name: &str) -> Self {
        Self::Primitive(Primitive::Variable(Variable::universal(name)))
    }

    /// Create a anynmous variable term.
    pub fn anonymous_variable() -> Self {
        Self::Primitive(Primitive::Variable(Variable::anonymous()))
    }

    /// Create a existential variable term.
    pub fn existential_variable(name: &str) -> Self {
        Self::Primitive(Primitive::Variable(Variable::existential(name)))
    }

    /// Create a groud term.
    pub fn ground(value: AnyDataValue) -> Self {
        Self::Primitive(Primitive::Ground(GroundTerm::new(value)))
    }

    /// Create an integer term
    pub fn integer(number: i64) -> Self {
        Self::Primitive(Primitive::Ground(GroundTerm::new(
            AnyDataValue::new_integer_from_i64(number),
        )))
    }
}

impl Display for Term {
    fn fmt(&self, _f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}

impl ProgramComponent for Term {
    fn parse(_string: &str) -> Result<Self, ProgramConstructionError>
    where
        Self: Sized,
    {
        todo!()
    }

    fn origin(&self) -> &Origin {
        match self {
            Term::Primitive(primitive) => primitive.origin(),
            Term::FunctionTerm(function) => function.origin(),
            Term::Map(map) => map.origin(),
            Term::Operation(operation) => operation.origin(),
            Term::Tuple(tuple) => tuple.origin(),
        }
    }

    fn set_origin(self, origin: Origin) -> Self
    where
        Self: Sized,
    {
        match self {
            Term::Primitive(primitive) => Term::Primitive(primitive.set_origin(origin)),
            Term::FunctionTerm(function) => Term::FunctionTerm(function.set_origin(origin)),
            Term::Map(map) => Term::Map(map.set_origin(origin)),
            Term::Operation(operation) => Term::Operation(operation.set_origin(origin)),
            Term::Tuple(tuple) => Term::Tuple(tuple.set_origin(origin)),
        }
    }

    fn validate(&self) -> Result<(), ProgramConstructionError>
    where
        Self: Sized,
    {
        todo!()
    }
}

// impl ASTConstructable for Term {
//     type Node<'a> = crate::io::parser::ast::term::Term<'a>;

//     fn from_ast_node<'a>(
//         node: Self::Node<'a>,
//         origin: crate::rule_model::origin::ExternalReference,
//         context: &super::ASTContext,
//     ) -> Self {
//         todo!()
//     }
// }
