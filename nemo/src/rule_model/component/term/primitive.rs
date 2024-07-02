//! This module defines [PrimitiveTerm].

pub mod ground;
pub mod variable;

use std::{fmt::Display, hash::Hash};

use ground::GroundTerm;
use variable::Variable;

use crate::rule_model::{component::ProgramComponent, origin::Origin};

/// Primitive term
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd)]
pub enum Primitive {
    /// Variable
    Variable(Variable),
    /// Ground term
    Ground(GroundTerm),
}

impl Primitive {
    /// Return `true` when this term is not a variable and `false` otherwise.
    pub fn is_ground(&self) -> bool {
        matches!(self, Self::Ground(_))
    }
}

impl Display for Primitive {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Primitive::Variable(variable) => variable.fmt(f),
            Primitive::Ground(ground) => ground.fmt(f),
        }
    }
}

impl ProgramComponent for Primitive {
    fn parse(_string: &str) -> Result<Self, crate::rule_model::error::ProgramConstructionError>
    where
        Self: Sized,
    {
        todo!()
    }

    fn origin(&self) -> &Origin {
        match self {
            Self::Variable(variable) => variable.origin(),
            Self::Ground(ground) => ground.origin(),
        }
    }

    fn set_origin(self, origin: Origin) -> Self
    where
        Self: Sized,
    {
        match self {
            Self::Variable(variable) => Self::Variable(variable.set_origin(origin)),
            Self::Ground(ground) => Self::Ground(ground.set_origin(origin)),
        }
    }

    fn validate(&self) -> Result<(), crate::rule_model::error::ProgramConstructionError>
    where
        Self: Sized,
    {
        match self {
            Primitive::Variable(variable) => variable.validate(),
            Primitive::Ground(ground) => ground.validate(),
        }
    }
}

// impl ASTConstructable for Primitive {
//     type Node<'a> = Term<'a>;

//     fn from_ast_node<'a>(
//         node: Self::Node<'a>,
//         origin: ExternalReference,
//         context: &ASTContext,
//     ) -> Self {
//         match node {
//             Term::Primitive(primitive) => {
//                 Primitive::Ground(GroundTerm::from_ast_node(primitive, origin, context))
//             }
//             Term::Blank(token) => {
//                 let value: AnyDataValue = todo!();

//                 Primitive::Ground(GroundTerm::create_parsed(value, origin))
//             }
//             Term::UniversalVariable(_) | Term::ExistentialVariable(_) => {
//                 Primitive::Variable(Variable::from_ast_node(node, origin, context))
//             }
//             _ => unreachable!("TODO"),
//         }
//     }
// }
