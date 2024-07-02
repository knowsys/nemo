//! This module defines [ExistentialVariable].

use std::{fmt::Display, hash::Hash};

use crate::rule_model::{
    component::ProgramComponent, error::ProgramConstructionError, origin::Origin,
};

use super::VariableName;

/// Variable that allows to assert the existence of an object
#[derive(Debug, Clone, Eq)]
pub struct ExistentialVariable {
    /// Origin of this component
    origin: Origin,

    /// Name of the variable
    name: VariableName,
}

impl ExistentialVariable {
    /// Create a new [ExistentialVariable].
    pub fn new(name: &str) -> Self {
        Self {
            origin: Origin::Created,
            name: VariableName::new(name.to_string()),
        }
    }

    /// Return the name of this variable.
    pub fn name(&self) -> String {
        self.name.to_string()
    }
}

impl Display for ExistentialVariable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "!{}", self.name)
    }
}

impl PartialEq for ExistentialVariable {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
    }
}

impl PartialOrd for ExistentialVariable {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.name.partial_cmp(&other.name)
    }
}

impl Hash for ExistentialVariable {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.name.hash(state);
    }
}

impl ProgramComponent for ExistentialVariable {
    fn parse(_string: &str) -> Result<Self, ProgramConstructionError>
    where
        Self: Sized,
    {
        todo!()
    }

    fn origin(&self) -> &Origin {
        &self.origin
    }

    fn set_origin(mut self, origin: Origin) -> Self
    where
        Self: Sized,
    {
        self.origin = origin;
        self
    }

    fn validate(&self) -> Result<(), ProgramConstructionError>
    where
        Self: Sized,
    {
        todo!()
    }
}

// impl ASTConstructable for ExistentialVariable {
//     type Node<'a> = Term<'a>;

//     fn from_ast_node<'a>(node: Term<'a>, origin: ExternalReference, _context: &ASTContext) -> Self {
//         if let Term::UniversalVariable(token) = node {
//             let name = token.span.to_string();

//             Self {
//                 origin: Origin::External(origin),
//                 name: VariableName::new(name),
//             }
//         } else {
//             unreachable!("TODO")
//         }
//     }
// }
