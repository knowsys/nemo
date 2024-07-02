//! This module defines [UniversalVariable].

use std::{fmt::Display, hash::Hash};

use crate::rule_model::{
    component::ProgramComponent, error::ProgramConstructionError, origin::Origin,
};

use super::VariableName;

/// Variable that can be bound to a specific value
///
/// Universal variables may not have a name,
/// in which case we call them anonymous.
#[derive(Debug, Clone, Eq)]
pub struct UniversalVariable {
    /// Origin of this component
    origin: Origin,

    /// Name of the variable
    ///
    /// This can be `None` in case this is an anonymous variable.
    name: Option<VariableName>,
}

impl UniversalVariable {
    /// Create a new named [UniversalVariable]
    pub fn new(name: &str) -> Self {
        Self {
            origin: Origin::Created,
            name: Some(VariableName::new(name.to_string())),
        }
    }

    /// Create a new anonymous [UniversalVariable]
    pub fn new_anonymous() -> Self {
        Self {
            origin: Origin::Created,
            name: None,
        }
    }

    /// Return the name of this variable,
    /// or `None` if the variable is unnamed.
    pub fn name(&self) -> Option<String> {
        self.name.as_ref().map(|name| name.to_string())
    }

    /// Return `true` if this is an anonymous variable,
    /// and `false` otherwise
    pub fn is_anonymous(&self) -> bool {
        self.name.is_none()
    }
}

impl Display for UniversalVariable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.name {
            Some(name) => write!(f, "?{}", name),
            None => write!(f, "_"),
        }
    }
}

impl PartialEq for UniversalVariable {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
    }
}

impl PartialOrd for UniversalVariable {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.name.partial_cmp(&other.name)
    }
}

impl Hash for UniversalVariable {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.name.hash(state);
    }
}

impl ProgramComponent for UniversalVariable {
    fn parse(_string: &str) -> Result<Self, ProgramConstructionError> {
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

// impl ASTConstructable for UniversalVariable {
//     type Node<'a> = Term<'a>;

//     fn from_ast_node<'a>(node: Term<'a>, origin: ExternalReference, _context: &ASTContext) -> Self {
//         if let Term::UniversalVariable(token) = node {
//             let name = token.span.to_string();

//             Self {
//                 origin: Origin::External(origin),
//                 name: Some(VariableName::new(name)),
//             }
//         } else {
//             unreachable!("TODO")
//         }
//     }
// }
