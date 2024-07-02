//! This module defines [GroundTerm].

use std::{fmt::Display, hash::Hash};

use nemo_physical::datavalues::AnyDataValue;

use crate::rule_model::{
    component::ProgramComponent, error::ProgramConstructionError, origin::Origin,
};

/// Primitive ground term
#[derive(Debug, Clone, Eq)]
pub struct GroundTerm {
    /// Origin of this component
    origin: Origin,
    /// Value of this term
    value: AnyDataValue,
}

impl GroundTerm {
    /// Create a new [GroundTerm].
    pub fn new(value: AnyDataValue) -> Self {
        Self {
            origin: Origin::Created,
            value,
        }
    }
}

impl Display for GroundTerm {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.value.fmt(f)
    }
}

impl PartialEq for GroundTerm {
    fn eq(&self, other: &Self) -> bool {
        self.value == other.value
    }
}

impl PartialOrd for GroundTerm {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.value.partial_cmp(&other.value)
    }
}

impl Hash for GroundTerm {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.value.hash(state);
    }
}

impl ProgramComponent for GroundTerm {
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
        Ok(())
    }
}

// impl ASTConstructable for GroundTerm {
//     type Node<'a> = Primitive<'a>;

//     fn from_ast_node<'a>(
//         node: Self::Node<'a>,
//         origin: ExternalReference,
//         context: &ASTContext,
//     ) -> Self {
//         match node {
//             Primitive::Constant(token) => {
//                 Self::create_parsed(AnyDataValue::new_iri(token.to_string()), origin)
//             }
//             Primitive::PrefixedConstant {
//                 prefix, constant, ..
//             } => {
//                 let prefixed_constant = prefix
//                     .map(|token| {
//                         context
//                             .prefixes
//                             .get(&token.to_string())
//                             .cloned()
//                             .unwrap_or(token.to_string()) // TODO: We could also panic here
//                     })
//                     .unwrap_or(String::from(""))
//                     + &constant.to_string();

//                 Self::create_parsed(AnyDataValue::new_iri(prefixed_constant), origin)
//             }
//             Primitive::Number {
//                 span,
//                 sign,
//                 before,
//                 dot,
//                 after,
//                 exponent,
//             } => {
//                 // TODO: Create number values
//                 // Self::create_parsed(AnyDataValue:: span.to_string(), origin)
//                 todo!()
//             }
//             Primitive::String(string) => {
//                 Self::create_parsed(AnyDataValue::new_plain_string(string.to_string()), origin)
//             }
//             Primitive::Iri(iri) => {
//                 Self::create_parsed(AnyDataValue::new_iri(iri.to_string()), origin)
//             }
//             Primitive::RdfLiteral { string, iri, .. } => Self::create_parsed(
//                 AnyDataValue::new_other(string.to_string(), iri.to_string()),
//                 origin,
//             ),
//         }
//     }
// }
