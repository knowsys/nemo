//! This module defines [Literal]

use std::{fmt::Display, hash::Hash};

use crate::rule_model::error::ValidationError;

use super::{atom::Atom, term::operation::Operation, ProgramComponent};

/// Literal
///
/// An [Atom], its negation, or an [Operation].
/// Literals are used to represent conditions that must be satisfied
/// for a rule to be applicable.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Literal {
    /// Positive atom
    Positive(Atom),
    /// Negative atom
    Negative(Atom),
    /// Operation
    Operation(Operation),
}

impl Display for Literal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Literal::Positive(positive) => write!(f, "{}", positive),
            Literal::Negative(negative) => write!(f, "~{}", negative),
            Literal::Operation(operation) => write!(f, "{}", operation),
        }
    }
}

impl ProgramComponent for Literal {
    fn parse(_string: &str) -> Result<Self, ValidationError>
    where
        Self: Sized,
    {
        todo!()
    }

    fn origin(&self) -> &crate::rule_model::origin::Origin {
        match self {
            Literal::Positive(positive) => positive.origin(),
            Literal::Negative(negative) => negative.origin(),
            Literal::Operation(operation) => operation.origin(),
        }
    }

    fn set_origin(self, origin: crate::rule_model::origin::Origin) -> Self
    where
        Self: Sized,
    {
        match self {
            Literal::Positive(positive) => Literal::Positive(positive.set_origin(origin)),
            Literal::Negative(negative) => Literal::Negative(negative.set_origin(origin)),
            Literal::Operation(operation) => Literal::Operation(operation.set_origin(origin)),
        }
    }

    fn validate(&self) -> Result<(), ValidationError>
    where
        Self: Sized,
    {
        todo!()
    }
}
