//! This module defines [Literal]

use std::{fmt::Display, hash::Hash};

use crate::rule_model::error::ProgramConstructionError;

use super::{atom::Atom, term::operation::Operation, ProgramComponent};

/// A literal that can either be a positive or negative atom or an operation
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
    fn fmt(&self, _f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}

impl ProgramComponent for Literal {
    fn parse(_string: &str) -> Result<Self, ProgramConstructionError>
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

    fn validate(&self) -> Result<(), ProgramConstructionError>
    where
        Self: Sized,
    {
        todo!()
    }
}
