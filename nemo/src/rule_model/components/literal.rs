//! This module defines [Literal].

use std::{fmt::Display, hash::Hash};

use crate::rule_model::{
    error::ValidationErrorBuilder, origin::Origin, pipeline::id::ProgramComponentId,
};

use super::{
    atom::Atom,
    tag::Tag,
    term::{
        operation::Operation,
        primitive::{variable::Variable, Primitive},
        Term,
    },
    ComponentBehavior, ComponentIdentity, IterableComponent, IterablePrimitives, IterableVariables,
    ProgramComponent, ProgramComponentKind,
};

/// Literal
///
/// An [Atom], its negation, or an [Operation].
/// Literals are used to represent conditions that must be satisfied
/// for a rule to be applicable.
#[derive(Debug, Clone, PartialEq, Hash)]
pub enum Literal {
    /// Positive atom
    Positive(Atom),
    /// Negative atom
    Negative(Atom),
    /// Operation
    Operation(Operation),
}

impl Literal {
    /// Return an iterator over the terms contained in this literal.
    pub fn terms(&self) -> Box<dyn Iterator<Item = &Term> + '_> {
        match self {
            Literal::Positive(literal) => Box::new(literal.terms()),
            Literal::Negative(literal) => Box::new(literal.terms()),
            Literal::Operation(literal) => Box::new(literal.arguments()),
        }
    }

    /// If literal is not an operation, return the predicate.
    /// Returns `None` otherwise.
    pub fn predicate(&self) -> Option<Tag> {
        match self {
            Literal::Positive(atom) => Some(atom.predicate()),
            Literal::Negative(atom) => Some(atom.predicate()),
            Literal::Operation(_) => None,
        }
    }
}

impl Display for Literal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Literal::Positive(positive) => write!(f, "{positive}"),
            Literal::Negative(negative) => write!(f, "~{negative}"),
            Literal::Operation(operation) => write!(f, "{operation}"),
        }
    }
}

impl ComponentBehavior for Literal {
    fn kind(&self) -> ProgramComponentKind {
        ProgramComponentKind::Literal
    }

    fn validate(&self) -> Result<(), ValidationReport> {
        match self {
            Literal::Positive(atom) => atom.validate(builder),
            Literal::Negative(atom) => atom.validate(builder),
            Literal::Operation(operation) => operation.validate(builder),
        }
    }

    fn boxed_clone(&self) -> Box<dyn ProgramComponent> {
        match self {
            Literal::Positive(atom) => atom.boxed_clone(),
            Literal::Negative(atom) => atom.boxed_clone(),
            Literal::Operation(operation) => operation.boxed_clone(),
        }
    }
}

impl ComponentIdentity for Literal {
    fn id(&self) -> ProgramComponentId {
        match self {
            Literal::Positive(atom) => atom.id(),
            Literal::Negative(atom) => atom.id(),
            Literal::Operation(operation) => operation.id(),
        }
    }

    fn set_id(&mut self, id: ProgramComponentId) {
        match self {
            Literal::Positive(atom) => atom.set_id(id),
            Literal::Negative(atom) => atom.set_id(id),
            Literal::Operation(operation) => operation.set_id(id),
        }
    }

    fn origin(&self) -> &Origin {
        match self {
            Literal::Positive(atom) => atom.origin(),
            Literal::Negative(atom) => atom.origin(),
            Literal::Operation(operation) => operation.origin(),
        }
    }

    fn set_origin(&mut self, origin: Origin) {
        match self {
            Literal::Positive(atom) => atom.set_origin(origin),
            Literal::Negative(atom) => atom.set_origin(origin),
            Literal::Operation(operation) => operation.set_origin(origin),
        }
    }
}

impl IterableComponent for Literal {
    fn children<'a>(&'a self) -> Box<dyn Iterator<Item = &'a dyn ProgramComponent> + 'a> {
        match self {
            Literal::Positive(atom) => atom.children(),
            Literal::Negative(atom) => atom.children(),
            Literal::Operation(operation) => operation.children(),
        }
    }

    fn children_mut<'a>(
        &'a mut self,
    ) -> Box<dyn Iterator<Item = &'a mut dyn ProgramComponent> + 'a> {
        match self {
            Literal::Positive(atom) => atom.children_mut(),
            Literal::Negative(atom) => atom.children_mut(),
            Literal::Operation(operation) => operation.children_mut(),
        }
    }
}

impl IterableVariables for Literal {
    fn variables<'a>(&'a self) -> Box<dyn Iterator<Item = &'a Variable> + 'a> {
        match self {
            Literal::Positive(literal) => literal.variables(),
            Literal::Negative(literal) => literal.variables(),
            Literal::Operation(literal) => literal.variables(),
        }
    }

    fn variables_mut<'a>(&'a mut self) -> Box<dyn Iterator<Item = &'a mut Variable> + 'a> {
        match self {
            Literal::Positive(literal) => literal.variables_mut(),
            Literal::Negative(literal) => literal.variables_mut(),
            Literal::Operation(literal) => literal.variables_mut(),
        }
    }
}

impl IterablePrimitives for Literal {
    fn primitive_terms<'a>(&'a self) -> Box<dyn Iterator<Item = &'a Primitive> + 'a> {
        match self {
            Literal::Positive(literal) => literal.primitive_terms(),
            Literal::Negative(literal) => literal.primitive_terms(),
            Literal::Operation(literal) => literal.primitive_terms(),
        }
    }

    fn primitive_terms_mut<'a>(&'a mut self) -> Box<dyn Iterator<Item = &'a mut Term> + 'a> {
        match self {
            Literal::Positive(literal) => literal.primitive_terms_mut(),
            Literal::Negative(literal) => literal.primitive_terms_mut(),
            Literal::Operation(literal) => literal.primitive_terms_mut(),
        }
    }
}
