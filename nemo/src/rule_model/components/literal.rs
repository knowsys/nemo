//! This module defines [Literal]

use std::{fmt::Display, hash::Hash};

use crate::rule_model::error::ValidationErrorBuilder;

use super::{
    atom::Atom,
    tag::Tag,
    term::{
        operation::Operation,
        primitive::{variable::Variable, Primitive},
        Term,
    },
    IterablePrimitives, IterableVariables, ProgramComponent, ProgramComponentKind,
};

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

impl Literal {
    /// Return an iterator over the arguments contained in this literal.
    pub fn arguments(&self) -> Box<dyn Iterator<Item = &Term> + '_> {
        match self {
            Literal::Positive(literal) => Box::new(literal.arguments()),
            Literal::Negative(literal) => Box::new(literal.arguments()),
            Literal::Operation(literal) => Box::new(literal.arguments()),
        }
    }

    /// Return the predicate (positive Atom | negative Atom) or None (Operation) of the
    /// Literal
    pub fn predicate(&self) -> Option<Tag> {
        match self {
            Literal::Positive(atom) | Literal::Negative(atom) => Some(atom.predicate()),
            Literal::Operation(_) => None,
        }
    }

    /// Return the predicate (positive Atom | negative Atom) or None (Operation) of the
    /// Literal as a reference
    pub fn predicate_ref(&self) -> Option<&Tag> {
        match self {
            Literal::Positive(atom) | Literal::Negative(atom) => Some(atom.predicate_ref()),
            Literal::Operation(_) => None,
        }
    }

    /// Return the predicate as a reference and its length (positive Atom | negative Atom) or None (Operation) of the
    /// Literal
    pub fn predicate_ref_and_len(&self) -> Option<(&Tag, usize)> {
        match self {
            Literal::Positive(atom) | Literal::Negative(atom) => Some(atom.predicate_ref_and_len()),
            Literal::Operation(_) => None,
        }
    }

    // / Return the number of subterms in this literal if its a (positive Atom | negative Atom) or
    // / None (Operation).
    // pub fn len(&self) -> Option<usize> {
    //     match self {
    //         Literal::Positive(atom) | Literal::Negative(atom) => Some(atom.len()),
    //         Literal::Operation(_) => None,
    //     }
    // }
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

    fn validate(&self, builder: &mut ValidationErrorBuilder) -> Option<()>
    where
        Self: Sized,
    {
        match self {
            Literal::Positive(literal) => literal.validate(builder),
            Literal::Negative(literal) => literal.validate(builder),
            Literal::Operation(literal) => literal.validate(builder),
        }
    }

    fn kind(&self) -> ProgramComponentKind {
        ProgramComponentKind::Literal
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

    fn primitive_terms_mut<'a>(&'a mut self) -> Box<dyn Iterator<Item = &'a mut Primitive> + 'a> {
        match self {
            Literal::Positive(literal) => literal.primitive_terms_mut(),
            Literal::Negative(literal) => literal.primitive_terms_mut(),
            Literal::Operation(literal) => literal.primitive_terms_mut(),
        }
    }
}
