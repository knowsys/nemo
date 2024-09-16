//! This module defines [Literal]

use std::{fmt::Display, hash::Hash};

use crate::{
    parse_component,
    rule_model::{error::ValidationErrorBuilder, translation::ASTProgramTranslation},
};

use super::{
    atom::Atom,
    parse::ComponentParseError,
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
    fn parse(string: &str) -> Result<Self, ComponentParseError>
    where
        Self: Sized,
    {
        parse_component!(
            string,
            crate::parser::ast::expression::Expression::parse_complex,
            ASTProgramTranslation::build_body_literal
        )
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

    fn validate(&self, builder: &mut ValidationErrorBuilder) -> Result<(), ()>
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
