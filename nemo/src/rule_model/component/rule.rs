//! This module defines [Rule] and [RuleBuilder]

use std::{fmt::Display, hash::Hash};

use crate::rule_model::origin::Origin;

use super::{atom::Atom, literal::Literal, term::operation::Operation, ProgramComponent};

/// A rule
#[derive(Debug, Clone, Eq)]
pub struct Rule {
    /// Origin of this component
    origin: Origin,

    /// Name of the rule
    name: Option<String>,

    /// Head of the rule
    head: Vec<Atom>,
    /// Body of the rule
    body: Vec<Literal>,
}

impl Rule {
    /// Create a new [Rule].
    pub fn new(head: Vec<Atom>, body: Vec<Literal>) -> Self {
        Self {
            origin: Origin::Created,
            name: None,
            head,
            body,
        }
    }

    /// Set the name of the rule.
    pub fn set_name(mut self, name: &str) -> Self {
        self.name = Some(name.to_string());
        self
    }
}

impl Display for Rule {
    fn fmt(&self, _f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}

impl PartialEq for Rule {
    fn eq(&self, other: &Self) -> bool {
        self.head == other.head && self.body == other.body
    }
}

impl Hash for Rule {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.head.hash(state);
        self.body.hash(state);
    }
}

impl ProgramComponent for Rule {
    fn parse(_string: &str) -> Result<Self, crate::rule_model::error::ProgramConstructionError>
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

    fn validate(&self) -> Result<(), crate::rule_model::error::ProgramConstructionError>
    where
        Self: Sized,
    {
        todo!()
    }
}

/// Builder for a rule
#[derive(Debug, Default)]
pub struct RuleBuilder {
    /// Origin of the rule
    origin: Origin,

    /// Builder for the head of the rule
    head: RuleHeadBuilder,
    /// Builder for the body of the rule
    body: RuleBodyBuilder,
}

impl RuleBuilder {
    /// Set the [Origin] of the built rule.
    pub fn origin(mut self, origin: Origin) -> Self {
        self.origin = origin;
        self
    }

    /// Return a builder for the body of the rule.
    pub fn body(self) -> RuleBodySubBuilder {
        RuleBodySubBuilder { builder: self }
    }

    /// Return a builder for the head of the rule.
    pub fn head(self) -> RuleHeadSubBuilder {
        RuleHeadSubBuilder { builder: self }
    }

    /// Finish building and return a [Rule].
    pub fn finalize(self) -> Rule {
        Rule::new(self.head.finalize(), self.body.finalize()).set_origin(self.origin)
    }
}

/// Builder for the rule body
#[derive(Debug, Default)]
pub struct RuleBodyBuilder {
    /// Current list of [Literal]s
    literals: Vec<Literal>,
}

impl RuleBodyBuilder {
    /// Add a positive atom to the body of the rule.
    pub fn add_positive_atom(mut self, atom: Atom) -> Self {
        self.literals.push(Literal::Positive(atom));
        self
    }

    /// Add a negative atom to the body of the rule.
    pub fn add_negative_atom(mut self, atom: Atom) -> Self {
        self.literals.push(Literal::Negative(atom));
        self
    }

    /// Add an operation to the body of the rule.
    pub fn add_operation(mut self, opreation: Operation) -> Self {
        self.literals.push(Literal::Operation(opreation));
        self
    }

    /// Finish building and return a list of [Literal]s.
    pub fn finalize(self) -> Vec<Literal> {
        self.literals
    }
}

/// Subbuilder for building the body of a rule
#[derive(Debug)]
pub struct RuleBodySubBuilder {
    builder: RuleBuilder,
}

impl RuleBodySubBuilder {
    /// Add a positive atom to the body of the rule.
    pub fn add_positive_atom(mut self, atom: Atom) -> Self {
        self.builder.body = self.builder.body.add_positive_atom(atom);
        self
    }

    /// Add a negative atom to the body of the rule.
    pub fn add_negative_atom(mut self, atom: Atom) -> Self {
        self.builder.body = self.builder.body.add_negative_atom(atom);
        self
    }

    /// Add an operation to the body of the rule.
    pub fn add_operation(mut self, opreation: Operation) -> Self {
        self.builder.body = self.builder.body.add_operation(opreation);
        self
    }

    /// Return to the [RuleBuilder]
    pub fn done(self) -> RuleBuilder {
        self.builder
    }
}

/// Builder for the rule head
#[derive(Debug, Default)]
pub struct RuleHeadBuilder {
    /// Current list of [Atom]s
    atoms: Vec<Atom>,
}

impl RuleHeadBuilder {
    /// Add another atom to the head of the rule.
    pub fn add_atom(mut self, atom: Atom) -> Self {
        self.atoms.push(atom);
        self
    }

    /// Finish building and return a list of [Atom]s.
    pub fn finalize(self) -> Vec<Atom> {
        self.atoms
    }
}

/// Subbuilder for building the head of a rule
#[derive(Debug)]
pub struct RuleHeadSubBuilder {
    builder: RuleBuilder,
}

impl RuleHeadSubBuilder {
    /// Add another atom to the head of the rule.
    pub fn add_atom(mut self, atom: Atom) -> Self {
        self.builder.head = self.builder.head.add_atom(atom);
        self
    }

    /// Return to the [RuleBuilder]
    pub fn done(self) -> RuleBuilder {
        self.builder
    }
}
