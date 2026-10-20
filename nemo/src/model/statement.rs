//! Top-level statements.

use std::fmt::{self, Display, Formatter};

use crate::model::{Directive, Fact, Rule, Span};

/// A top-level statement of a [Program](crate::model::Program).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Statement {
    /// A fact.
    Fact(Fact),
    /// A rule.
    Rule(Rule),
    /// A directive.
    Directive(Directive),
}

impl Statement {
    /// Returns the statement's source position.
    pub fn span(&self) -> Option<Span> {
        match self {
            Statement::Fact(fact) => fact.span,
            Statement::Rule(rule) => rule.span,
            Statement::Directive(directive) => directive.span(),
        }
    }

    /// Sets the statement's source position.
    pub fn set_span(&mut self, span: Option<Span>) {
        match self {
            Statement::Fact(fact) => fact.span = span,
            Statement::Rule(rule) => rule.span = span,
            Statement::Directive(directive) => directive.set_span(span),
        }
    }
}

impl Display for Statement {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Statement::Fact(fact) => write!(f, "{fact}"),
            Statement::Rule(rule) => write!(f, "{rule}"),
            Statement::Directive(directive) => write!(f, "{directive}"),
        }
    }
}

impl From<Fact> for Statement {
    fn from(fact: Fact) -> Self {
        Statement::Fact(fact)
    }
}

impl From<Rule> for Statement {
    fn from(rule: Rule) -> Self {
        Statement::Rule(rule)
    }
}

impl From<Directive> for Statement {
    fn from(directive: Directive) -> Self {
        Statement::Directive(directive)
    }
}
