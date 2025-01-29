//! This module deifnes [Order].

use std::{fmt::Display, hash::Hash};

use crate::rule_model::{error::ValidationErrorBuilder, origin::Origin};

use super::{
    atom::Atom, tag::Tag, term::operation::Operation, ProgramComponent, ProgramComponentKind,
};

/// Order directive
///
/// Defines an order between tuples of a predicate.
#[derive(Debug, Clone)]
pub struct Order {
    /// Origin of this component
    origin: Origin,

    /// Predicate on which the order is defined
    predicate: Tag,

    /// [Atom] that is dominating
    dominating: Atom,
    /// [Atom] that is dominated
    dominated: Atom,
    /// List of [Operation]s that defines the condition when `dominating` is preferred over `dominated`
    condition: Vec<Operation>,
}

impl Order {
    /// Create a mew [Order].
    pub fn new(
        predicate: Tag,
        dominating: Atom,
        dominated: Atom,
        condition: Vec<Operation>,
    ) -> Self {
        Self {
            origin: Origin::default(),
            predicate,
            dominating,
            dominated,
            condition,
        }
    }

    /// Return the output predicate.
    pub fn predicate(&self) -> &Tag {
        &self.predicate
    }

    /// Return the [Operation] defining the order.
    pub fn condition(&self) -> &[Operation] {
        &self.condition
    }

    /// Return the [Atom] that is dominating.
    pub fn dominating(&self) -> &Atom {
        &self.dominating
    }

    /// Return the [Atom] that is dominated.
    pub fn dominated(&self) -> &Atom {
        &self.dominated
    }
}

impl Display for Order {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // write!(f, "@order {} .", self.predicate)
        todo!()
    }
}

impl PartialEq for Order {
    fn eq(&self, other: &Self) -> bool {
        self.predicate == other.predicate
            && self.condition == other.condition
            && self.dominated == other.dominated
            && self.dominating == other.dominating
    }
}

impl Hash for Order {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.predicate.hash(state);
        self.dominating.hash(state);
        self.dominated.hash(state);
        self.condition.hash(state);
    }
}

impl ProgramComponent for Order {
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

    fn validate(&self, _builder: &mut ValidationErrorBuilder) -> Option<()>
    where
        Self: Sized,
    {
        Some(())
    }

    fn kind(&self) -> ProgramComponentKind {
        ProgramComponentKind::Order
    }
}
