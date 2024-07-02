//! This module defines [Aggregate]

use std::{fmt::Display, hash::Hash};

use crate::rule_model::{component::ProgramComponent, origin::Origin};

use super::{primitive::variable::Variable, Term};

/// Aggregate operation on logical values
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum AggregateKind {
    /// Count of distinct values
    CountValues,
    /// Minimum numerical value
    MinNumber,
    /// Maximum numerical value
    MaxNumber,
    /// Sum of numerical values
    SumOfNumbers,
}

/// An aggregate
#[derive(Debug, Clone, Eq)]
pub struct Aggregate {
    /// Origin of this component
    origin: Origin,

    /// Type of aggrgate operation
    kind: AggregateKind,
    /// Expression over which to aggragte
    aggregate: Term,
    /// Distinct variables
    distinct: Vec<Variable>,
}

impl Aggregate {
    /// Create a new [Aggregate].
    pub fn new(kind: AggregateKind, aggregate: Term, distinct: Vec<Variable>) -> Self {
        Self {
            origin: Origin::default(),
            kind,
            aggregate,
            distinct,
        }
    }
}

impl Display for Aggregate {
    fn fmt(&self, _f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}

impl PartialEq for Aggregate {
    fn eq(&self, other: &Self) -> bool {
        self.kind == other.kind
            && self.aggregate == other.aggregate
            && self.distinct == other.distinct
    }
}

impl Hash for Aggregate {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.kind.hash(state);
        self.aggregate.hash(state);
        self.distinct.hash(state);
    }
}

impl ProgramComponent for Aggregate {
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
