//! This module defines [Aggregate]

use std::{fmt::Display, hash::Hash};

use crate::rule_model::{
    component::{IteratableVariables, ProgramComponent},
    origin::Origin,
};

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

impl Display for AggregateKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let name = match self {
            AggregateKind::CountValues => "count",
            AggregateKind::MinNumber => "min",
            AggregateKind::MaxNumber => "max",
            AggregateKind::SumOfNumbers => "sum",
        };

        f.write_fmt(format_args!("#{}", name))
    }
}

/// Aggregate
///
/// Function that performs a computatin over a set of [Term]s
/// and returns a single value.
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
    pub fn new<Variables: IntoIterator<Item = Variable>>(
        kind: AggregateKind,
        aggregate: Term,
        distinct: Variables,
    ) -> Self {
        Self {
            origin: Origin::default(),
            kind,
            aggregate,
            distinct: distinct.into_iter().collect(),
        }
    }

    /// Create a new sum [Aggregate].
    pub fn sum<Variables: IntoIterator<Item = Variable>>(
        aggregate: Term,
        distinct: Variables,
    ) -> Self {
        Self::new(AggregateKind::SumOfNumbers, aggregate, distinct)
    }

    /// Create a new count [Aggregate].
    pub fn count<Variables: IntoIterator<Item = Variable>>(
        aggregate: Term,
        distinct: Variables,
    ) -> Self {
        Self::new(AggregateKind::CountValues, aggregate, distinct)
    }

    /// Create a new min [Aggregate].
    pub fn min<Variables: IntoIterator<Item = Variable>>(
        aggregate: Term,
        distinct: Variables,
    ) -> Self {
        Self::new(AggregateKind::MinNumber, aggregate, distinct)
    }

    /// Create a new sum [Aggregate].
    pub fn max<Variables: IntoIterator<Item = Variable>>(
        aggregate: Term,
        distinct: Variables,
    ) -> Self {
        Self::new(AggregateKind::MaxNumber, aggregate, distinct)
    }
}

impl Display for Aggregate {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("{}({}", self.kind, self.aggregate))?;

        for (distinct_index, variable) in self.distinct.iter().enumerate() {
            variable.fmt(f)?;

            if distinct_index < self.distinct.len() - 1 {
                f.write_str(", ")?;
            }
        }

        f.write_str(")")
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

impl IteratableVariables for Aggregate {
    fn variables<'a>(&'a self) -> Box<dyn Iterator<Item = &'a Variable> + 'a> {
        Box::new(self.aggregate.variables().chain(self.distinct.iter()))
    }

    fn variables_mut<'a>(&'a mut self) -> Box<dyn Iterator<Item = &'a mut Variable> + 'a> {
        Box::new(
            self.aggregate
                .variables_mut()
                .chain(self.distinct.iter_mut()),
        )
    }
}
