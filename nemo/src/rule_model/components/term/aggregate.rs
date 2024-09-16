//! This module defines [Aggregate].
#![allow(missing_docs)]

use std::{fmt::Display, hash::Hash};

use enum_assoc::Assoc;
use nemo_physical::aggregates::operation::AggregateOperation;
use strum_macros::EnumIter;

use crate::{
    parse_component,
    parser::ast::ProgramAST,
    rule_model::{
        components::{
            parse::ComponentParseError, IterablePrimitives, IterableVariables, ProgramComponent,
            ProgramComponentKind,
        },
        error::{validation_error::ValidationErrorKind, ValidationErrorBuilder},
        origin::Origin,
        translation::ASTProgramTranslation,
    },
    syntax::builtin::aggregate,
};

use super::{
    primitive::{variable::Variable, Primitive},
    value_type::ValueType,
    Term,
};

/// Aggregate operation on logical values
#[derive(Assoc, EnumIter, Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[func(pub fn name(&self) -> &'static str)]
#[func(pub fn value_type(&self) -> ValueType)]
#[func(pub fn input_type(&self) -> Option<ValueType>)]
pub enum AggregateKind {
    /// Count of distinct values
    #[assoc(name = aggregate::COUNT)]
    #[assoc(value_type = ValueType::Number)]
    CountValues,
    /// Minimum numerical value
    #[assoc(name = aggregate::MIN)]
    #[assoc(value_type = ValueType::Number)]
    #[assoc(input_type = ValueType::Number)]
    MinNumber,
    /// Maximum numerical value
    #[assoc(name = aggregate::MAX)]
    #[assoc(value_type = ValueType::Number)]
    #[assoc(input_type = ValueType::Number)]
    #[assoc(physical = AggregateOperation::Max)]
    MaxNumber,
    /// Sum of numerical values
    #[assoc(name = aggregate::SUM)]
    #[assoc(value_type = ValueType::Number)]
    #[assoc(input_type = ValueType::Number)]
    SumOfNumbers,
}

impl Display for AggregateKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("#{}", self.name()))
    }
}

impl Into<AggregateOperation> for AggregateKind {
    fn into(self) -> AggregateOperation {
        match self {
            AggregateKind::CountValues => AggregateOperation::Count,
            AggregateKind::MinNumber => AggregateOperation::Min,
            AggregateKind::MaxNumber => AggregateOperation::Max,
            AggregateKind::SumOfNumbers => AggregateOperation::Sum,
        }
    }
}

/// Aggregate
///
/// Function that performs a computation over a set of [Term]s
/// and returns a single value.
#[derive(Debug, Clone, Eq)]
pub struct Aggregate {
    /// Origin of this component
    origin: Origin,

    /// Type of aggregate operation
    kind: AggregateKind,
    /// Expression over which to aggregate
    aggregate: Box<Term>,
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
            aggregate: Box::new(aggregate),
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

    /// Return the value type of this term.
    pub fn value_type(&self) -> ValueType {
        self.kind.value_type()
    }

    /// Return a reference to aggregated term.
    pub fn aggregate_term(&self) -> &Term {
        &self.aggregate
    }

    /// Return the kind of aggregate.
    pub fn aggregate_kind(&self) -> AggregateKind {
        self.kind
    }

    /// Return a iterator over the distinct variables
    pub fn distinct(&self) -> impl Iterator<Item = &Variable> {
        self.distinct.iter()
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

impl PartialOrd for Aggregate {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        match self.kind.partial_cmp(&other.kind) {
            Some(core::cmp::Ordering::Equal) => {}
            ord => return ord,
        }
        match self.aggregate.partial_cmp(&other.aggregate) {
            Some(core::cmp::Ordering::Equal) => {}
            ord => return ord,
        }
        self.distinct.partial_cmp(&other.distinct)
    }
}

impl ProgramComponent for Aggregate {
    fn parse(string: &str) -> Result<Self, ComponentParseError>
    where
        Self: Sized,
    {
        parse_component!(
            string,
            crate::parser::ast::expression::complex::aggregation::Aggregation::parse,
            ASTProgramTranslation::build_aggregation
        )
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

    fn validate(&self, builder: &mut ValidationErrorBuilder) -> Result<(), ()>
    where
        Self: Sized,
    {
        let input_type = self.aggregate.value_type();
        if let Some(expected_type) = self.kind.input_type() {
            if input_type != expected_type {
                builder.report_error(
                    self.aggregate.origin().clone(),
                    ValidationErrorKind::AggregateInvalidValueType {
                        found: input_type.name().to_string(),
                        expected: expected_type.name().to_string(),
                    },
                );

                return Err(());
            }
        }

        Ok(())
    }

    fn kind(&self) -> ProgramComponentKind {
        ProgramComponentKind::Aggregation
    }
}

impl IterableVariables for Aggregate {
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

impl IterablePrimitives for Aggregate {
    fn primitive_terms<'a>(&'a self) -> Box<dyn Iterator<Item = &'a Primitive> + 'a> {
        self.aggregate.primitive_terms()
    }

    fn primitive_terms_mut<'a>(&'a mut self) -> Box<dyn Iterator<Item = &'a mut Primitive> + 'a> {
        self.aggregate.primitive_terms_mut()
    }
}
