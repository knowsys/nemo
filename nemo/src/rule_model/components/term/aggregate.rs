//! This module defines [Aggregate].
#![allow(missing_docs)]

use std::{
    collections::HashSet,
    fmt::{Display, Write},
    hash::Hash,
};

use enum_assoc::Assoc;
use nemo_physical::aggregates::operation::AggregateOperation;
use strum_macros::EnumIter;

use crate::{
    rule_model::{
        components::{
            component_iterator, component_iterator_mut, ComponentBehavior, ComponentIdentity,
            ComponentSource, IterableComponent, IterablePrimitives, IterableVariables,
            ProgramComponent, ProgramComponentKind,
        },
        error::{info::Info, validation_error::ValidationError, ValidationReport},
        origin::Origin,
        pipeline::id::ProgramComponentId,
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

impl From<AggregateKind> for AggregateOperation {
    fn from(value: AggregateKind) -> Self {
        match value {
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
#[derive(Debug, Clone)]
pub struct Aggregate {
    /// Origin of this component
    origin: Origin,
    /// Id of this component
    id: ProgramComponentId,

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
            id: ProgramComponentId::default(),
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

    /// Return a reference to the aggregated term.
    pub fn aggregate_term(&self) -> &Term {
        &self.aggregate
    }

    /// Return a mutable reference to the aggregated term.
    pub fn aggregate_term_mut(&mut self) -> &mut Term {
        &mut self.aggregate
    }

    /// Return the kind of aggregate.
    pub fn aggregate_kind(&self) -> AggregateKind {
        self.kind
    }

    /// Return a iterator over the distinct variables.
    pub fn distinct(&self) -> impl Iterator<Item = &Variable> {
        self.distinct.iter()
    }

    /// Return a mutable iterator over the distinct variables.
    pub fn distinct_mut(&mut self) -> impl Iterator<Item = &mut Variable> {
        self.distinct.iter_mut()
    }

    /// Add a new distinct variable.
    pub fn push_distinct(&mut self, variable: Variable) {
        self.distinct.push(variable);
    }

    /// Remove a distinct variable at the given index
    /// and return it.
    ///
    /// # Panics
    /// Panics if the index is out of bounds.
    pub fn remove_distinct(&mut self, index: usize) -> Variable {
        self.distinct.remove(index)
    }

    /// Return whether the aggregate expression is ground.
    pub fn is_ground(&self) -> bool {
        self.aggregate.is_ground()
    }

    /// Reduce this term by evaluating all contained expressions,
    /// and return a new [Aggregate] with the same [Origin] as `self`.
    ///
    /// This function does nothing if `self` is not ground.
    ///
    /// Returns `None` if any intermediate result is undefined.
    pub fn reduce(&self) -> Option<Self> {
        Some(Self {
            origin: self.origin.clone(),
            id: ProgramComponentId::default(),
            kind: self.kind,
            aggregate: Box::new(self.aggregate.reduce()?),
            distinct: self.distinct.clone(),
        })
    }

    /// Check wether this term can be reduced to a ground value,
    /// except for global variables that need to be resolved.
    ///
    /// This is the case if
    ///     * This term does not contain non-global variables.
    ///     * This term does not contain undefined intermediate values.
    pub fn is_resolvable(&self) -> bool {
        self.aggregate.is_resolvable()
    }
}

impl Display for Aggregate {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("{}({}", self.kind, self.aggregate))?;

        if !self.distinct.is_empty() {
            f.write_char(',')?;
        }

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

impl ComponentBehavior for Aggregate {
    fn kind(&self) -> ProgramComponentKind {
        ProgramComponentKind::Aggregation
    }

    fn validate(&self) -> Result<(), ValidationReport> {
        let mut report = ValidationReport::default();

        for child in self.children() {
            report.merge(child.validate());
        }

        let input_type = self.aggregate.value_type();
        if let Some(expected_type) = self.kind.input_type() {
            if input_type != ValueType::Any && input_type != expected_type {
                report.add(
                    &*self.aggregate,
                    ValidationError::AggregateInvalidValueType {
                        found: input_type.name().to_string(),
                        expected: expected_type.name().to_string(),
                    },
                );
            }
        }

        let mut distinct_set = if self.aggregate.is_primitive() {
            self.aggregate.variables().collect()
        } else {
            HashSet::new()
        };

        for variable in &self.distinct {
            if !variable.is_universal() {
                let variable_type = match variable {
                    Variable::Universal(_) => "",
                    Variable::Existential(_) => "existential",
                    Variable::Global(_) => "global",
                }
                .to_owned();

                report.add(
                    variable,
                    ValidationError::AggregateDistinctNonNamedUniversal { variable_type },
                );
            }

            if !variable.is_anonymous() {
                if let Some(found) = distinct_set.get(variable) {
                    report
                        .add(
                            variable,
                            ValidationError::AggregateRepeatedDistinctVariable {
                                variable: Box::new(variable.clone()),
                            },
                        )
                        .add_context(*found, Info::FirstDefinition);
                } else {
                    distinct_set.insert(variable);
                }
            } else {
                report.add(
                    variable,
                    ValidationError::AggregateDistinctNonNamedUniversal {
                        variable_type: String::from("anonymous"),
                    },
                );
            };
        }

        report.result()
    }

    fn boxed_clone(&self) -> Box<dyn ProgramComponent> {
        Box::new(self.clone())
    }
}

impl ComponentSource for Aggregate {
    type Source = Origin;

    fn origin(&self) -> Origin {
        self.origin.clone()
    }

    fn set_origin(&mut self, origin: Origin) {
        self.origin = origin;
    }
}

impl ComponentIdentity for Aggregate {
    fn id(&self) -> ProgramComponentId {
        self.id
    }

    fn set_id(&mut self, id: ProgramComponentId) {
        self.id = id;
    }
}

impl IterableComponent for Aggregate {
    fn children<'a>(&'a self) -> Box<dyn Iterator<Item = &'a dyn ProgramComponent> + 'a> {
        let aggregate_iter = component_iterator(std::iter::once(&*self.aggregate));
        let distinct_iter = component_iterator(self.distinct());

        Box::new(aggregate_iter.chain(distinct_iter))
    }

    fn children_mut<'a>(
        &'a mut self,
    ) -> Box<dyn Iterator<Item = &'a mut dyn ProgramComponent> + 'a> {
        let aggregate_iter = component_iterator_mut(std::iter::once(&mut *self.aggregate));
        let distinct_iter = component_iterator_mut(self.distinct.iter_mut());

        Box::new(aggregate_iter.chain(distinct_iter))
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

    fn primitive_terms_mut<'a>(&'a mut self) -> Box<dyn Iterator<Item = &'a mut Term> + 'a> {
        self.aggregate.primitive_terms_mut()
    }
}

#[cfg(test)]
mod test {
    use crate::rule_model::{
        components::term::{aggregate::AggregateKind, primitive::variable::Variable, Term},
        translation::TranslationComponent,
    };

    use super::Aggregate;

    #[test]
    fn parse_aggregate() {
        let aggregate = Aggregate::parse("#sum(?x, ?y)").unwrap();

        assert_eq!(
            Aggregate::new(
                AggregateKind::SumOfNumbers,
                Term::from(Variable::universal("x")),
                vec![Variable::universal("y")]
            ),
            aggregate
        );
    }
}
