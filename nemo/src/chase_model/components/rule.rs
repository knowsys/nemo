//! This module defines [ChaseRule].

use crate::rule_model::origin::Origin;

use super::{
    aggregate::ChaseAggregate,
    atom::{primitive_atom::PrimitiveAtom, variable_atom::VariableAtom},
    filter::ChaseFilter,
    operation::ChaseOperation,
    ChaseComponent,
};

/// The positive body of a [ChaseRule]
#[derive(Debug, Default)]
struct ChaseRuleBodyPositive {
    /// Atoms that bind variables
    atoms: Vec<VariableAtom>,
    /// Computation of new bindings
    operations: Vec<ChaseOperation>,
    /// Filtering of results
    filters: Vec<ChaseFilter>,
}

/// The negative body of a [ChaseRule]
#[derive(Debug, Default)]
struct ChaseRuleBodyNegative {
    /// Negated atoms
    atoms: Vec<VariableAtom>,
    /// For each negated atom, the filters that are applied
    filters: Vec<Vec<ChaseFilter>>,
}

/// Handling of aggregation within a [ChaseRule]
#[derive(Debug, Default)]
struct ChaseRuleAggregation {
    /// Aggregate
    aggregate: Option<ChaseAggregate>,

    /// New values created from the aggregation result
    operations: Vec<ChaseOperation>,
    /// Filters based on the aggregation result
    filters: Vec<ChaseFilter>,
}

/// Head of a [ChaseRule]
#[derive(Debug, Default)]
struct ChaseRuleHead {
    /// Head atoms of the rule
    atoms: Vec<PrimitiveAtom>,
    /// Index of the head atom which contains the aggregate
    aggregate_head_index: Option<usize>,
}

/// Representation of a rule in a [ChaseProgram][super::program::ChaseProgram]
#[allow(dead_code)]
#[derive(Debug, Default)]
pub(crate) struct ChaseRule {
    /// Origin of this component
    origin: Origin,

    /// Positive part of the body
    positive: ChaseRuleBodyPositive,
    /// Negative part of the body
    negative: ChaseRuleBodyNegative,
    /// Aggregation
    aggregation: ChaseRuleAggregation,
    /// Head of the rule
    head: ChaseRuleHead,
}

impl ChaseRule {
    /// Add an atom to the positive part of the body.
    pub(crate) fn add_positive_atom(&mut self, atom: VariableAtom) {
        self.positive.atoms.push(atom);
    }

    /// Add an operation for the positive part of the body.
    pub(crate) fn add_positive_operation(&mut self, operation: ChaseOperation) {
        self.positive.operations.push(operation)
    }

    /// Add a filter to the positive part of the body.
    pub(crate) fn add_positive_filter(&mut self, filter: ChaseFilter) {
        self.positive.filters.push(filter);
    }

    /// Add an atom to the negative part of the body.
    pub(crate) fn add_negative_atom(&mut self, atom: VariableAtom) {
        self.negative.atoms.push(atom);
        self.negative.filters.push(Vec::default())
    }

    /// Add a filter to the negative part of the body.
    pub(crate) fn add_negative_filter(&mut self, atom_index: usize, filter: ChaseFilter) {
        self.negative.filters[atom_index].push(filter)
    }

    /// Add a filter to the negative part of the body.
    ///
    /// # Panics
    /// Panics if the current filter vector is empty.
    pub(crate) fn add_negative_filter_last(&mut self, filter: ChaseFilter) {
        self.negative
            .filters
            .last_mut()
            .expect("expected a filter slot")
            .push(filter)
    }

    /// Add a new aggregation operation to the rule.
    pub(crate) fn add_aggregation(&mut self, aggregate: ChaseAggregate, head_index: usize) {
        self.aggregation.aggregate = Some(aggregate);
        self.head.aggregate_head_index = Some(head_index);
    }

    /// Add a new operation that uses the result of aggregation.
    pub(crate) fn add_aggregation_operation(&mut self, operation: ChaseOperation) {
        self.aggregation.operations.push(operation);
    }

    /// Add a new filter that uses the result of aggregation.
    pub(crate) fn add_aggregation_filter(&mut self, filter: ChaseFilter) {
        self.aggregation.filters.push(filter);
    }

    /// Add a new atom to the head of the rule.
    pub(crate) fn add_head_atom(&mut self, atom: PrimitiveAtom) {
        self.head.atoms.push(atom)
    }
}

impl ChaseComponent for ChaseRule {
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
}
