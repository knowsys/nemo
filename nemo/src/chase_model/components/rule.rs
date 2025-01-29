//! This module defines [ChaseRule].

use std::collections::HashMap;

use crate::rule_model::{
    components::{
        tag::Tag, term::primitive::variable::Variable, IterablePrimitives, IterableVariables,
    },
    origin::Origin,
};

use super::{
    aggregate::ChaseAggregate,
    atom::{primitive_atom::PrimitiveAtom, variable_atom::VariableAtom},
    filter::ChaseFilter,
    operation::ChaseOperation,
    order::ChaseOrder,
    ChaseComponent,
};

/// The positive body of a [ChaseRule]
#[derive(Debug, Default, Clone)]
struct ChaseRuleBodyPositive {
    /// Atoms that bind variables
    atoms: Vec<VariableAtom>,
    /// Computation of new bindings
    operations: Vec<ChaseOperation>,
    /// Filtering of results
    filters: Vec<ChaseFilter>,
}

/// The negative body of a [ChaseRule]
#[derive(Debug, Default, Clone)]
struct ChaseRuleBodyNegative {
    /// Negated atoms
    atoms: Vec<VariableAtom>,
    /// For each negated atom, the filters that are applied
    filters: Vec<Vec<ChaseFilter>>,
}

/// Handling of aggregation within a [ChaseRule]
#[derive(Debug, Default, Clone)]
struct ChaseRuleAggregation {
    /// Aggregate
    aggregate: Option<ChaseAggregate>,

    /// New values created from the aggregation result
    operations: Vec<ChaseOperation>,
    /// Filters based on the aggregation result
    filters: Vec<ChaseFilter>,
}

/// Head of a [ChaseRule]
#[derive(Debug, Default, Clone)]
struct ChaseRuleHead {
    /// Head atoms of the rule
    atoms: Vec<PrimitiveAtom>,
    /// Index of the head atom which contains the aggregate
    aggregate_head_index: Option<usize>,
}

/// Representation of a rule in a [ChaseProgram][super::program::ChaseProgram]
#[allow(dead_code)]
#[derive(Debug, Default, Clone)]
pub struct ChaseRule {
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
    /// Order of head predicates
    order: HashMap<Tag, ChaseOrder>,
}

impl ChaseRule {
    /// Create a simple positive rule.
    #[cfg(test)]
    pub(crate) fn positive_rule(
        head: Vec<PrimitiveAtom>,
        body: Vec<VariableAtom>,
        filters: Vec<ChaseFilter>,
    ) -> Self {
        let mut result = Self::default();
        result.head.atoms = head;
        result.positive.atoms = body;
        result.positive.filters = filters;

        result
    }
}

impl ChaseRule {
    /// Return the list of head atoms contained in this rule.
    pub(crate) fn head(&self) -> &Vec<PrimitiveAtom> {
        &self.head.atoms
    }

    /// Return the list of positive body atoms contained in this rule.
    pub(crate) fn positive_body(&self) -> &Vec<VariableAtom> {
        &self.positive.atoms
    }

    /// Return the list of filters that will be applied to the positive part of the body.
    pub(crate) fn positive_filters(&self) -> &Vec<ChaseFilter> {
        &self.positive.filters
    }

    /// Return the list of operations that will be applied to the positive part of the body.
    pub(crate) fn positive_operations(&self) -> &Vec<ChaseOperation> {
        &self.positive.operations
    }

    /// Return the list of negative body atoms contained in this rule.
    pub(crate) fn negative_body(&self) -> &Vec<VariableAtom> {
        &self.negative.atoms
    }

    /// Return the list of filters that will be applied to the negative part of the body.
    pub(crate) fn negative_filters(&self) -> &Vec<Vec<ChaseFilter>> {
        &self.negative.filters
    }

    /// Return the aggregation that will be evaluated during this rule's application.
    pub(crate) fn aggregate(&self) -> Option<&ChaseAggregate> {
        self.aggregation.aggregate.as_ref()
    }

    /// Return the list of filters that will be applied to the negative part of the body.
    pub(crate) fn aggregate_filters(&self) -> &Vec<ChaseFilter> {
        &self.aggregation.filters
    }

    /// Return the list of operations that will be applied to the result of the aggregation.
    pub(crate) fn aggregate_operations(&self) -> &Vec<ChaseOperation> {
        &self.aggregation.operations
    }

    /// Return the index of the head atom that contains the aggregation
    pub(crate) fn aggregate_head_index(&self) -> Option<usize> {
        self.head.aggregate_head_index
    }

    /// Return the predicate that need updating and their order
    pub(crate) fn orders(&self) -> &HashMap<Tag, ChaseOrder> {
        &self.order
    }
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
    pub(crate) fn _add_aggregation_filter(&mut self, filter: ChaseFilter) {
        self.aggregation.filters.push(filter);
    }

    /// Add a new atom to the head of the rule.
    pub(crate) fn add_head_atom(&mut self, atom: PrimitiveAtom) {
        self.head.atoms.push(atom)
    }

    /// Add an ordering to a predicate.
    pub(crate) fn add_order(&mut self, predicate: Tag, order: ChaseOrder) {
        self.order.insert(predicate, order);
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

impl IterableVariables for ChaseRule {
    fn variables<'a>(&'a self) -> Box<dyn Iterator<Item = &'a Variable> + 'a> {
        let head_variables = self.head().iter().flat_map(|atom| atom.variables());
        let positive_body_variables = self
            .positive_body()
            .iter()
            .flat_map(|atom| atom.variables());
        let positive_operation_variables = self
            .positive_operations()
            .iter()
            .flat_map(|operation| operation.variables());
        let positive_filter_variables = self
            .positive_filters()
            .iter()
            .flat_map(|filter| filter.variables());

        let negative_body_variables = self
            .negative_body()
            .iter()
            .flat_map(|atom| atom.variables());
        let negative_filter_variables = self
            .negative_filters()
            .iter()
            .flatten()
            .flat_map(|filter| filter.variables());

        let aggregation_variables = self
            .aggregate()
            .into_iter()
            .flat_map(|aggregate| aggregate.variables());
        let aggregation_operation_variables = self
            .aggregate_operations()
            .iter()
            .flat_map(|operation| operation.variables());
        let aggregation_filter_variables = self
            .aggregate_filters()
            .iter()
            .flat_map(|filter| filter.variables());

        Box::new(
            head_variables
                .chain(positive_body_variables)
                .chain(positive_operation_variables)
                .chain(positive_filter_variables)
                .chain(negative_body_variables)
                .chain(negative_filter_variables)
                .chain(aggregation_variables)
                .chain(aggregation_operation_variables)
                .chain(aggregation_filter_variables),
        )
    }

    fn variables_mut<'a>(&'a mut self) -> Box<dyn Iterator<Item = &'a mut Variable> + 'a> {
        let head_variables = self
            .head
            .atoms
            .iter_mut()
            .flat_map(|atom| atom.variables_mut());
        let positive_body_variables = self
            .positive
            .atoms
            .iter_mut()
            .flat_map(|atom| atom.variables_mut());
        let positive_operation_variables = self
            .positive
            .operations
            .iter_mut()
            .flat_map(|operation| operation.variables_mut());
        let positive_filter_variables = self
            .positive
            .filters
            .iter_mut()
            .flat_map(|filter| filter.variables_mut());

        let negative_body_variables = self
            .negative
            .atoms
            .iter_mut()
            .flat_map(|atom| atom.variables_mut());
        let negative_filter_variables = self
            .negative
            .filters
            .iter_mut()
            .flatten()
            .flat_map(|filter| filter.variables_mut());

        let aggregation_variables = self
            .aggregation
            .aggregate
            .as_mut()
            .into_iter()
            .flat_map(|aggregate| aggregate.variables_mut());
        let aggregation_operation_variables = self
            .aggregation
            .operations
            .iter_mut()
            .flat_map(|operation| operation.variables_mut());
        let aggregation_filter_variables = self
            .aggregation
            .filters
            .iter_mut()
            .flat_map(|filter| filter.variables_mut());

        Box::new(
            head_variables
                .chain(positive_body_variables)
                .chain(positive_operation_variables)
                .chain(positive_filter_variables)
                .chain(negative_body_variables)
                .chain(negative_filter_variables)
                .chain(aggregation_variables)
                .chain(aggregation_operation_variables)
                .chain(aggregation_filter_variables),
        )
    }
}

impl IterablePrimitives for ChaseRule {
    fn primitive_terms<'a>(
        &'a self,
    ) -> Box<dyn Iterator<Item = &'a crate::rule_model::components::term::primitive::Primitive> + 'a>
    {
        let head_terms = self.head().iter().flat_map(|atom| atom.primitive_terms());
        let positive_operation_terms = self
            .positive_operations()
            .iter()
            .flat_map(|operation| operation.primitive_terms());
        let positive_filter_terms = self
            .positive_filters()
            .iter()
            .flat_map(|filter| filter.primitive_terms());

        let negative_filter_terms = self
            .negative_filters()
            .iter()
            .flatten()
            .flat_map(|filter| filter.primitive_terms());

        let aggregation_operation_terms = self
            .aggregate_operations()
            .iter()
            .flat_map(|operation| operation.primitive_terms());
        let aggregation_filter_terms = self
            .aggregate_filters()
            .iter()
            .flat_map(|filter| filter.primitive_terms());

        Box::new(
            head_terms
                .chain(positive_operation_terms)
                .chain(positive_filter_terms)
                .chain(negative_filter_terms)
                .chain(aggregation_operation_terms)
                .chain(aggregation_filter_terms),
        )
    }

    fn primitive_terms_mut<'a>(
        &'a mut self,
    ) -> Box<
        dyn Iterator<Item = &'a mut crate::rule_model::components::term::primitive::Primitive> + 'a,
    > {
        todo!()
    }
}
