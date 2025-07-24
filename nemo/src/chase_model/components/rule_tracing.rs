//! This module defines [ChaseRule].

use std::fmt::Display;

use crate::{
    chase_model::{
        ChaseAtom, analysis::variable_order::VariableOrder, components::rule::ChaseRule,
    },
    rule_model::{
        components::{
            IterablePrimitives, IterableVariables,
            tag::Tag,
            term::{
                Term,
                primitive::{Primitive, variable::Variable},
            },
        },
        origin::Origin,
    },
    syntax,
    util::seperated_list::DisplaySeperatedList,
};

use super::{
    aggregate::ChaseAggregate,
    atom::{primitive_atom::PrimitiveAtom, variable_atom::VariableAtom},
    filter::ChaseFilter,
    operation::ChaseOperation,
};

#[derive(Debug, Clone)]
pub struct VariableRuleAtom {
    predicate: Tag,
    rule: Option<usize>,
    variables: Vec<Variable>,
}

impl Display for VariableRuleAtom {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let terms = DisplaySeperatedList::display(
            self.terms(),
            &format!("{} ", syntax::SEQUENCE_SEPARATOR),
        );
        let predicate = self.predicate();

        let rule = if let Some(rule) = self.rule {
            rule.to_string()
        } else {
            String::default()
        };

        f.write_str(&format!(
            "{predicate}_{}{}{terms}{}",
            rule,
            syntax::expression::atom::OPEN,
            syntax::expression::atom::CLOSE
        ))
    }
}

impl VariableRuleAtom {
    /// Construct a new [VariableAtom].
    pub(crate) fn new(predicate: Tag, rule: Option<usize>, variables: Vec<Variable>) -> Self {
        Self {
            predicate,
            rule,
            variables,
        }
    }

    pub fn to_primitive_atom(&self) -> PrimitiveAtom {
        let terms = self
            .variables
            .iter()
            .map(|variable| Primitive::Variable(variable.clone()))
            .collect::<Vec<_>>();

        PrimitiveAtom::new(self.predicate.clone(), terms)
    }

    pub fn to_variable_atom(&self) -> VariableAtom {
        VariableAtom::new(self.predicate.clone(), self.variables.clone())
    }

    pub fn rule(&self) -> Option<usize> {
        self.rule.clone()
    }

    pub fn push(&mut self, variable: Variable) {
        self.variables.push(variable);
    }
}

impl ChaseAtom for VariableRuleAtom {
    type TypeTerm = Variable;

    fn predicate(&self) -> Tag {
        self.predicate.clone()
    }

    fn terms(&self) -> impl Iterator<Item = &Self::TypeTerm> {
        self.variables.iter()
    }

    fn terms_mut(&mut self) -> impl Iterator<Item = &mut Self::TypeTerm> {
        self.variables.iter_mut()
    }
}

impl IterableVariables for VariableRuleAtom {
    fn variables<'a>(&'a self) -> Box<dyn Iterator<Item = &'a Variable> + 'a> {
        Box::new(self.terms())
    }

    fn variables_mut<'a>(&'a mut self) -> Box<dyn Iterator<Item = &'a mut Variable> + 'a> {
        Box::new(self.terms_mut())
    }
}

/// The positive body of a [ChaseRule]
#[derive(Debug, Default, Clone)]
struct TracingChaseRuleBodyPositive {
    /// Atoms that bind variables
    atoms: Vec<VariableRuleAtom>,
    /// Computation of new bindings
    operations: Vec<ChaseOperation>,
    /// Filtering of results
    filters: Vec<ChaseFilter>,
}

/// The negative body of a [ChaseRule]
#[derive(Debug, Default, Clone)]
struct TracingChaseRuleBodyNegative {
    /// Negated atoms
    atoms: Vec<VariableAtom>,
    /// For each negated atom, the filters that are applied
    filters: Vec<Vec<ChaseFilter>>,
}

/// Handling of aggregation within a [ChaseRule]
#[derive(Debug, Default, Clone)]
struct TracingChaseRuleAggregation {
    /// Aggregate
    aggregate: Option<ChaseAggregate>,

    /// New values created from the aggregation result
    operations: Vec<ChaseOperation>,
    /// Filters based on the aggregation result
    filters: Vec<ChaseFilter>,
}

/// Head of a [ChaseRule]
#[derive(Debug, Default, Clone)]
struct TracingChaseRuleHead {
    /// Head atoms of the rule
    atoms: Vec<PrimitiveAtom>,
    /// Index of the head atom which contains the aggregate
    _aggregate_head_index: Option<usize>,
}

/// Representation of a rule in a [ChaseProgram][super::program::ChaseProgram]
#[allow(dead_code)]
#[derive(Debug, Default, Clone)]
pub struct TracingChaseRule {
    /// Origin of this component
    origin: Origin,

    /// Positive part of the body
    positive: TracingChaseRuleBodyPositive,
    /// Negative part of the body
    negative: TracingChaseRuleBodyNegative,
    /// Aggregation
    aggregation: TracingChaseRuleAggregation,
    /// Head of the rule
    head: TracingChaseRuleHead,
}

impl TracingChaseRule {
    /// Create a simple positive rule.
    #[cfg(test)]
    pub(crate) fn _positive_rule(
        head: Vec<PrimitiveAtom>,
        body: Vec<VariableRuleAtom>,
        filters: Vec<ChaseFilter>,
    ) -> Self {
        let mut result = Self::default();
        result.head.atoms = head;
        result.positive.atoms = body;
        result.positive.filters = filters;

        result
    }
}

impl TracingChaseRule {
    /// Return a default variable order
    pub fn default_order(&self) -> VariableOrder {
        let mut result = VariableOrder::default();

        for variable in self.positive.atoms.iter().flat_map(|atom| atom.variables()) {
            result.push(variable.clone());
        }

        result
    }

    /// Return a [ChaseRule] of the same form.
    pub fn to_chase_rule(&self) -> ChaseRule {
        let mut result = ChaseRule::default();

        for atom in self.head() {
            result.add_head_atom(atom.clone());
        }

        for atom in self.positive_body() {
            result.add_positive_atom(atom.to_variable_atom());
        }

        for filter in self.positive_filters() {
            result.add_positive_filter(filter.clone());
        }

        for operation in self.positive_operations() {
            result.add_positive_operation(operation.clone());
        }

        for (atom, filters) in self.negative_body().iter().zip(self.negative_filters()) {
            result.add_negative_atom(atom.clone());

            for filter in filters {
                result.add_negative_filter_last(filter.clone());
            }
        }

        if let Some(aggregate) = self.aggregate() {
            if let Some(index) = self._aggregate_head_index() {
                result.add_aggregation(aggregate.clone(), index);
            }
        }

        for filter in self.aggregate_filters() {
            result._add_aggregation_filter(filter.clone());
        }

        for operation in self.aggregate_operations() {
            result.add_aggregation_operation(operation.clone());
        }

        result
    }

    /// Return the list of head atoms contained in this rule.
    pub(crate) fn head(&self) -> &Vec<PrimitiveAtom> {
        &self.head.atoms
    }

    /// Return the list of positive body atoms contained in this rule.
    pub(crate) fn positive_body(&self) -> &Vec<VariableRuleAtom> {
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
    pub(crate) fn _aggregate_head_index(&self) -> Option<usize> {
        self.head._aggregate_head_index
    }
}

impl TracingChaseRule {
    /// Add an atom to the positive part of the body.
    pub(crate) fn add_positive_atom(&mut self, atom: VariableRuleAtom) {
        self.positive.atoms.push(atom);
    }

    /// Add an operation for the positive part of the body.
    pub(crate) fn _add_positive_operation(&mut self, operation: ChaseOperation) {
        self.positive.operations.push(operation)
    }

    /// Add a filter to the positive part of the body.
    pub(crate) fn add_positive_filter(&mut self, filter: ChaseFilter) {
        self.positive.filters.push(filter);
    }

    /// Add an atom to the negative part of the body.
    pub(crate) fn _add_negative_atom(&mut self, atom: VariableAtom) {
        self.negative.atoms.push(atom);
        self.negative.filters.push(Vec::default())
    }

    /// Add a filter to the negative part of the body.
    ///
    /// # Panics
    /// Panics if the current filter vector is empty.
    pub(crate) fn _add_negative_filter_last(&mut self, filter: ChaseFilter) {
        self.negative
            .filters
            .last_mut()
            .expect("expected a filter slot")
            .push(filter)
    }

    /// Add a new aggregation operation to the rule.
    pub(crate) fn _add_aggregation(&mut self, aggregate: ChaseAggregate, head_index: usize) {
        self.aggregation.aggregate = Some(aggregate);
        self.head._aggregate_head_index = Some(head_index);
    }

    /// Add a new operation that uses the result of aggregation.
    pub(crate) fn _add_aggregation_operation(&mut self, operation: ChaseOperation) {
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
}

impl Display for TracingChaseRule {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let head = DisplaySeperatedList::display(
            self.head().iter(),
            &format!("{} ", syntax::SEQUENCE_SEPARATOR),
        );

        let body = DisplaySeperatedList::display(
            self.positive_body().iter(),
            &format!("{} ", syntax::SEQUENCE_SEPARATOR),
        );

        let filters = DisplaySeperatedList::display(
            self.positive_filters().iter(),
            &format!("{} ", syntax::SEQUENCE_SEPARATOR),
        );

        f.write_str(&format!("{head} :- {body} {filters}"))
    }
}

impl IterableVariables for TracingChaseRule {
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

impl IterablePrimitives for TracingChaseRule {
    fn primitive_terms<'a>(&'a self) -> Box<dyn Iterator<Item = &'a Primitive> + 'a> {
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

    fn primitive_terms_mut<'a>(&'a mut self) -> Box<dyn Iterator<Item = &'a mut Term> + 'a> {
        todo!()
    }
}
