//! This module defines [ChaseRule].

use std::{
    collections::{HashMap, HashSet},
    fmt::Display,
};

use nemo_physical::{
    function::tree::FunctionTree,
    tabular::{
        filters::{FilterTransformPattern, TransformPosition},
        operations::{OperationColumnMarker, OperationTableGenerator},
    },
};

use crate::{
    chase_model::{
        ChaseAtom,
        components::{
            import::ChaseImportClause,
            term::operation_term::{Operation, OperationTerm},
        },
    },
    execution::planning::operations::operation::operation_term_to_function_tree,
    rule_model::{
        components::{
            IterablePrimitives, IterableVariables,
            term::{
                operation::operation_kind::OperationKind,
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

/// Import of a [ChaseRule]
#[derive(Debug, Default, Clone)]
struct ChaseRuleImports {
    /// Imports
    imports: Vec<ChaseImportClause>,

    /// Operations applied after executing the imports
    operations: Vec<ChaseOperation>,
    /// Filters applied after executing the imports
    filters: Vec<ChaseFilter>,

    /// Negations applied to import
    negation: ChaseRuleBodyNegative,
}

/// Representation of a rule in a [ChaseProgram][super::program::ChaseProgram]
#[derive(Debug, Default, Clone)]
pub struct ChaseRule {
    /// Origin of this component
    _origin: Origin,

    /// Positive part of the body
    positive: ChaseRuleBodyPositive,
    /// Negative part of the body
    negative: ChaseRuleBodyNegative,
    /// Aggregation
    aggregation: ChaseRuleAggregation,
    /// Head of the rule
    head: ChaseRuleHead,

    /// Imports during rule evaluation
    imports: ChaseRuleImports,
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

impl Display for ChaseRule {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let head = DisplaySeperatedList::display(
            self.head().iter(),
            &format!("{} ", syntax::SEQUENCE_SEPARATOR),
        );

        let body = DisplaySeperatedList::display(
            self.positive_body().iter(),
            &format!("{} ", syntax::SEQUENCE_SEPARATOR),
        );

        let operations = DisplaySeperatedList::display(
            self.positive_operations().iter(),
            &format!("{} ", syntax::SEQUENCE_SEPARATOR),
        );

        let filters = DisplaySeperatedList::display(
            self.positive_filters().iter(),
            &format!("{} ", syntax::SEQUENCE_SEPARATOR),
        );

        f.write_str(&format!("{head} :- {body} {operations} {filters}"))
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

    pub(crate) fn _negative_atoms(&self) -> &Vec<VariableAtom> {
        &self.negative.atoms
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

    /// Return the list of imports that will be executed as part of this rule evaluation.
    pub(crate) fn imports(&self) -> &Vec<ChaseImportClause> {
        &self.imports.imports
    }

    /// Return the list of operations applied after the imports.
    pub(crate) fn imports_operations(&self) -> &Vec<ChaseOperation> {
        &self.imports.operations
    }

    /// return the list of filters applied after the imports.
    pub(crate) fn imports_filters(&self) -> &Vec<ChaseFilter> {
        &self.imports.filters
    }

    /// Return the list of negative body atoms contained in this rule.
    pub(crate) fn imports_negative_body(&self) -> &Vec<VariableAtom> {
        &self.imports.negation.atoms
    }

    /// Return the list of filters that will be applied to the negative part of the body.
    pub(crate) fn imports_negative_filters(&self) -> &Vec<Vec<ChaseFilter>> {
        &self.imports.negation.filters
    }

    /// Return an iterator over the body variables (without imports).
    pub(crate) fn body_variables(&self) -> Box<dyn Iterator<Item = &Variable> + '_> {
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

        Box::new(
            positive_body_variables
                .chain(positive_operation_variables)
                .chain(positive_filter_variables)
                .chain(negative_body_variables)
                .chain(negative_filter_variables),
        )
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

    /// Return an iterator over primitive terms
    pub(crate) fn primitive_terms(&self) -> Box<dyn Iterator<Item = &Primitive> + '_> {
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

    /// Add a new import clause to the rule.
    pub(crate) fn add_import_clause(&mut self, import: ChaseImportClause) {
        self.imports.imports.push(import);
    }

    /// Add a new operation applied after imports.
    pub(crate) fn add_import_operation(&mut self, operation: ChaseOperation) {
        self.imports.operations.push(operation);
    }

    /// Add a new filter applied after imports.
    pub(crate) fn add_import_filter(&mut self, filter: ChaseFilter) {
        self.imports.filters.push(filter);
    }

    /// Add an atom to the negative part of the body.
    pub(crate) fn add_import_negative_atom(&mut self, atom: VariableAtom) {
        self.imports.negation.atoms.push(atom);
        self.imports.negation.filters.push(Vec::default())
    }

    /// Add a filter to the negative part of the body.
    ///
    /// # Panics
    /// Panics if the current filter vector is empty.
    pub(crate) fn add_import_negative_filter_last(&mut self, filter: ChaseFilter) {
        self.imports
            .negation
            .filters
            .last_mut()
            .expect("expected a filter slot")
            .push(filter)
    }

    /// Prepare the rule in such a way that it is suitable for tracing.
    ///
    /// This includes
    ///     * Moving all statements from import to the positive and negative body of the rule
    pub(crate) fn prepare_tracing(&self) -> Self {
        if self.imports.imports.is_empty() {
            return self.clone();
        }

        let mut result = self.clone();

        if result.positive.atoms.len() == 1
            && result.positive.atoms[0].predicate().name() == "_EMPTY"
        {
            result.positive.atoms.clear();
        }

        for clause in result.imports.imports.drain(..) {
            let atom = VariableAtom::new(clause.predicate().clone(), clause.bindings().clone());
            result.positive.atoms.push(atom);
        }

        result
            .positive
            .operations
            .append(&mut result.imports.operations);

        result.positive.filters.append(&mut result.imports.filters);

        let derived_variables = result
            .positive_body()
            .iter()
            .flat_map(|atom| atom.variables())
            .chain(
                result
                    .imports_operations()
                    .iter()
                    .map(|operation| operation.variable()),
            )
            .cloned()
            .collect::<HashSet<_>>();

        let mut new_filters = Vec::<ChaseFilter>::default();
        result.positive.operations.retain(|operation| {
            if derived_variables.contains(operation.variable()) {
                let new_filter = ChaseFilter::new(OperationTerm::Operation(Operation::new(
                    OperationKind::Equal,
                    vec![
                        OperationTerm::Primitive(Primitive::Variable(operation.variable().clone())),
                        operation.operation().clone(),
                    ],
                )));
                new_filters.push(new_filter);
                false
            } else {
                true
            }
        });

        result.positive.filters.extend(new_filters);

        result
            .negative
            .atoms
            .append(&mut result.imports.negation.atoms);
        result
            .negative
            .filters
            .append(&mut result.imports.negation.filters);

        result
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
        let import_variables = self.imports().iter().flat_map(|import| import.variables());
        let import_operation_variables = self
            .imports
            .operations
            .iter()
            .flat_map(|operation| operation.variables());
        let import_filter_variables = self
            .imports
            .filters
            .iter()
            .flat_map(|filter| filter.variables());
        let import_negative_body_variables = self
            .imports_negative_body()
            .iter()
            .flat_map(|atom| atom.variables());
        let import_negative_filter_variables = self
            .imports_negative_filters()
            .iter()
            .flatten()
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
                .chain(aggregation_filter_variables)
                .chain(import_variables)
                .chain(import_operation_variables)
                .chain(import_filter_variables)
                .chain(import_negative_body_variables)
                .chain(import_negative_filter_variables),
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

        let import_variables = self
            .imports
            .imports
            .iter_mut()
            .flat_map(|import| import.variables_mut());
        let import_operation_variables = self
            .imports
            .operations
            .iter_mut()
            .flat_map(|operation| operation.variables_mut());
        let import_filter_variables = self
            .imports
            .filters
            .iter_mut()
            .flat_map(|filter| filter.variables_mut());
        let imports_negative_body_variables = self
            .imports
            .negation
            .atoms
            .iter_mut()
            .flat_map(|atom| atom.variables_mut());
        let imports_negative_filter_variables = self
            .imports
            .negation
            .filters
            .iter_mut()
            .flatten()
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
                .chain(aggregation_filter_variables)
                .chain(import_variables)
                .chain(import_operation_variables)
                .chain(import_filter_variables)
                .chain(imports_negative_body_variables)
                .chain(imports_negative_filter_variables),
        )
    }
}

impl ChaseRule {
    pub(crate) fn into_filter_transform_pattern(self) -> Option<FilterTransformPattern> {
        let mut filters = Vec::new();
        let mut transform_positions = Vec::new();

        let mut variable_positions = HashMap::new();
        let mut variable_operations = HashMap::new();

        let mut variable_translation = OperationTableGenerator::new();

        for (position, variable) in self.positive.atoms[0].terms().enumerate() {
            variable_translation.add_marker(variable.clone());
            variable_positions.insert(variable.clone(), position);
        }

        for filter in self.positive.filters {
            let tree = operation_term_to_function_tree(&variable_translation, filter.filter());
            filters.push(tree);
        }

        for operation in self.positive.operations {
            let variable = operation.variable();
            variable_translation.add_marker(variable.clone());
            let tree =
                operation_term_to_function_tree(&variable_translation, operation.operation());
            variable_operations.insert(variable.clone(), tree);
        }

        for (position, term) in self.head.atoms[0].terms().enumerate() {
            match term {
                Primitive::Ground(ground) => {
                    transform_positions.push(TransformPosition::new(
                        position,
                        FunctionTree::constant(ground.value()),
                    ));
                }
                Primitive::Variable(variable) => {
                    if let Some(&reference) = variable_positions.get(variable) {
                        transform_positions.push(TransformPosition::new(
                            position,
                            FunctionTree::reference(OperationColumnMarker(reference)),
                        ));
                    } else {
                        let operation = variable_operations
                            .get(variable)
                            .expect("every variable is bound");
                        transform_positions
                            .push(TransformPosition::new(position, operation.clone()));
                    }
                }
            }
        }

        if filters.is_empty() && transform_positions.is_empty() {
            None
        } else {
            Some(FilterTransformPattern::new(
                FunctionTree::boolean_conjunction(filters),
                transform_positions,
            ))
        }
    }
}

impl IterablePrimitives for ChaseRule {
    type TermType = Primitive;

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

    fn primitive_terms_mut<'a>(&'a mut self) -> Box<dyn Iterator<Item = &'a mut Primitive> + 'a> {
        unimplemented!("currently unused, needs support in the entire chase model")
    }
}
