//! This module defines [NormalizedRule].

use std::{
    collections::{HashMap, HashSet},
    fmt::Display,
};

use nemo_physical::{
    datavalues::AnyDataValue,
    function::tree::{FunctionLeaf, FunctionTree},
    tabular::{
        filters::{FilterTransformPattern, TransformPosition},
        operations::OperationColumnMarker,
    },
};

use crate::{
    execution::planning::{
        VariableTranslation,
        analysis::variable_order::VariableOrder,
        normalization::{
            aggregate::Aggregation,
            atom::{body::BodyAtom, head::HeadAtom, import::ImportAtom},
            generator::VariableGenerator,
            operation::Operation,
        },
        operations::{filter::GeneratorFilter, function::GeneratorFunction},
    },
    rule_model::components::{
        import_export::clause::ImportLiteral,
        tag::Tag,
        term::primitive::{Primitive, variable::Variable},
    },
    syntax,
    util::seperated_list::DisplaySeperatedList,
};

/// Represents a normalized rule
#[derive(Debug, Default, Clone)]
pub struct NormalizedRule {
    /// Positive body atoms
    positive: Vec<BodyAtom>,
    /// Negative body atoms
    negative: Vec<BodyAtom>,
    /// Import Atoms
    positive_imports: Vec<ImportAtom>,
    /// Negated import atoms
    negative_imports: Vec<ImportAtom>,

    /// Operations
    operations: Vec<Operation>,

    /// Head of the rule
    head: Vec<HeadAtom>,
    /// Aggregation and the head index it occurs in
    aggregation: Option<(Aggregation, usize)>,

    /// Variable order
    variable_order: Option<VariableOrder>,
    /// Unique identifier of this rule
    id: usize,
}

impl NormalizedRule {
    /// Create a simple positive rule.
    #[cfg(test)]
    pub(crate) fn positive_rule(
        head: Vec<HeadAtom>,
        body: Vec<BodyAtom>,
        operations: Vec<Operation>,
    ) -> Self {
        Self {
            head,
            positive: body,
            operations,
            ..Default::default()
        }
    }
}

impl Display for NormalizedRule {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let head = DisplaySeperatedList::display(
            self.head.iter(),
            &format!("{} ", syntax::SEQUENCE_SEPARATOR),
        );

        let body = self
            .positive
            .iter()
            .map(ToString::to_string)
            .chain(self.negative.iter().map(|atom| format!("~{atom}")))
            .chain(self.operations.iter().map(ToString::to_string))
            .chain(self.positive_imports.iter().map(ToString::to_string))
            .chain(
                self.negative_imports
                    .iter()
                    .map(|import| format!("~{import}")),
            )
            .chain(
                self.aggregation
                    .as_ref()
                    .map(|(aggregation, _)| aggregation.to_string()),
            );

        let body = DisplaySeperatedList::display(body, &format!("{} ", syntax::SEQUENCE_SEPARATOR));

        f.write_str(&format!("{head} :- {body}"))
    }
}

impl NormalizedRule {
    /// Return the list of head atoms in this rule.
    pub fn head(&self) -> &Vec<HeadAtom> {
        &self.head
    }

    /// Return the list of positive body atoms in this rule.
    pub fn positive(&self) -> &Vec<BodyAtom> {
        &self.positive
    }

    /// Return the list of negative body atoms in this rule.
    pub fn negative(&self) -> &Vec<BodyAtom> {
        &self.negative
    }

    /// Return the list of imported atoms in this rule.
    pub fn positive_imports(&self) -> &Vec<ImportAtom> {
        &self.positive_imports
    }

    /// Return the list of negated imported atoms in this rule.
    pub fn negative_imports(&self) -> &Vec<ImportAtom> {
        &self.negative_imports
    }

    /// Return a list of all positive atoms, including import atoms
    pub fn positive_all(&self) -> Vec<BodyAtom> {
        let mut positive = self.positive.clone();

        positive.extend(self.positive_imports.iter().map(|atom| {
            BodyAtom::new(
                atom.predicate(),
                atom.variables().cloned().collect::<Vec<_>>(),
            )
        }));

        positive
    }

    /// Return the list of operations in this rule.
    pub fn operations(&self) -> &Vec<Operation> {
        &self.operations
    }

    /// Return the aggregate in this rule.
    pub fn aggregate(&self) -> Option<&Aggregation> {
        self.aggregation
            .as_ref()
            .map(|(aggregation, _)| aggregation)
    }

    /// Return the head index containing the aggregation operation.
    pub fn aggregate_index(&self) -> Option<usize> {
        self.aggregation.as_ref().map(|(_, index)| *index)
    }

    /// Return the id of this rule.
    pub fn id(&self) -> usize {
        self.id
    }
}

impl NormalizedRule {
    /// Return the variable order.
    ///
    /// # Panics
    /// Panics if the variable order has not been computed it.
    /// Call `analyze` on [super::program::NormalizedProgram]
    /// before calling this function.
    pub fn variable_order(&self) -> &VariableOrder {
        self.variable_order
            .as_ref()
            .expect("variable order not available for this rule")
    }

    /// Set a variable order for this rule.
    pub fn set_variable_order(&mut self, variable_order: VariableOrder) {
        self.variable_order = Some(variable_order);
    }

    /// Return whether this rule contains existential variables.
    pub fn is_existential(&self) -> bool {
        self.head
            .iter()
            .flat_map(|atom| atom.terms())
            .any(|term| term.is_existential())
    }

    /// Return whether this rule contains any aggregates.
    pub fn contains_aggregates(&self) -> bool {
        self.aggregate().is_some()
    }

    /// Return whether this rule is "recursive",
    /// i.e. contains a head atom that is also in the positive body
    pub fn is_recursive(&self) -> bool {
        for head_atom in &self.head {
            let head_predicate = head_atom.predicate();

            for positive_atom in &self.positive {
                if positive_atom.predicate() == head_predicate {
                    return true;
                }
            }
        }

        false
    }

    /// Prepare the rule in such a way that it is suitable for tracing.
    ///
    /// This includes
    ///     * Moving all statements from positive import to the positive body of the rule
    ///     * Moving all statements from negative import ot the negative body of the rule
    pub fn prepare_tracing(&self) -> Self {
        let mut result = self.clone();

        let mut positive = self.positive.clone();
        for import in self.positive_imports() {
            let atom = BodyAtom::new(import.predicate().clone(), import.variables_cloned());
            positive.push(atom);
        }

        let mut negative = self.negative.clone();
        for import in self.negative_imports() {
            let atom = BodyAtom::new(import.predicate().clone(), import.variables_cloned());
            negative.push(atom);
        }

        result.positive = positive;
        result.negative = negative;

        result.positive_imports.clear();
        result.negative_imports.clear();

        result
    }

    /// Return an iterator over all variables that do not occur in the head of this rule.
    pub fn variables_non_head(&self) -> impl Iterator<Item = &Variable> {
        let positive_variables = self.positive.iter().flat_map(|atom| atom.terms());
        let negative_variables = self.negative.iter().flat_map(|atom| atom.terms());
        let positive_import_variables = self
            .positive_imports
            .iter()
            .flat_map(|atom| atom.variables());
        let negative_import_variables = self
            .negative_imports
            .iter()
            .flat_map(|atom| atom.variables());

        let operation_variables = self
            .operations
            .iter()
            .flat_map(|operation| operation.variables());
        let aggregation_variables = self
            .aggregation
            .as_ref()
            .map(|(aggregation, _)| aggregation.variables())
            .into_iter()
            .flatten();

        positive_variables
            .chain(negative_variables)
            .chain(positive_import_variables)
            .chain(negative_import_variables)
            .chain(operation_variables)
            .chain(aggregation_variables)
    }

    /// Return an iterator over all variables contained in this rule.
    pub fn variables(&self) -> impl Iterator<Item = &Variable> {
        let head_variables = self.head.iter().flat_map(|atom| atom.variables());
        let non_head_variables = self.variables_non_head();

        head_variables.chain(non_head_variables)
    }

    /// Return an iterator over all predicates occurring in a positive atom
    /// (including import atoms).
    pub fn predicates_positive(&self) -> impl Iterator<Item = (Tag, usize)> {
        let positive = self
            .positive
            .iter()
            .map(|atom| (atom.predicate(), atom.arity()));
        let import = self
            .positive_imports
            .iter()
            .map(|atom| (atom.predicate(), atom.arity()));

        positive.chain(import)
    }

    /// Return an iterator over all predicates occurring in a negative atom.
    pub fn predicates_negative(&self) -> impl Iterator<Item = (Tag, usize)> {
        let negative = self
            .negative
            .iter()
            .map(|atom| (atom.predicate(), atom.arity()));
        let import = self
            .negative_imports
            .iter()
            .map(|atom| (atom.predicate(), atom.arity()));

        negative.chain(import)
    }

    /// Return an iterator over all predicates occurring in the head.
    pub fn predicates_head(&self) -> impl Iterator<Item = (Tag, usize)> {
        self.head
            .iter()
            .map(|atom| (atom.predicate(), atom.arity()))
    }

    /// Return an iterator over predicates used in this rule
    /// together with their arities.
    pub fn predicates(&self) -> impl Iterator<Item = (Tag, usize)> {
        let head = self
            .head
            .iter()
            .map(|atom| (atom.predicate(), atom.arity()));
        let positive = self
            .positive
            .iter()
            .map(|atom| (atom.predicate(), atom.arity()));
        let negative = self
            .negative
            .iter()
            .map(|atom| (atom.predicate(), atom.arity()));
        let positive_import = self
            .positive_imports
            .iter()
            .map(|atom| (atom.predicate(), atom.arity()));
        let negative_import = self
            .negative_imports
            .iter()
            .map(|atom| (atom.predicate(), atom.arity()));

        head.chain(positive)
            .chain(negative)
            .chain(positive_import)
            .chain(negative_import)
    }

    /// Return the set of frontier variables in this rule,
    /// i.e. the set of variables contained
    /// in both the head and the (positive) body of the rule.
    pub fn frontier(&self) -> HashSet<Variable> {
        let head_variables = self
            .head
            .iter()
            .flat_map(|atom| atom.variables())
            .collect::<HashSet<_>>();

        let positive_variables = self.positive.iter().flat_map(|atom| atom.terms());
        let import_variables = self
            .positive_imports
            .iter()
            .flat_map(|atom| atom.variables());

        let body_variables = positive_variables
            .chain(import_variables)
            .collect::<HashSet<_>>();

        head_variables
            .intersection(&body_variables)
            .cloned()
            .cloned()
            .collect::<HashSet<_>>()
    }

    /// Return an iterator over all [AnyDataValue] contained in this rule.
    pub fn datavalues(&self) -> impl Iterator<Item = AnyDataValue> {
        let head_values = self.head.iter().flat_map(|atom| {
            atom.terms().filter_map(|term| {
                if let Primitive::Ground(ground) = term {
                    Some(ground.value())
                } else {
                    None
                }
            })
        });
        let operation_values = self
            .operations
            .iter()
            .flat_map(|operation| operation.datavalues());

        head_values.chain(operation_values)
    }

    /// Translate the [HeadAtom]s into a list of [BodyAtom]s
    /// and additional filter [Operation]s.
    ///
    /// New variables are appended to the given variable order.
    pub fn normalize_existential_head(
        &self,
        order: &mut VariableOrder,
    ) -> (Vec<BodyAtom>, Vec<Operation>) {
        let mut generator = VariableGenerator::default();

        let mut atoms = Vec::<BodyAtom>::default();
        let mut operations = Vec::<Operation>::default();

        for head_atom in &self.head {
            let mut used_variables = HashSet::<Variable>::default();
            let mut variables = Vec::<Variable>::default();

            for term in head_atom.terms() {
                match term {
                    Primitive::Variable(variable) => {
                        if !used_variables.insert(variable.clone()) {
                            let new_variable = generator.universal("RESTRICTED_HEAD");
                            let new_operation = Operation::new_assignment(
                                new_variable.clone(),
                                Operation::new_variable(variable.clone()),
                            );

                            variables.push(new_variable.clone());
                            order.push(new_variable);
                            operations.push(new_operation);
                        } else {
                            variables.push(variable.clone());
                        }
                    }
                    Primitive::Ground(ground_term) => {
                        let new_variable = generator.universal("RESTRICTED_HEAD");
                        let new_operation = Operation::new_assignment(
                            new_variable.clone(),
                            Operation::new_ground(ground_term.clone()),
                        );

                        variables.push(new_variable.clone());
                        order.push(new_variable);
                        operations.push(new_operation);
                    }
                }
            }

            atoms.push(BodyAtom::new(head_atom.predicate(), variables));
        }

        (atoms, operations)
    }
}

impl NormalizedRule {
    /// Receives a [crate::rule_model::components::rule::Rule]
    /// and normalizes into a [NormalizedRule].
    ///
    /// # Panics
    /// Panics if rule is ill-formed.
    pub fn normalize_rule(rule: &crate::rule_model::components::rule::Rule, id: usize) -> Self {
        let mut generator = VariableGenerator::default();

        let mut operations = rule
            .body_operations()
            .map(Operation::normalize_body_operation)
            .collect::<Vec<_>>();

        let mut positive = Vec::<BodyAtom>::default();
        for atom in rule.body_positive() {
            let (normalized_atom, new_operations) = BodyAtom::normalize_atom(&mut generator, atom);

            positive.push(normalized_atom);
            operations.extend(new_operations);
        }

        let mut negative = Vec::<BodyAtom>::default();
        for atom in rule.body_negative() {
            let (normalized_atom, new_operations) = BodyAtom::normalize_atom(&mut generator, atom);

            negative.push(normalized_atom);
            operations.extend(new_operations);
        }

        let mut head = Vec::<HeadAtom>::default();
        let mut aggregation: Option<(Aggregation, usize)> = None;
        for (atom_index, atom) in rule.head().iter().enumerate() {
            let (normalized_atom, new_operations, new_aggregation) =
                HeadAtom::normalize_atom(&mut generator, atom);

            head.push(normalized_atom);
            operations.extend(new_operations);

            if let Some(new_aggregation) = new_aggregation {
                aggregation = Some((new_aggregation, atom_index));
            }
        }

        let mut positive_imports = Vec::default();
        let mut negative_imports = Vec::default();

        for import in rule.imports() {
            match import {
                ImportLiteral::Positive(clause) => {
                    positive_imports.push(ImportAtom::normalize_import(clause))
                }
                ImportLiteral::Negative(clause) => {
                    negative_imports.push(ImportAtom::normalize_import(clause))
                }
            }
        }

        Self {
            positive,
            negative,
            positive_imports,
            negative_imports,
            operations,
            head,
            aggregation,
            variable_order: None,
            id,
        }
    }

    pub(crate) fn into_filter_transform_pattern(mut self) -> Option<FilterTransformPattern> {
        let positive_variables = self.positive.iter().flat_map(|atom| atom.terms()).cloned().collect::<Vec<_>>();
        let mut translation = VariableTranslation::new();
        for variable in positive_variables.iter().chain(self.variables()) {
            translation.add_marker(variable.clone());
        }

        let generator_functions = GeneratorFunction::new(positive_variables, &mut self.operations);
        let generator_filters =
            GeneratorFilter::new(generator_functions.output_variables(), &mut self.operations);

        let filters = generator_filters
            .filters()
            .map(|filter| filter.function_tree(&translation))
            .collect::<Vec<_>>();

        let mut variable_operations = generator_functions
            .functions()
            .map(|(variable, function)| (variable.clone(), function.function_tree(&translation)))
            .collect::<HashMap<_, _>>();

        let mut variable_positions = HashMap::new();
        for (position, variable) in self.positive[0].terms().enumerate() {
            variable_positions.insert(variable.clone(), position);
        }

        loop {
            let mut changed = false;
            let operations = variable_operations.clone();

            for (variable, _operation) in generator_functions.functions() {
                let tree = variable_operations
                    .get_mut(variable)
                    .expect("all variables are bound");
                for mut term in tree.leaves() {
                    if let ref mut subtree @ FunctionTree::Leaf(FunctionLeaf::Reference(_)) = term {
                        let FunctionTree::Leaf(FunctionLeaf::Reference(reference)) = subtree else {
                            unreachable!()
                        };
                        let variable = translation
                            .find(reference)
                            .expect("all variables are bound");
                        if let Some(operation) = operations.get(variable) {
                            // this is an auxiliary variable, inline it
                            changed = true;
                            *(*subtree) = operation.clone();
                        }
                    }
                }
            }

            if !changed {
                break;
            }
        }

        let mut transform_positions = Vec::new();

        for (position, term) in self.head[0].terms().enumerate() {
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

#[cfg(test)]
mod test {
    use nemo_physical::{
        datavalues::AnyDataValue,
        function::tree::FunctionTree,
        tabular::{
            filters::{FilterTransformPattern, TransformPosition},
            operations::OperationTable,
        },
    };

    #[cfg(not(miri))]
    use test_log::test;

    use crate::{
        execution::planning::normalization::{
            atom::{body::BodyAtom, head::HeadAtom},
            operation::Operation,
            rule::NormalizedRule,
        },
        rule_model::components::{
            tag::Tag,
            term::{
                operation::operation_kind::OperationKind,
                primitive::{Primitive, variable::Variable},
            },
        },
    };

    #[test]
    fn filter_transform_pattern_with_expression_chain() {
        let head = HeadAtom::new(
            Tag::new("head".to_string()),
            vec![
                Primitive::universal_variable("?x"),
                Primitive::universal_variable("?r"),
            ],
        );

        let body = BodyAtom::new(
            Tag::new("body".to_string()),
            vec![Variable::universal("?x"), Variable::universal("?y")],
        );

        let z = Operation::new_assignment(
            Variable::universal("?z"),
            Operation::Operation {
                kind: OperationKind::NumericProduct,
                subterms: vec![
                    Operation::Primitive(Primitive::ground(AnyDataValue::new_integer_from_u64(2))),
                    Operation::Primitive(Primitive::universal_variable("?y")),
                ],
            },
        );

        let r = Operation::new_assignment(
            Variable::universal("?r"),
            Operation::Operation {
                kind: OperationKind::NumericSum,
                subterms: vec![
                    Operation::Primitive(Primitive::universal_variable("?z")),
                    Operation::Primitive(Primitive::ground(AnyDataValue::new_integer_from_u64(7))),
                ],
            },
        );

        let rule = NormalizedRule::positive_rule(vec![head], vec![body], vec![z, r]);

        let mut table = OperationTable::default();
        let zero = *table.push_new();
        let one = *table.push_new();

        let pattern = rule.into_filter_transform_pattern();
        let expected = Some(FilterTransformPattern::new(
            FunctionTree::constant(AnyDataValue::new_boolean(true)),
            vec![
                TransformPosition::new(0, FunctionTree::reference(zero)),
                TransformPosition::new(
                    1,
                    FunctionTree::numeric_sum(vec![
                        FunctionTree::numeric_product(vec![
                            FunctionTree::constant(AnyDataValue::new_integer_from_u64(2)),
                            FunctionTree::reference(one),
                        ]),
                        FunctionTree::constant(AnyDataValue::new_integer_from_u64(7)),
                    ]),
                ),
            ],
        ));

        assert_eq!(pattern, expected)
    }
}
