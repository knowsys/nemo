//! This module defines [NormalizedRule].

use std::fmt::Display;

use crate::{
    chase_model::analysis::variable_order::VariableOrder,
    execution::planning_new::normalization::{
        aggregate::Aggregation,
        atom::{body::BodyAtom, head::HeadAtom, import::ImportAtom},
        generator::VariableGenerator,
        operation::Operation,
    },
    io::format_builder::ImportExportBuilder,
    rule_model::components::{tag::Tag, term::primitive::variable::Variable},
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
    imports: Vec<ImportAtom>,

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
        let mut result = Self::default();

        result.head = head;
        result.positive = body;
        result.operations = operations;

        result
    }
}

impl Display for NormalizedRule {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let head = DisplaySeperatedList::display(
            self.head.iter(),
            &format!("{} ", syntax::SEQUENCE_SEPARATOR),
        );

        let positive = DisplaySeperatedList::display(
            self.positive.iter(),
            &format!("{} ", syntax::SEQUENCE_SEPARATOR),
        );

        let negative = DisplaySeperatedList::display(
            self.negative.iter(),
            &format!("{} ", syntax::SEQUENCE_SEPARATOR),
        );

        let operations = DisplaySeperatedList::display(
            self.operations.iter(),
            &format!("{} ", syntax::SEQUENCE_SEPARATOR),
        );

        let imports = DisplaySeperatedList::display(
            self.imports.iter(),
            &format!("{} ", syntax::SEQUENCE_SEPARATOR),
        );

        let aggregation = self
            .aggregation
            .as_ref()
            .map(|(aggregation, _)| aggregation.to_string())
            .unwrap_or_default();

        let body = vec![positive, imports, negative, operations, aggregation];
        let body =
            DisplaySeperatedList::display(body.iter(), &format!("{} ", syntax::SEQUENCE_SEPARATOR));

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
        &self.positive
    }

    /// Return the list of imported atoms in this rule.
    pub fn imports(&self) -> &Vec<ImportAtom> {
        &self.imports
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

    /// Return the id of this rule.
    pub fn id(&self) -> usize {
        self.id
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
        let import = self
            .imports
            .iter()
            .map(|atom| (atom.predicate(), atom.arity()));

        head.chain(positive).chain(negative).chain(import)
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

    /// Set a [VariableOrder] for this rule.
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
    ///     * Moving all statements from import to the positive and negative body of the rule
    pub fn prepare_tracing(&self) -> Self {
        let mut result = self.clone();

        let mut positive = self.positive.clone();
        for import in self.imports() {
            let atom = BodyAtom::new(import.predicate().clone(), import.variables_cloned());
            positive.push(atom);
        }

        result.positive = positive;
        result.imports.clear();

        result
    }

    /// Return an iterator over all variables contained in this rule.
    pub fn variables(&self) -> impl Iterator<Item = &Variable> {
        let head_variables = self.head.iter().flat_map(|atom| atom.variables());
        let positive_variables = self.positive.iter().flat_map(|atom| atom.terms());
        let negative_variables = self.negative.iter().flat_map(|atom| atom.terms());
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

        head_variables
            .chain(positive_variables)
            .chain(negative_variables)
            .chain(operation_variables)
            .chain(aggregation_variables)
    }
}

impl NormalizedRule {
    /// Receives a [crate::rule_model::components::rule::Rule]
    /// and normalizes into a [NormalizedRule].
    ///
    /// # Panics
    /// Panics if rule is ill-formed.
    pub fn normalize_rule(
        import_builder: &ImportExportBuilder,
        rule: &crate::rule_model::components::rule::Rule,
        id: usize,
    ) -> Self {
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

        let imports = rule
            .imports()
            .map(|import| ImportAtom::normalize_import(import_builder, import))
            .collect::<Vec<_>>();

        Self {
            positive,
            negative,
            imports,
            operations,
            head,
            aggregation,
            variable_order: None,
            id,
        }
    }
}
