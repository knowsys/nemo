//! This module defines a function for translating logical rules into chase rules

use std::collections::{HashMap, HashSet};

use crate::{
    chase_model::components::{
        atom::{primitive_atom::PrimitiveAtom, variable_atom::VariableAtom},
        filter::ChaseFilter,
        rule::ChaseRule,
    },
    rule_model::components::{
        atom::Atom,
        literal::Literal,
        term::{
            aggregate::Aggregate,
            primitive::{variable::Variable, Primitive},
            Term,
        },
        IterableVariables,
    },
};

use super::ProgramChaseTranslation;

impl ProgramChaseTranslation {
    /// Translate a [rule][crate::rule_model::components::rule::Rule]
    /// into a [ChaseRule].
    ///
    /// # Panics
    /// Panics if
    ///     * the rule contains structured terms
    ///     * the body contains any aggregates
    pub(crate) fn build_rule(
        &mut self,
        rule: &mut crate::rule_model::components::rule::Rule,
    ) -> ChaseRule {
        let mut result = ChaseRule::default();

        let variable_assignments = Self::variables_assignments(rule);
        Self::apply_variable_assignment(rule, &variable_assignments);

        // Handle positive and negative atoms
        for literal in rule.body() {
            match literal {
                Literal::Positive(atom) => {
                    let (variable_atom, filters) = self.build_body_atom(atom);

                    result.add_positive_atom(variable_atom);
                    for filter in filters {
                        result.add_positive_filter(filter);
                    }
                    self.predicate_arity.insert(atom.predicate(), atom.len());
                }
                Literal::Negative(atom) => {
                    let (variable_atom, filters) = self.build_body_atom(atom);

                    result.add_negative_atom(variable_atom);
                    for filter in filters {
                        result.add_negative_filter_last(filter);
                    }
                    self.predicate_arity.insert(atom.predicate(), atom.len());
                }
                Literal::Operation(_) => {
                    // Will be handled below
                }
            }
        }

        // Handle operations
        self.handle_operations(&mut result, rule);

        // Handle head
        self.handle_head(&mut result, rule.head());

        result
    }

    /// Creates a map assigning variables that are equal to each other.
    fn variables_assignments(
        rule: &crate::rule_model::components::rule::Rule,
    ) -> HashMap<Variable, Variable> {
        let mut assignment = HashMap::<Variable, Variable>::new();

        for literal in rule.body() {
            if let Literal::Operation(operation) = literal {
                if let Some((left, Term::Primitive(Primitive::Variable(right)))) =
                    operation.variable_assignment()
                {
                    // Operation has the form ?left = ?right
                    if let Some(assigned) = assignment.get(left) {
                        assignment.insert(right.clone(), assigned.clone());
                    } else if let Some(assigned) = assignment.get(right) {
                        assignment.insert(left.clone(), assigned.clone());
                    } else {
                        assignment.insert(left.clone(), right.clone());
                    }
                }
            }
        }

        assignment
    }

    /// Replace each variable occurring in the rule
    /// according to the given variable assignment map.
    fn apply_variable_assignment(
        rule: &mut crate::rule_model::components::rule::Rule,
        assignment: &HashMap<Variable, Variable>,
    ) {
        for variable in rule.variables_mut() {
            if let Some(new_variable) = assignment.get(variable) {
                if let Some(name) = new_variable.name() {
                    variable.rename(name.to_owned());
                }
            }
        }
    }

    /// For a given positive body atom, return the corresponding [VariableAtom].
    ///
    /// # Panics
    /// Panics if atom contains a structured term or an aggregate.
    fn build_body_atom(&mut self, atom: &Atom) -> (VariableAtom, Vec<ChaseFilter>) {
        let predicate = atom.predicate().clone();
        let mut variables = Vec::new();

        let mut filters = Vec::new();

        let mut used_variables = HashSet::<&Variable>::new();

        for argument in atom.terms() {
            match argument {
                Term::Primitive(Primitive::Variable(variable)) => {
                    if variable.is_anonymous() {
                        let new_variable = self.create_fresh_variable();
                        variables.push(new_variable);
                    } else if !used_variables.insert(variable) {
                        // If the variable was already used in the same atom,
                        // we create a new variable

                        let new_variable = self.create_fresh_variable();
                        let new_filter = Self::build_filter_primitive(
                            &new_variable,
                            &Primitive::Variable(variable.clone()),
                        );

                        variables.push(new_variable);
                        filters.push(new_filter);
                    } else {
                        variables.push(variable.clone());
                    }
                }
                Term::Primitive(primitive) => {
                    let new_variable = self.create_fresh_variable();
                    let new_filter = Self::build_filter_primitive(&new_variable, primitive);

                    variables.push(new_variable);
                    filters.push(new_filter);
                }
                Term::Operation(operation) => {
                    let new_variable = self.create_fresh_variable();
                    let new_filter = Self::build_filter_operation(&new_variable, operation);

                    variables.push(new_variable);
                    filters.push(new_filter);
                }
                _ => unreachable!(
                    "invalid program: body may not include structured terms or aggregates"
                ),
            }
        }

        let variable_atom = VariableAtom::new(predicate, variables);
        (variable_atom, filters)
    }

    /// Translates each [Operation][crate::rule_model::components::term::operation::Operation]
    /// into a [ChaseFilter] or [ChaseOperation][crate::chase_model::components::operation::ChaseOperation]
    /// depending on the occurring variables.
    fn handle_operations(
        &mut self,
        result: &mut ChaseRule,
        rule: &crate::rule_model::components::rule::Rule,
    ) {
        let mut derived_variables = rule.positive_variables();
        let mut handled_literals = HashSet::new();

        // We compute a new value if
        //    * the operation has the form of an assignment
        //    * the "right-hand side" of the assignment only contains derived variables
        // We compute derived variables by starting with variables
        // that are bound by positive body literals
        // and variables that are given a value through an assignment as outlined above
        loop {
            let current_count = derived_variables.len();

            for (literal_index, literal) in rule.body().iter().enumerate() {
                if handled_literals.contains(&literal_index) {
                    continue;
                }

                if let Literal::Operation(operation) = literal {
                    if let Some((variable, term)) = operation.variable_assignment() {
                        if variable.is_universal()
                            && variable.name().is_some()
                            && term
                                .variables()
                                .all(|variable| derived_variables.contains(variable))
                            && !derived_variables.contains(variable)
                        {
                            derived_variables.insert(variable);

                            let new_operation = Self::build_operation(variable, term);
                            result.add_positive_operation(new_operation);

                            handled_literals.insert(literal_index);
                        }
                    }
                }
            }

            if derived_variables.len() == current_count {
                break;
            }
        }

        // The remaining operation terms become filters
        for (literal_index, literal) in rule.body().iter().enumerate() {
            if handled_literals.contains(&literal_index) {
                continue;
            }

            if let Literal::Operation(operation) = literal {
                let new_operation = Self::build_operation_term(operation);
                let new_filter = ChaseFilter::new(new_operation);

                result.add_positive_filter(new_filter);
            }
        }
    }

    /// Translates each head atom into the [PrimitiveAtom],
    /// while taking care of operations and aggregates.
    fn handle_head(&mut self, result: &mut ChaseRule, head: &[Atom]) {
        for (head_index, atom) in head.iter().enumerate() {
            let predicate = atom.predicate().clone();
            let mut terms = Vec::new();

            let mut aggregate: Option<(&Aggregate, usize, HashSet<Variable>)> = None;
            let aggregate_variable = Variable::universal("__AGGREGATE");

            for (argument_index, argument) in atom.terms().enumerate() {
                match argument {
                    Term::Primitive(primitive) => terms.push(primitive.clone()),
                    Term::Aggregate(term_aggregate) => {
                        aggregate = Some((term_aggregate, argument_index, HashSet::default()));

                        terms.push(Primitive::Variable(aggregate_variable.clone()));
                    }
                    Term::Operation(operation) => {
                        let new_variable = self.create_fresh_variable();

                        let (new_operation, operation_aggregate) = self
                            .build_operation_with_aggregate(
                                operation,
                                aggregate_variable.clone(),
                                new_variable.clone(),
                            );
                        if let Some(operation_aggregate) = operation_aggregate {
                            let mut operation_variables =
                                new_operation.variables().cloned().collect::<HashSet<_>>();
                            operation_variables.remove(&aggregate_variable);
                            operation_variables.remove(new_operation.variable());

                            aggregate =
                                Some((operation_aggregate, argument_index, operation_variables));
                            result.add_aggregation_operation(new_operation);
                        } else {
                            result.add_positive_operation(new_operation)
                        }

                        terms.push(Primitive::Variable(new_variable));
                    }
                    _ => unreachable!("invalid program: rule head contains complex terms"),
                }
            }

            if let Some((aggregate, argument_index, initial)) = aggregate {
                let group_by_variables =
                    Self::compute_group_by_variables(initial, terms.iter(), argument_index);

                let chase_aggregate = self.build_aggregate(
                    result,
                    aggregate,
                    &group_by_variables,
                    aggregate_variable.clone(),
                );

                result.add_aggregation(chase_aggregate, head_index);
            }

            self.predicate_arity.insert(predicate.clone(), terms.len());
            result.add_head_atom(PrimitiveAtom::new(predicate, terms));
        }
    }

    /// Compute group-by-variables for a head atom.
    ///
    /// Essentially, these are all variables contained in some terms
    /// that are not the term containing the aggregate.
    fn compute_group_by_variables<'a>(
        initial: HashSet<Variable>,
        terms: impl Iterator<Item = &'a Primitive>,
        current_index: usize,
    ) -> Vec<Variable> {
        let mut result = initial;

        for (term_index, term) in terms.enumerate() {
            if term_index == current_index {
                continue;
            }

            result.extend(term.variables().cloned());
        }

        result.into_iter().collect()
    }
}
