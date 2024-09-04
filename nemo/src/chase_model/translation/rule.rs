//! This module defines a function for translating logical rules into chase rules

use std::collections::{HashMap, HashSet};

use crate::{
    chase_model::components::{
        aggregate::ChaseAggregate,
        atom::{primitive_atom::PrimitiveAtom, variable_atom::VariableAtom},
        filter::ChaseFilter,
        rule::ChaseRule,
    },
    rule_model::components::{
        atom::Atom,
        literal::Literal,
        term::{
            primitive::{
                variable::{Variable, VariableName},
                Primitive,
            },
            Term,
        },
        IterableVariables, ProgramComponent,
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
                }
                Literal::Negative(atom) => {
                    let (variable_atom, filters) = self.build_body_atom(atom);
                    result.add_negative_atom(variable_atom);
                    for filter in filters {
                        result.add_negative_filter_last(filter);
                    }
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
                if let Some((left, term)) = operation.variable_assignment() {
                    if let Term::Primitive(Primitive::Variable(right)) = term {
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
                    let new_name = VariableName::new(name);
                    variable.rename(new_name);
                }
            }
        }
    }

    /// For a given positive body atom, return the corresponding [VariableAtom].
    ///
    /// # Panics
    /// Panics if atom contains a structured term or an aggregate.
    fn build_body_atom(&mut self, atom: &Atom) -> (VariableAtom, Vec<ChaseFilter>) {
        let origin = atom.origin().clone();
        let predicate = atom.predicate().clone();
        let mut variables = Vec::new();

        let mut filters = Vec::new();

        let mut used_variables = HashSet::<&Variable>::new();

        for argument in atom.arguments() {
            match argument {
                Term::Primitive(Primitive::Variable(variable)) => {
                    if !used_variables.insert(variable) {
                        // If the variable was already used in the same atom,
                        // we create a new variable

                        let new_variable = Variable::universal(&self.create_fresh_variable());
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
                    let new_variable = Variable::universal(&self.create_fresh_variable());
                    let new_filter = Self::build_filter_primitive(&new_variable, primitive);

                    variables.push(new_variable);
                    filters.push(new_filter);
                }
                Term::Operation(operation) => {
                    let new_variable = Variable::universal(&self.create_fresh_variable());
                    let new_filter = Self::build_filter_operation(&new_variable, operation);

                    variables.push(new_variable);
                    filters.push(new_filter);
                }
                _ => unreachable!(
                    "invalid program: body may not include structured terms or aggregates"
                ),
            }
        }

        let variable_atom = VariableAtom::new(origin, predicate, variables);
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
                        {
                            derived_variables.insert(variable);

                            let new_operation = Self::build_operation(variable, operation);
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
                let new_filter = ChaseFilter::new(operation.origin().clone(), new_operation);

                result.add_positive_filter(new_filter);
            }
        }
    }

    /// Translates each head atom into the [PrimitiveAtom],
    /// while taking care of operations and aggregates.
    fn handle_head(&mut self, result: &mut ChaseRule, head: &Vec<Atom>) {
        let mut chase_aggregate: Option<ChaseAggregate> = None;

        for atom in head {
            let origin = atom.origin().clone();
            let predicate = atom.predicate().clone();
            let mut terms = Vec::new();

            for (argument_index, argument) in atom.arguments().enumerate() {
                let group_by_variables =
                    Self::compute_group_by_variables(atom.arguments(), argument_index);

                match argument {
                    Term::Primitive(primitive) => terms.push(primitive.clone()),
                    Term::Aggregate(aggregate) => {
                        let new_aggregate =
                            self.build_aggregate(result, aggregate, &group_by_variables);

                        terms.push(Primitive::Variable(new_aggregate.output_variable().clone()));
                        chase_aggregate = Some(new_aggregate);
                    }
                    Term::Operation(operation) => {
                        let new_variable = Variable::universal(&self.create_fresh_variable());

                        let new_operation = self.build_operation_with_aggregate(
                            result,
                            operation,
                            &group_by_variables,
                            new_variable.clone(),
                            &mut chase_aggregate,
                        );

                        result.add_aggregation_operation(new_operation);
                        terms.push(Primitive::Variable(new_variable));
                    }
                    _ => unreachable!("invalid program: rule head contains complex terms"),
                }
            }

            result.add_head_atom(PrimitiveAtom::new(origin, predicate, terms))
        }
    }

    /// Compute group-by-variables for a head atom.
    ///
    /// Essentially, these are all variables contained in some terms
    /// that are not the term containing the aggregate.
    fn compute_group_by_variables<'a>(
        terms: impl Iterator<Item = &'a Term>,
        current_index: usize,
    ) -> HashSet<Variable> {
        let mut result = HashSet::new();

        for (term_index, term) in terms.enumerate() {
            if term_index == current_index {
                continue;
            }

            result.extend(term.variables().cloned());
        }

        result
    }
}
