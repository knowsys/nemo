//! Module for common functionality for the computation of different reliances.

use std::collections::{HashMap, HashSet};

use petgraph::Directed;

use crate::util::{class_assignment::ClassAssignment, labeled_graph::LabeledGraph};

use super::{
    positive::PositiveReliance,
    restraint::RestraintReliance,
    rules::{Atom, Formula, GroundAtom, GroundTerm, PredicateId, Program, Rule, Term, Variable},
    self_restraint::SelfRestraintReliance,
};

#[derive(Debug, Clone)]
pub(super) struct Interpretation {
    atoms: Vec<GroundAtom>,
}

impl Interpretation {
    pub fn new(atoms: Vec<GroundAtom>) -> Self {
        Self { atoms }
    }

    pub fn extend(mut self, other: Interpretation) -> Self {
        self.atoms.extend(other.atoms);

        self
    }

    pub fn atoms(&self) -> &Vec<GroundAtom> {
        &self.atoms
    }
}

pub(super) type VariableAssignment = ClassAssignment<Variable, GroundTerm>;

pub(super) enum RelianceCheckResult {
    Abort,
    Success,
    Extend,
}

pub(super) trait RelianceImplementation {
    fn valid_assignment(
        term_source: &Term,
        term_target: &Term,
        assignment: &VariableAssignment,
    ) -> bool;

    fn check_conditions(
        mapping_domain: &Vec<usize>,
        rule_source: &Rule,
        rule_target: &Rule,
        assignment: &VariableAssignment,
    ) -> RelianceCheckResult;
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub(super) enum RelianceKind {
    Positive,
    Restraint,
}

pub(super) type RelianceGraph = LabeledGraph<usize, RelianceKind, Directed>;

pub(super) struct RelianceGraphConstructor {
    program: Program,
}

impl RelianceGraphConstructor {
    pub fn new(program: &crate::model::Program) -> Self {
        Self {
            program: Program::from_parsed_rules(program.rules()),
        }
    }

    fn unify_terms(
        assignment: &mut VariableAssignment,
        term_source: &Term,
        term_target: &Term,
    ) -> bool {
        match term_source {
            Term::Variable(variable_source) => match term_target {
                Term::Variable(variable_target) => {
                    assignment.merge_classes(variable_source, variable_target)
                }
                Term::Ground(ground_target) => {
                    assignment.assign_value(variable_source, *ground_target)
                }
            },
            Term::Ground(ground_source) => match term_target {
                Term::Variable(variable_target) => {
                    assignment.assign_value(variable_target, *ground_source)
                }
                Term::Ground(ground_target) => ground_source == ground_target,
            },
        }
    }

    fn extend<Implementation: RelianceImplementation>(
        mapping_domain: &mut Vec<usize>,
        assignment: &VariableAssignment,
        rule_source: &Rule,
        rule_target: &Rule,
        formula_source: &Formula,
        formula_target: &Formula,
    ) -> bool {
        let body_target_start = *mapping_domain.last().unwrap_or_else(|| &0);

        for (index_target, atom_target) in formula_target
            .atoms()
            .iter()
            .enumerate()
            .skip(body_target_start)
        {
            mapping_domain.push(index_target);

            for atom_source in formula_source.atoms() {
                if let Some(extended_assignment) =
                    Self::extend_assignment::<Implementation>(atom_source, atom_target, assignment)
                {
                    match Implementation::check_conditions(
                        mapping_domain,
                        rule_source,
                        rule_target,
                        &extended_assignment,
                    ) {
                        RelianceCheckResult::Abort => {
                            return false;
                        }
                        RelianceCheckResult::Success => {
                            return true;
                        }
                        RelianceCheckResult::Extend => {
                            return Self::extend::<Implementation>(
                                mapping_domain,
                                &extended_assignment,
                                rule_source,
                                rule_target,
                                formula_source,
                                formula_target,
                            )
                        }
                    }
                }
            }

            mapping_domain.pop();
        }

        false
    }

    fn extend_assignment<Implementation: RelianceImplementation>(
        atom_source: &Atom,
        atom_target: &Atom,
        assignment: &VariableAssignment,
    ) -> Option<VariableAssignment> {
        if !atom_source.compatible(atom_target) {
            return None;
        }

        let mut result = assignment.clone();

        for (term_source, term_target) in atom_source.terms.iter().zip(&atom_target.terms) {
            if !Self::unify_terms(&mut result, term_source, term_target) {
                return None;
            }

            if !Implementation::valid_assignment(term_source, term_target, assignment) {
                return None;
            }
        }

        Some(result)
    }

    pub fn build_reliance_graph(&self) -> RelianceGraph {
        let mut predicate_to_body = HashMap::<PredicateId, Vec<usize>>::new();
        let mut predicate_to_head_ex = HashMap::<PredicateId, Vec<usize>>::new();

        for (rule_index, rule) in self.program.rules().iter().enumerate() {
            for atom_body in rule.body().atoms() {
                let rules = predicate_to_body
                    .entry(atom_body.predicate.clone())
                    .or_default();
                rules.push(rule_index);
            }

            if !rule.is_existential() {
                continue;
            }

            for atom_head in rule.head().atoms() {
                let rules = predicate_to_head_ex
                    .entry(atom_head.predicate.clone())
                    .or_default();
                rules.push(rule_index);
            }
        }

        let mut reliance_graph = RelianceGraph::default();

        for (index_source, rule_source) in self.program.rules().iter().enumerate() {
            let mut finished_rules_positive = HashSet::<usize>::new();
            let mut finished_rules_restraint = HashSet::<usize>::new();

            for atom_head_source in rule_source.head().atoms() {
                // Positive Reliance
                if let Some(rule_target_indices) =
                    predicate_to_body.get(&atom_head_source.predicate)
                {
                    for &index_target in rule_target_indices {
                        if finished_rules_positive.contains(&index_target) {
                            continue;
                        }
                        finished_rules_positive.insert(index_target);

                        let rule_target = &self.program.rules()[index_target];

                        let assignment = VariableAssignment::default();
                        let mut mapping_domain = Vec::<usize>::default();

                        if Self::extend::<PositiveReliance>(
                            &mut mapping_domain,
                            &assignment,
                            rule_source,
                            rule_target,
                            rule_source.head(),
                            rule_target.body(),
                        ) {
                            reliance_graph.add_edge(
                                index_source,
                                index_target,
                                RelianceKind::Positive,
                            );
                        }
                    }
                }

                // Restraint Reliance
                if let Some(rule_target_indices) =
                    predicate_to_head_ex.get(&atom_head_source.predicate)
                {
                    for &index_target in rule_target_indices {
                        if finished_rules_restraint.contains(&index_target) {
                            continue;
                        }
                        finished_rules_restraint.insert(index_target);

                        let rule_target = &self.program.rules()[index_target];

                        let assignment = VariableAssignment::default();
                        let mut mapping_domain = Vec::<usize>::default();

                        if Self::extend::<RestraintReliance>(
                            &mut mapping_domain,
                            &assignment,
                            rule_source,
                            rule_target,
                            rule_source.head(),
                            rule_target.head(),
                        ) {
                            reliance_graph.add_edge(
                                index_source,
                                index_target,
                                RelianceKind::Restraint,
                            );
                        }

                        if index_source == index_target {
                            let assignment = VariableAssignment::default();
                            let mut mapping_domain = Vec::<usize>::default();

                            if Self::extend::<SelfRestraintReliance>(
                                &mut mapping_domain,
                                &assignment,
                                rule_source,
                                rule_target,
                                rule_source.head(),
                                rule_target.head(),
                            ) {
                                reliance_graph.add_edge(
                                    index_source,
                                    index_target,
                                    RelianceKind::Restraint,
                                );
                            }
                        }
                    }
                }
            }
        }

        reliance_graph
    }
}
