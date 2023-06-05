//! Module for common functionality for the computation of different reliances.

use crate::util::class_assignment::ClassAssignment;

use super::rules::{Formula, GroundAtom, GroundTerm, Rule, Term, Variable};

#[derive(Debug)]
pub(super) struct Interpretation {
    atoms: Vec<GroundAtom>,
}

impl Interpretation {
    pub fn new(atoms: Vec<GroundAtom>) -> Self {
        Self { atoms }
    }

    pub fn extend(&mut self, other: Interpretation) {
        self.atoms.extend(other.atoms)
    }

    pub fn atoms(&self) -> &Vec<GroundAtom> {
        &self.atoms
    }
}

/// TODO: Description
pub(super) type VariableAssignment = ClassAssignment<Variable, GroundTerm>;

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
            Term::Ground(ground_target) => assignment.assign_value(variable_source, *ground_target),
        },
        Term::Ground(ground_source) => match term_target {
            Term::Variable(variable_target) => {
                assignment.assign_value(variable_target, *ground_source)
            }
            Term::Ground(ground_target) => ground_source == ground_target,
        },
    }
}

fn extend(
    mapping_domain: &mut Vec<usize>,
    formula_source: &Formula,
    formula_target: &Formula,
    assignment: &VariableAssignment,
) -> bool {
    let body_target_start: usize = *mapping_domain.last().unwrap_or_else(|| &0);

    for (index_target, atom_target) in formula_target
        .atoms()
        .iter()
        .enumerate()
        .skip(body_target_start)
    {
        mapping_domain.push(index_target);
    }

    todo!()
}
