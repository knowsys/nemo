//! Module for common functionality for the computation of different reliances.

use crate::util::class_assignment::ClassAssignment;

use super::rules::{GroundAtom, GroundTerm, Term, Variable};

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
