//! Functionality for implementing restraint reliances.

use super::{
    common::{RelianceCheckResult, RelianceImplementation, VariableAssignment},
    rules::{Atom, Rule, Term, Variable},
};

pub(super) struct RestraintReliance {}

// TODO: Probably move some of those into common
impl RestraintReliance {
    fn term_assigned_to_null(term: &Term, assignment: &VariableAssignment) -> bool {
        if let Term::Variable(variable_target) = term {
            if let Some(assigned_target) = assignment.value(variable_target) {
                return assigned_target.is_null();
            }
        }

        false
    }

    fn contains_null_assignments(atoms: &[Atom], assignment: &VariableAssignment) -> bool {
        for atom in atoms {
            for term in &atom.terms {
                if let Term::Variable(variable) = term {
                    if let Some(assigned_value) = assignment.value(variable) {
                        if assigned_value.is_null() {
                            return true;
                        }
                    }
                }
            }
        }

        false
    }

    /// ????
    fn check_existential_variable(atoms: &[Atom], assignment: &VariableAssignment) -> Option<usize> {
        for (atom_index, atom) in atoms.iter().enumerate() {
            for term in &atom.terms {
                if let Term::Variable(variable) = term {
                    if variable.is_existential() && assignment.value(variable).is_none() {
                        return Some(atom_index)
                    }
                }
            }
        }

        None
    }
}

impl RelianceImplementation for RestraintReliance {
    fn valid_assignment(
        term_source: &Term,
        term_target: &Term,
        assignment: &VariableAssignment,
    ) -> bool {
        // We do not allow any assignment of a universal variable to a null
        // TODO: Not sure why
        !(term_source.is_universal() && Self::term_assigned_to_null(term_target, assignment))
            && !(term_target.is_universal() && Self::term_assigned_to_null(term_source, assignment))
    }

    fn check_conditions(
        mapping_domain: &Vec<usize>,
        rule_source: &Rule,
        rule_target: &Rule,
        assignment: &VariableAssignment,
    ) -> RelianceCheckResult {
        let (head_target_mapped, head_target_notmapped) = rule_target.head().split(mapping_domain);

        if Self::contains_null_assignments(&rule_source.body().atoms(), assignment)
            || Self::contains_null_assignments(&rule_target.body().atoms(), assignment)
        {
            return RelianceCheckResult::Abort;
        }

        let unmapped_existential_count = Self::count_existential_variable(&head_target_notmapped.atoms(), assignment);
        if unmapped_existential_count > 0

        todo!()
    }
}
