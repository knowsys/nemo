//! Functionality for implementing positive reliances.

use crate::{
    execution::selection_strategy::dependency_graph::reliances::{
        rules::AssignmentRestriction, satisfy::is_satisfiable,
    },
    util::class_assignment::ClassValue,
};

use super::{
    common::{RelianceCheckResult, RelianceImplementation, RelianceType, VariableAssignment},
    rules::{Atom, Rule, Term},
};

pub(super) struct PositiveReliance {}

impl PositiveReliance {
    fn term_assigned_to_null(term: &Term, assignment: &VariableAssignment) -> bool {
        if let Term::Variable(variable_target) = term {
            if let ClassValue::Assigned(assigned_target) = assignment.value(variable_target) {
                return assigned_target.is_null();
            }
        }

        false
    }

    fn contains_null_assignments(atoms: &[Atom], assignment: &VariableAssignment) -> bool {
        for atom in atoms {
            for term in &atom.terms {
                if let Term::Variable(variable) = term {
                    if let ClassValue::Assigned(assigned_value) = assignment.value(variable) {
                        if assigned_value.is_null() {
                            return true;
                        }
                    }
                }
            }
        }

        false
    }
}

impl RelianceImplementation for PositiveReliance {
    fn valid_assignment(
        term_source: &Term,
        term_target: &Term,
        assignment: &VariableAssignment,
    ) -> bool {
        // We disallow assignments where a universal variable of the source rule
        // is assigned to some group of variables that will become nulls
        !(term_source.is_universal() && Self::term_assigned_to_null(term_target, assignment))
    }

    fn check_conditions(
        mapping_domain: &Vec<usize>,
        rule_source: &Rule,
        rule_target: &Rule,
        assignment: &VariableAssignment,
    ) -> RelianceCheckResult {
        let (_, body_target_notmapped) = rule_target.body().split(mapping_domain);

        if Self::contains_null_assignments(&rule_source.body().atoms(), assignment) {
            return RelianceCheckResult::Abort;
        }

        if Self::contains_null_assignments(&body_target_notmapped.atoms(), assignment) {
            return RelianceCheckResult::Extend;
        }

        let interpretation_before_source = rule_source
            .body()
            .apply_grounding(assignment)
            .extend(body_target_notmapped.apply_grounding(assignment));

        let interpretation_after_source = interpretation_before_source.clone().extend(
            rule_source
                .head()
                .apply_restricted()
                .apply_grounding(assignment),
        );

        if is_satisfiable(
            &rule_source
                .head()
                .apply_assignment(assignment, AssignmentRestriction::Universal),
            &interpretation_before_source,
        ) {
            return RelianceCheckResult::Extend;
        }

        if is_satisfiable(
            &rule_target
                .body()
                .apply_assignment(assignment, AssignmentRestriction::Unrestricted),
            &interpretation_before_source,
        ) {
            return RelianceCheckResult::Extend;
        }

        if is_satisfiable(
            &rule_target
                .head()
                .apply_assignment(assignment, AssignmentRestriction::Universal),
            &interpretation_after_source,
        ) {
            return RelianceCheckResult::Abort;
        }

        RelianceCheckResult::Success
    }

    fn formula_source(rule: &Rule) -> &super::rules::Formula {
        rule.head()
    }

    fn formula_target(rule: &Rule) -> &super::rules::Formula {
        rule.body()
    }

    fn reliance_type() -> RelianceType {
        RelianceType::Positive
    }
}
