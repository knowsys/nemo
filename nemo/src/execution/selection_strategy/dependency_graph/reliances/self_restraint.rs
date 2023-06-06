//! Functionality for implementing self-restraint reliances.

use super::{
    common::{RelianceImplementation, VariableAssignment},
    rules::{Rule, Term},
};

pub(super) struct SelfRestraintReliance {}

impl RelianceImplementation for SelfRestraintReliance {
    fn valid_assignment(
        term_source: &Term,
        term_target: &Term,
        assignment: &VariableAssignment,
    ) -> bool {
        // No preliminary checks can be done
        // TODO: Really?
        true
    }

    fn check_conditions(
        mapping_domain: &Vec<usize>,
        rule_source: &Rule,
        rule_target: &Rule,
        assignment: &VariableAssignment,
    ) -> super::common::RelianceCheckResult {
        todo!()
    }
}
