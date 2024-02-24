//! This module contains a helper function to create
//! a node in an execution plan that realized negation.

use nemo_physical::management::execution_plan::{ExecutionNodeRef, ExecutionPlan};

use crate::{
    execution::rule_execution::VariableTranslation,
    model::{
        chase_model::{ChaseAtom, VariableAtom},
        Constraint,
    },
    table_manager::TableManager,
};

use super::{filter::node_filter, union::subplan_union};

/// Compute the appropriate execution plan to evaluate negated atoms.
pub(crate) fn node_negation(
    plan: &mut ExecutionPlan,
    table_manager: &TableManager,
    variable_translation: &VariableTranslation,
    node_main: ExecutionNodeRef,
    current_step_number: usize,
    subtracted_atoms: &[VariableAtom],
    subtracted_filters: &[Constraint],
) -> ExecutionNodeRef {
    let subtracted = subtracted_atoms
        .iter()
        .map(|atom| {
            let subtract_markers = variable_translation.operation_table(atom.terms().iter());
            let node = subplan_union(
                plan,
                table_manager,
                &atom.predicate(),
                0..current_step_number,
                subtract_markers.clone(),
            );

            // We simply apply all constraints to this node
            // Constraints which do not reference this atom will be filtered in the physical layer
            let node_filtered = node_filter(plan, variable_translation, node, subtracted_filters);

            // The tables may contain columns that are not part of `node_main`.
            // These need to be projected away.
            let markers_project_target = node_main.markers().restrict(&subtract_markers);
            plan.projectreorder(markers_project_target, node_filtered)
        })
        .collect();

    plan.subtract(node_main, subtracted)
}
