//! This module contains a helper function for computing the seminaive join

use nemo_physical::{
    management::execution_plan::{ExecutionNodeRef, ExecutionPlan},
    tabular::operations::OperationTable,
};

use crate::{
    chase_model::components::atom::{variable_atom::VariableAtom, ChaseAtom},
    execution::rule_execution::VariableTranslation,
    rule_model::components::IterableVariables,
    table_manager::TableManager,
};

use super::union::subplan_union;

/// Compute the appropriate execution plan to perform a join with the seminaive evaluation strategy.
pub(crate) fn node_join(
    plan: &mut ExecutionPlan,
    table_manager: &TableManager,
    variable_translation: &VariableTranslation,
    step_last_applied: usize,
    current_step_number: usize,
    input_atoms: &[VariableAtom],
    output_markers: OperationTable,
) -> ExecutionNodeRef {
    let mut node_result = plan.union_empty(output_markers.clone());

    // We divide the atoms of the body into two parts:
    //    * Main: Those atoms who received new elements since the last rule application
    //    * Side: Those atoms which did not receive new elements since the last rule application
    let mut side_atoms = Vec::new();
    let mut main_atoms = Vec::new();

    for atom in input_atoms {
        let last_step = if let Some(step) = table_manager.last_step(&atom.predicate()) {
            step
        } else {
            // Table is empty and therefore the join will be empty
            return node_result;
        };

        if last_step < step_last_applied {
            side_atoms.push(atom);
        } else {
            main_atoms.push(atom);
        }
    }

    if main_atoms.is_empty() {
        // No updates, hence the join is empty
        return node_result;
    }

    for atom_index in 0..main_atoms.len() {
        let mut seminaive_node = plan.join_empty(output_markers.clone());

        // For every atom that did not receive any update since the last rule application take all available elements
        for atom in &side_atoms {
            let atom_markers = variable_translation.operation_table(atom.variables());
            let subnode = subplan_union(
                plan,
                table_manager,
                &atom.predicate(),
                0..step_last_applied,
                atom_markers,
            );

            seminaive_node.add_subnode(subnode);
        }

        // For every atom before the mid point we take all the tables until the current `rule_step`
        for &atom in main_atoms.iter().take(atom_index) {
            let atom_markers = variable_translation.operation_table(atom.variables());
            let subnode = subplan_union(
                plan,
                table_manager,
                &atom.predicate(),
                0..current_step_number,
                atom_markers,
            );

            seminaive_node.add_subnode(subnode);
        }

        // For the middle atom we only take the new tables
        let midnode = subplan_union(
            plan,
            table_manager,
            &main_atoms[atom_index].predicate(),
            step_last_applied..current_step_number,
            variable_translation.operation_table(main_atoms[atom_index].variables()),
        );
        seminaive_node.add_subnode(midnode);

        // For every atom past the mid point we take only the old tables
        for atom in main_atoms.iter().skip(atom_index + 1) {
            let atom_markers = variable_translation.operation_table(atom.variables());
            let subnode = subplan_union(
                plan,
                table_manager,
                &atom.predicate(),
                0..step_last_applied,
                atom_markers,
            );

            seminaive_node.add_subnode(subnode);
        }

        node_result.add_subnode(seminaive_node);
    }

    node_result
}
