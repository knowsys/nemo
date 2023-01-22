//! Defines the function that performs a seminaive join over a list of atoms.

use std::collections::HashSet;

use crate::{
    logical::{
        model::{Atom, Filter, Variable},
        program_analysis::variable_order::VariableOrder,
        table_manager::TableKey,
        TableManager,
    },
    physical::{
        dictionary::Dictionary,
        management::execution_plan::{ExecutionNodeRef, ExecutionTree},
        tabular::operations::triescan_join::JoinBinding,
        util::Reordering,
    },
};

use super::plan_util::{compute_filters, join_binding, order_atom, subtree_union};

// Calculate a subtree consiting of join representing one variant of an seminaive evaluation.
#[allow(clippy::too_many_arguments)]
fn subtree_join<Dict: Dictionary>(
    tree: &mut ExecutionTree<TableKey>,
    manager: &TableManager<Dict>,
    side_atoms: &[&Atom],
    main_atoms: &[&Atom],
    side_orders: &[Reordering],
    main_orders: &[Reordering],
    rule_step: usize,
    overall_step: usize,
    mid: usize,
    join_binding: &JoinBinding,
) -> ExecutionNodeRef<TableKey> {
    let mut join_node = tree.join_empty(join_binding.clone());

    // For every atom that did not receive any update since the last rule application take all available elements
    for (atom_index, atom) in side_atoms.iter().enumerate() {
        let subnode = subtree_union(
            tree,
            manager,
            atom.predicate(),
            0..rule_step,
            &side_orders[atom_index],
        );

        join_node.add_subnode(subnode);
    }

    // For every atom before the mid point we take all the tables until the current `rule_step`
    for (atom_index, atom) in main_atoms.iter().take(mid).enumerate() {
        let subnode = subtree_union(
            tree,
            manager,
            atom.predicate(),
            0..overall_step,
            &main_orders[atom_index],
        );

        join_node.add_subnode(subnode);
    }

    // For the middle atom we only take the new tables
    let midnode = subtree_union(
        tree,
        manager,
        main_atoms[mid].predicate(),
        rule_step..overall_step,
        &main_orders[mid],
    );

    join_node.add_subnode(midnode);

    // For every atom past the mid point we take only the old tables
    for (atom_index, atom) in main_atoms.iter().enumerate().skip(mid + 1) {
        let subnode = subtree_union(
            tree,
            manager,
            atom.predicate(),
            0..rule_step,
            &main_orders[atom_index],
        );

        join_node.add_subnode(subnode);
    }

    join_node
}

/// Given a list of atoms and filters compute the appropriate execution tree to perform the join of those atoms
/// with the seminaive evaluation strategy.
pub fn seminaive_join<Dict: Dictionary>(
    tree: &mut ExecutionTree<TableKey>,
    table_manager: &TableManager<Dict>,
    step_last_applied: usize,
    current_step_number: usize,
    variable_order: &VariableOrder,
    variables: &HashSet<Variable>,
    atoms: &[&Atom],
    filters: &[&Filter],
) -> Option<ExecutionNodeRef<TableKey>> {
    // We divide the atoms of the body into two parts:
    //    * Main: Those atoms who received new elements since the last rule application
    //    * Side: Those atoms which did not receive new elements since the last rule application
    let mut body_side = Vec::<&Atom>::new();
    let mut body_main = Vec::<&Atom>::new();

    for &atom in atoms {
        if let Some(last_step) = table_manager.predicate_last_step(atom.predicate()) {
            if last_step < step_last_applied {
                body_side.push(atom);
            } else {
                body_main.push(atom);
            }
        } else {
            return None;
        }
    }

    if body_main.is_empty() {
        return None;
    }

    // The variable order forces a specific [`ColumnOrder`] on each table
    // Needs to be done for the main and side atoms
    let main_orders: Vec<Reordering> = body_main
        .iter()
        .map(|&a| order_atom(a, &variable_order))
        .collect();
    let side_orders: Vec<Reordering> = body_side
        .iter()
        .map(|&a| order_atom(a, &variable_order))
        .collect();

    // Join binding needs to be computed for both types of atoms as well
    let side_binding = body_side
        .iter()
        .enumerate()
        .map(|(i, &a)| join_binding(a, &side_orders[i], &variable_order));
    let main_binding = body_main
        .iter()
        .enumerate()
        .map(|(i, &a)| join_binding(a, &main_orders[i], &variable_order));
    // We then combine the bindings into one
    let join_binding: JoinBinding = side_binding.chain(main_binding).collect();

    // Now we can finally calculate the execution tree
    let mut seminaive_union = tree.union_empty();
    for atom_index in 0..body_main.len() {
        let seminaive_node = subtree_join(
            tree,
            table_manager,
            &body_side,
            &body_main,
            &side_orders,
            &main_orders,
            step_last_applied,
            current_step_number,
            atom_index,
            &join_binding,
        );

        seminaive_union.add_subnode(seminaive_node);
    }

    // Apply filters
    let (filter_classes, filter_assignments) =
        compute_filters(variables, &variable_order, &filters);

    let node_select_value = tree.select_value(seminaive_union, filter_assignments);
    let node_select_equal = tree.select_equal(node_select_value, filter_classes);

    Some(node_select_equal)
}
