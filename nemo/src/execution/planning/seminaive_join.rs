//! Defines the function that performs a seminaive join over a list of atoms.

use std::collections::HashMap;

use nemo_physical::{
    management::execution_plan::{ExecutionNodeRef, ExecutionPlan},
    tabular::operations::JoinBindings,
};

use crate::{
    model::{
        chase_model::{ChaseAtom, Constraint, VariableAtom},
        PrimitiveType, Variable,
    },
    program_analysis::variable_order::VariableOrder,
    table_manager::TableManager,
};

use super::plan_util::{atom_binding, compute_filters, subplan_union};

/// Generator for creating excution plans for seminaive joins of a fixed set of [`ChaseAtom`]s and [`Filter`]s.
#[derive(Debug)]
pub struct SeminaiveJoinGenerator {
    /// logical types of the variables
    pub variable_types: HashMap<Variable, PrimitiveType>,
    /// the atoms to join
    pub atoms: Vec<VariableAtom>,
    /// the filters to apply
    pub constraints: Vec<Constraint>,
}

impl SeminaiveJoinGenerator {
    /// Compute the appropriate execution tree to perform the join with the seminaive evaluation strategy.
    /// Note: The [`VariableOrder`] must only contain variables that occur in the `atoms` parameter.
    pub(crate) fn seminaive_join(
        &self,
        plan: &mut ExecutionPlan,
        table_manager: &TableManager,
        step_last_applied: usize,
        current_step_number: usize,
        variable_order: &VariableOrder,
    ) -> ExecutionNodeRef {
        // We divide the atoms of the body into two parts:
        //    * Main: Those atoms who received new elements since the last rule application
        //    * Side: Those atoms which did not receive new elements since the last rule application
        let mut side_atoms = Vec::new();
        let mut main_atoms = Vec::new();

        let mut side_binding = Vec::new();
        let mut main_binding = Vec::new();

        for atom in &self.atoms {
            let last_step = if let Some(step) = table_manager.last_step(atom.predicate()) {
                step
            } else {
                return plan.union_empty();
            };

            let binding = atom_binding(atom, variable_order);

            if last_step < step_last_applied {
                side_binding.push(binding);
                side_atoms.push(atom.predicate());
            } else {
                main_binding.push(binding);
                main_atoms.push(atom.predicate());
            }
        }

        if main_atoms.is_empty() {
            return plan.union_empty();
        }

        // We then combine the bindings into one
        let join_binding: JoinBindings = side_binding.into_iter().chain(main_binding).collect();

        // Now we can finally calculate the execution tree
        let mut seminaive_union = plan.union_empty();
        for atom_index in 0..main_atoms.len() {
            let mut seminaive_node = plan.join_empty(join_binding.clone());

            // For every atom that did not receive any update since the last rule application take all available elements
            for predicate in side_atoms.iter() {
                let subnode = subplan_union(
                    plan,
                    table_manager,
                    predicate.clone(),
                    &(0..step_last_applied),
                );
                seminaive_node.add_subnode(subnode);
            }

            // For every atom before the mid point we take all the tables until the current `rule_step`
            for predicate in main_atoms.iter().take(atom_index) {
                let subnode = subplan_union(
                    plan,
                    table_manager,
                    predicate.clone(),
                    &(0..current_step_number),
                );
                seminaive_node.add_subnode(subnode);
            }

            // For the middle atom we only take the new tables
            let midnode = subplan_union(
                plan,
                table_manager,
                main_atoms[atom_index].clone(),
                &(step_last_applied..current_step_number),
            );
            seminaive_node.add_subnode(midnode);

            // For every atom past the mid point we take only the old tables
            for predicate in main_atoms.iter().skip(atom_index + 1) {
                let subnode = subplan_union(
                    plan,
                    table_manager,
                    predicate.clone(),
                    &(0..step_last_applied),
                );
                seminaive_node.add_subnode(subnode);
            }

            seminaive_union.add_subnode(seminaive_node);
        }

        // Apply filters
        let conditions = compute_filters(variable_order, &self.constraints, &self.variable_types);

        plan.filter(seminaive_union, conditions)
    }
}
