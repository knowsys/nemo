//! This module defines [GeneratorJoinSeminaive].

use nemo_physical::management::execution_plan::ExecutionNodeRef;

use crate::{
    chase_model::analysis::variable_order::VariableOrder,
    execution::planning_new::{
        normalization::atom::body::BodyAtom,
        operations::{RuntimeInformation, join::GeneratorJoin, union::UnionRange},
    },
    rule_model::components::term::primitive::variable::Variable,
    table_manager::SubtableExecutionPlan,
};

/// Generator for execution plan nodes
/// representing the seminaive join of body atoms
#[derive(Debug)]
pub struct GeneratorJoinSeminaive {
    /// Atoms that define the join
    atoms: Vec<BodyAtom>,

    /// Variable order
    order: VariableOrder,
}

impl GeneratorJoinSeminaive {
    /// Create a new [GeneratorJoinSeminaive].
    pub fn new(atoms: Vec<BodyAtom>, order: VariableOrder) -> Self {
        Self { atoms, order }
    }

    /// Append this operation to the plan.
    pub fn create_plan(
        &self,
        plan: &mut SubtableExecutionPlan,
        runtime: &RuntimeInformation,
    ) -> ExecutionNodeRef {
        let markers_result = runtime.translation.operation_table(self.order.iter());
        let mut node_result = plan.plan_mut().union_empty(markers_result);

        // Atoms of predicates that received new elements since the last rule application
        let mut updated_atoms = Vec::<usize>::new();

        for (atom_index, atom) in self.atoms.iter().enumerate() {
            let last_step = if let Some(step) = runtime.table_manager.last_step(&atom.predicate()) {
                step
            } else {
                // Table is empty and therefore the join will be empty
                return node_result;
            };

            if last_step > runtime.step_last_application {
                updated_atoms.push(atom_index);
            }
        }

        for mid in 0..updated_atoms.len() {
            let mut ranges = Vec::<UnionRange>::default();
            let mut updated_index: usize = 0;

            for atom_index in 0..self.atoms.len() {
                let range = if updated_atoms.contains(&atom_index) {
                    let range = if updated_index < mid {
                        UnionRange::Old
                    } else if updated_index == mid {
                        UnionRange::New
                    } else {
                        UnionRange::All
                    };

                    updated_index += 1;
                    range
                } else {
                    UnionRange::Old
                };

                ranges.push(range);
            }

            let node_join = GeneratorJoin::new(self.atoms.clone(), ranges, &self.order)
                .create_plan(plan, runtime);
            node_result.add_subnode(node_join);
        }

        node_result
    }

    /// Return the variables marking the column of the node
    /// created by `create_plan`.
    pub fn output_variables(&self) -> Vec<Variable> {
        self.order.as_ordered_list()
    }
}
