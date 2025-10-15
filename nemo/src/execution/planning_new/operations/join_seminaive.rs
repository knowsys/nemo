//! This module defines [GeneratorJoinSeminaive].

use nemo_physical::management::execution_plan::ExecutionNodeRef;

use crate::{
    chase_model::analysis::variable_order::VariableOrder,
    execution::planning_new::{
        RuntimeInformation,
        normalization::atom::body::BodyAtom,
        operations::{join::GeneratorJoin, union::UnionRange},
    },
    rule_model::components::term::primitive::variable::Variable,
    table_manager::SubtableExecutionPlan,
};

/// Generator for execution plan nodes
/// representing the seminaive join of body atoms
#[derive(Debug)]
pub struct GeneratorJoinSeminaive {
    /// Seminaive variants
    variants: Vec<GeneratorJoin>,

    /// Variable order
    order: VariableOrder,
}

impl GeneratorJoinSeminaive {
    /// Create a new [GeneratorJoinSeminaive].
    pub fn new(atoms: Vec<BodyAtom>, order: &VariableOrder) -> Self {
        let variants = Self::create_variants(atoms, order, true);

        Self {
            variants,
            order: order.clone(),
        }
    }

    /// Create a new [GeneratorJoinSeminaive]
    /// where tables created in the during the last rule application step
    /// are considered as `old` tables.
    pub fn new_exclusive(atoms: Vec<BodyAtom>, order: &VariableOrder) -> Self {
        let variants = Self::create_variants(atoms, order, false);

        Self {
            variants,
            order: order.clone(),
        }
    }

    /// Create a generator for each seminaive variant.
    fn create_variants(
        atoms: Vec<BodyAtom>,
        order: &VariableOrder,
        last_is_new: bool,
    ) -> Vec<GeneratorJoin> {
        let mut result = Vec::<GeneratorJoin>::default();

        for mid in 0..atoms.len() {
            let mut ranges = Vec::<UnionRange>::default();

            for atom_index in 0..atoms.len() {
                let range = if atom_index < mid {
                    Self::range_old(last_is_new)
                } else if atom_index == mid {
                    Self::range_new(last_is_new)
                } else {
                    UnionRange::All
                };

                ranges.push(range);
            }

            let node_join = GeneratorJoin::new(atoms.clone(), ranges, order);
            result.push(node_join);
        }

        result
    }

    /// Return a [UnionRange] taking into account
    /// whether a table derived at the last rule application
    /// should be considered as `new`.
    fn range_new(last_is_new: bool) -> UnionRange {
        if last_is_new {
            UnionRange::New
        } else {
            UnionRange::NewExclusive
        }
    }

    /// Return a [UnionRange] taking into account
    /// whether a table derived at the last rule application
    /// should be considered as `old`.
    fn range_old(last_is_new: bool) -> UnionRange {
        if last_is_new {
            UnionRange::Old
        } else {
            UnionRange::OldInclusive
        }
    }

    /// Append this operation to the plan.
    pub fn create_plan(
        &self,
        plan: &mut SubtableExecutionPlan,
        runtime: &RuntimeInformation,
    ) -> ExecutionNodeRef {
        let markers_result = runtime.translation.operation_table(self.order.iter());
        let mut node_result = plan.plan_mut().union_empty(markers_result);

        for variant in &self.variants {
            if variant.is_empty(runtime) {
                continue;
            }

            let node_join = variant.create_plan(plan, runtime);
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
