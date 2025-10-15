//! This module defines [GeneratorDuplicates].

use nemo_physical::management::execution_plan::ExecutionNodeRef;

use crate::{
    execution::planning_new::RuntimeInformation, rule_model::components::tag::Tag,
    table_manager::SubtableExecutionPlan,
};

/// Generator for execution plans node
/// that eliminate duplicates from an input node
#[derive(Debug)]
pub struct GeneratorDuplicates {
    /// Predicate from which the duplicates are eliminated
    predicate: Tag,
}

impl GeneratorDuplicates {
    /// Create a new [GeneratorDuplicates].
    pub fn new(predicate: Tag) -> Self {
        Self { predicate }
    }

    /// Append this operation to the plan.
    pub fn create_plan(
        &self,
        plan: &mut SubtableExecutionPlan,
        input_node: ExecutionNodeRef,
        runtime: &RuntimeInformation,
    ) -> ExecutionNodeRef {
        let nodes_old = runtime
            .table_manager
            .tables_in_range(&self.predicate, &(0..runtime.step_current))
            .iter()
            .map(|id| {
                plan.plan_mut()
                    .fetch_table(input_node.markers_cloned(), *id)
            })
            .collect::<Vec<_>>();

        let node_union = plan
            .plan_mut()
            .union(input_node.markers_cloned(), nodes_old);

        plan.plan_mut().subtract(input_node, vec![node_union])
    }
}
