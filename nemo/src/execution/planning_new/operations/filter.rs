//! This module defines [GeneratorFilter].

use nemo_physical::management::execution_plan::ExecutionNodeRef;

use crate::{
    execution::planning_new::{
        normalization::operation::Operation, operations::RuntimeInformation,
    },
    rule_model::components::term::primitive::variable::Variable,
    table_manager::SubtableExecutionPlan,
};

/// Generator of filter nodes in execution plans
#[derive(Debug)]
pub struct GeneratorFilter {
    /// Variables
    variables: Vec<Variable>,

    /// Filters
    filters: Vec<Operation>,
}

impl GeneratorFilter {
    /// Create a new [GeneratorFilter].
    ///
    /// This operation is applied to [ExecutionBinding]
    /// and applies all `operations` that only use bound variables,
    /// and then removes those operations from the list.
    pub fn new(input_variables: Vec<Variable>, operations: &mut Vec<Operation>) -> Self {
        let mut filters = Vec::<Operation>::default();

        operations.retain(|filter| {
            if filter
                .variables()
                .all(|variable| input_variables.contains(variable))
            {
                filters.push(filter.clone());
                false
            } else {
                true
            }
        });

        Self {
            variables: input_variables,
            filters,
        }
    }

    /// Append this operation to the plan.
    pub fn create_plan(
        &self,
        plan: &mut SubtableExecutionPlan,
        input_node: ExecutionNodeRef,
        runtime: &RuntimeInformation,
    ) -> ExecutionNodeRef {
        let filters = self
            .filters
            .iter()
            .map(|filter| filter.function_tree(&runtime.translation))
            .collect::<Vec<_>>();

        plan.plan_mut().filter(input_node, filters)
    }
}
