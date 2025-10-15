//! This module defines [GeneratorRestrictedFrontier].

use nemo_physical::management::execution_plan::ExecutionNodeRef;

use crate::{
    execution::planning_new::RuntimeInformation,
    rule_model::components::term::primitive::variable::Variable,
    table_manager::SubtableExecutionPlan,
};

/// Generator for execution plan nodes that project
/// their input to the frontier variables of the rule
#[derive(Debug)]
pub struct GeneratorRestrictedFrontier {
    /// Frontier variables
    variables: Vec<Variable>,
}

impl GeneratorRestrictedFrontier {
    /// Create a new [GeneratorRestrictedFrontier].
    pub fn new(variables: Vec<Variable>) -> Self {
        Self { variables }
    }

    /// Append this operation to the plan.
    pub fn create_plan(
        &self,
        plan: &mut SubtableExecutionPlan,
        input_node: ExecutionNodeRef,
        runtime: &RuntimeInformation,
    ) -> ExecutionNodeRef {
        let markers = runtime.translation.operation_table(self.variables.iter());
        plan.plan_mut().projectreorder(markers, input_node)
    }
}
