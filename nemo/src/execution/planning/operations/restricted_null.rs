//! This module defines [GeneratorRestrictedNull].

use nemo_physical::management::execution_plan::ExecutionNodeRef;

use crate::{
    execution::planning::{RuntimeInformation, normalization::atom::head::HeadAtom},
    rule_model::components::term::primitive::variable::Variable,
    table_manager::SubtableExecutionPlan,
};

/// Generator for execution plan nodes
/// that append null columns to a given table
#[derive(Debug)]
pub struct GeneratorRestrictedNull {
    /// Existential variables
    existentials: Vec<Variable>,
}

impl GeneratorRestrictedNull {
    /// Create a new [GeneratorRestrictedNull].
    pub fn new(head: &[HeadAtom]) -> Self {
        let existentials = head
            .iter()
            .flat_map(|atom| atom.variables_existential().cloned())
            .collect::<Vec<_>>();

        Self { existentials }
    }

    /// Append this operation to the plan.
    pub fn create_plan(
        &self,
        plan: &mut SubtableExecutionPlan,
        input_node: ExecutionNodeRef,
        runtime: &RuntimeInformation,
    ) -> ExecutionNodeRef {
        let mut markers = input_node.markers_cloned();

        for variable in &self.existentials {
            let marker = *runtime
                .translation
                .get(variable)
                .expect("incomplete translation");
            markers.push(marker);
        }

        plan.plan_mut().null(markers, input_node)
    }
}
