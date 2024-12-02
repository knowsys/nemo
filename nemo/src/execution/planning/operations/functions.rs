//! This module contains a helper function
//! for computing a node in an execution plan,
//! which realizes a complex function application.

use nemo_physical::{
    management::execution_plan::{ExecutionNodeRef, ExecutionPlan},
    tabular::operations::FunctionAssignment,
};

use crate::{
    chase_model::components::operation::ChaseOperation,
    execution::rule_execution::VariableTranslation,
};

use super::operation::operation_term_to_function_tree;

/// Calculate helper structures that define the filters that need to be applied.
pub(crate) fn node_functions(
    plan: &mut ExecutionPlan,
    variable_translation: &VariableTranslation,
    subnode: ExecutionNodeRef,
    operations: &[ChaseOperation],
) -> ExecutionNodeRef {
    let mut output_markers = subnode.markers_cloned();
    let mut assignments = FunctionAssignment::new();

    for operation in operations {
        let marker = *variable_translation
            .get(operation.variable())
            .expect("All variables are known");
        let function_tree =
            operation_term_to_function_tree(variable_translation, operation.operation());

        assignments.insert(marker, function_tree);
        output_markers.push(marker);
    }

    plan.function(output_markers, subnode, assignments)
}
