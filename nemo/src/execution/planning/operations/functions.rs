//! This module contains a helper function
//! for computing a node in an execution plan,
//! which realizes a complex function application.

use nemo_physical::{
    management::execution_plan::{ExecutionNodeRef, ExecutionPlan},
    tabular::operations::FunctionAssignment,
};

use crate::{execution::rule_execution::VariableTranslation, model::chase_model::Constructor};

use super::term::term_to_function_tree;

/// Calculate helper structures that define the filters that need to be applied.
pub(crate) fn node_functions(
    plan: &mut ExecutionPlan,
    variable_translation: &VariableTranslation,
    subnode: ExecutionNodeRef,
    constructors: &[Constructor],
) -> ExecutionNodeRef {
    let mut output_markers = subnode.markers();
    let mut assignments = FunctionAssignment::new();

    for constructor in constructors {
        let marker = *variable_translation
            .get(constructor.variable())
            .expect("All variables are known");
        let function_tree = term_to_function_tree(variable_translation, constructor.term());

        assignments.insert(marker, function_tree);
        output_markers.push(marker);
    }

    plan.function(output_markers, subnode, assignments)
}
