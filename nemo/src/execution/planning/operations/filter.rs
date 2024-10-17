//! This module contains a helper function for computing a node in an execution plan,
//! which realizes a filter operation.

use nemo_physical::{
    management::execution_plan::{ExecutionNodeRef, ExecutionPlan},
    tabular::operations::Filters,
};

use crate::{
    chase_model::components::filter::ChaseFilter, execution::rule_execution::VariableTranslation,
};

use super::operation::operation_term_to_function_tree;

/// Calculate helper structures that define the filters that need to be applied.
pub(crate) fn node_filter(
    plan: &mut ExecutionPlan,
    variable_translation: &VariableTranslation,
    subnode: ExecutionNodeRef,
    chase_filters: &[ChaseFilter],
) -> ExecutionNodeRef {
    let filters = chase_filters
        .iter()
        .map(|operation| operation_term_to_function_tree(variable_translation, operation.filter()))
        .collect::<Filters>();

    plan.filter(subnode, filters)
}
