//! This module contains a helper function
//! for computing a node in an execution plan,
//! which realizes a complex function application.

use std::collections::HashSet;

use nemo_physical::management::execution_plan::{ExecutionNodeRef, ExecutionPlan};

use crate::{
    execution::rule_execution::VariableTranslation,
    model::{chase_model::ChaseAggregate, Variable},
};

/// Calculate helper structures that define the filters that need to be applied.
pub(crate) fn node_aggregate(
    _plan: &mut ExecutionPlan,
    _variable_translation: &VariableTranslation,
    subnode: ExecutionNodeRef,
    aggregates: &[ChaseAggregate],
    _aggregate_group_by_variables: &Option<HashSet<Variable>>,
) -> ExecutionNodeRef {
    if aggregates.is_empty() {
        return subnode;
    }

    todo!("aggregates are WIP");
    // let mut output_markers = subnode.markers();
    // let mut assignments = FunctionAssignment::new();

    // for aggregate in aggregates {
    //     let marker = *variable_translation
    //         .get(constructor.variable())
    //         .expect("All variables are known");
    //     let function_tree = term_to_function_tree(variable_translation, constructor.term());

    //     assignments.insert(marker, function_tree);
    //     output_markers.push(marker);
    // }

    // plan.aggregate(output_markers, subnode, assignments)
}
