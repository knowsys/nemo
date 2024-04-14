//! This module contains a helper function
//! for computing a node in an execution plan,
//! which realizes a complex function application.

use nemo_physical::{
    management::execution_plan::{ExecutionNodeRef, ExecutionPlan},
    tabular::operations::{aggregate::AggregateAssignment, OperationTable},
};

use crate::{execution::rule_execution::VariableTranslation, model::chase_model::ChaseAggregate};

/// Calculate helper structures that define the filters that need to be applied.
pub(crate) fn node_aggregate(
    plan: &mut ExecutionPlan,
    variable_translation: &VariableTranslation,
    subnode: ExecutionNodeRef,
    aggregate: &ChaseAggregate,
) -> ExecutionNodeRef {
    let aggregated_column = *variable_translation
        .get(&aggregate.input_variable)
        .expect("aggregated variable has to be known");

    let distinct_columns: Vec<_> = aggregate
        .distinct_variables
        .iter()
        .map(|variable| {
            *variable_translation
                .get(variable)
                .expect("aggregate distinct variables have to be known")
        })
        .collect();

    let group_by_columns: Vec<_> = aggregate
        .group_by_variables
        .iter()
        .map(|variable| {
            *variable_translation
                .get(variable)
                .expect("aggregate group-by variables have to be known")
        })
        .collect();

    // Update variables available after aggregation
    // Remove distinct and unused variables of the aggregate
    let mut output_markers = OperationTable::new_unique(0);
    for group_by_marker in group_by_columns.iter() {
        output_markers.push(*group_by_marker);
    }
    // Add aggregate output variable
    output_markers.push(
        *variable_translation
            .get(&aggregate.output_variable)
            .expect("aggregate output variable has to be known"),
    );

    plan.aggregate(
        output_markers,
        subnode,
        AggregateAssignment {
            aggregate_operation: aggregate.aggregate_operation,
            distinct_columns,
            group_by_columns,
            aggregated_column,
        },
    )
}
