//! This module contains a helper function
//! for computing a node in an execution plan,
//! which realizes a complex function application.

use nemo_physical::{
    management::execution_plan::{ExecutionNodeRef, ExecutionPlan},
    tabular::operations::{aggregate::AggregateAssignment, OperationColumnMarker, OperationTable},
};

use crate::{
    chase_model::components::aggregate::ChaseAggregate,
    execution::rule_execution::VariableTranslation,
};

fn operations_tables(
    input: &OperationTable,
    aggregate_input_column: &OperationColumnMarker,
    aggregate_output_column: &OperationColumnMarker,
    distinct_columns: &[OperationColumnMarker],
    group_by_columns: &[OperationColumnMarker],
) -> (OperationTable, OperationTable) {
    // Create input order that produces intended output order

    let mut ordered_input = OperationTable::default();
    let mut ordered_output = OperationTable::default();
    for column in input.iter() {
        if group_by_columns.contains(column) {
            if column != aggregate_input_column {
                ordered_input.push(*column);
            }

            ordered_output.push(*column);
        }
    }

    ordered_input.push(*aggregate_input_column);
    ordered_output.push(*aggregate_output_column);

    for column in input.iter() {
        if distinct_columns.contains(column) {
            ordered_input.push(*column);
        }
    }

    (ordered_input, ordered_output)
}

/// Calculate helper structures that define the filters that need to be applied.
pub(crate) fn node_aggregate(
    plan: &mut ExecutionPlan,
    variable_translation: &VariableTranslation,
    subnode: ExecutionNodeRef,
    aggregate: &ChaseAggregate,
) -> ExecutionNodeRef {
    let aggregate_input_column = *variable_translation
        .get(aggregate.input_variable())
        .expect("aggregated variable has to be known");
    let aggregate_output_column = *variable_translation
        .get(aggregate.output_variable())
        .expect("aggregate output has to be known");

    let distinct_columns: Vec<_> = aggregate
        .distinct_variables()
        .iter()
        .map(|variable| {
            *variable_translation
                .get(variable)
                .expect("aggregate distinct variables have to be known")
        })
        .collect();

    let group_by_columns: Vec<_> = aggregate
        .group_by_variables()
        .iter()
        .map(|variable| {
            *variable_translation
                .get(variable)
                .expect("aggregate group-by variables have to be known")
        })
        .collect();

    let unordered_input_markers = subnode.markers_cloned();
    let (ordered_input_markers, output_markers) = operations_tables(
        &unordered_input_markers,
        &aggregate_input_column,
        &aggregate_output_column,
        &distinct_columns,
        &group_by_columns,
    );

    let input_node = plan.projectreorder(ordered_input_markers, subnode);

    plan.aggregate(
        output_markers,
        input_node,
        AggregateAssignment {
            aggregate_operation: aggregate.aggregate_kind().into(),
            distinct_columns,
            group_by_columns,
            aggregated_column: aggregate_input_column,
        },
    )
}
