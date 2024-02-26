//! This module contains a helper function
//! for computing a node in an execution plan,
//! which realizes a complex function application.

use std::collections::HashSet;

use nemo_physical::{
    management::execution_plan::{ExecutionNodeRef, ExecutionPlan},
    tabular::operations::{aggregate::AggregationInstructions, OperationTable},
};

use crate::{
    execution::rule_execution::VariableTranslation,
    model::{chase_model::ChaseAggregate, Variable},
};

/// Calculate helper structures that define the filters that need to be applied.
pub(crate) fn node_aggregate(
    plan: &mut ExecutionPlan,
    variable_translation: &VariableTranslation,
    mut subnode: ExecutionNodeRef,
    aggregates: &[ChaseAggregate],
    aggregate_group_by_variables: &Option<HashSet<Variable>>,
) -> ExecutionNodeRef {
    if aggregates.is_empty() {
        return subnode;
    }

    let aggregate_group_by_variables = aggregate_group_by_variables
        .as_ref()
        .expect("aggregate group-by variables should be set");

    assert!(
        aggregates.len() <= 1,
        "currently only one aggregate term per rule is supported"
    );

    for aggregate in aggregates {
        if reorder_required(
            variable_translation,
            aggregate_group_by_variables,
            aggregate,
        ) {
            todo!("reorder required for aggregate, this is not yet supported. adding additional distinct variables can currently fix this in some cases, but will change aggregate results.");

            // // Perform reordering of input trie scan and materialize into temporary trie

            // // Build variable order
            // // Start with group-by variables, followed by the aggregate input variables
            // // Try to preserve as much of the relative ordering as possible, to reduce the amount of swap that need to be done during the projecting/reordering
            // let input_variable_set = HashSet::from_iter(aggregate.input_variables.iter().cloned());
            // let mut variable_order_after_reordering =
            //     variable_order.restrict_to(aggregate_group_by_variables);
            // for input_variable in variable_order
            //     .restrict_to(&input_variable_set)
            //     .as_ordered_list()
            // {
            //     variable_order_after_reordering.push(input_variable);
            // }

            // let reordering_column_indices: Vec<_> = variable_order_after_reordering
            //     .as_ordered_list()
            //     .iter()
            //     .map(|variable| *variable_order.get(variable).unwrap())
            //     .collect();

            // let reordering =
            //     ProjectReordering::from_vector(reordering_column_indices, variable_order.len());

            // current_plan.add_temporary_table(node.clone(), "Subtable Aggregate Reorder");
            // node = current_plan.plan_mut().project(node, reordering);

            // // Update variable order to reordering
            // variable_order = variable_order_after_reordering;
        }

        let aggregate_output_column_marker = *variable_translation
            .get(&aggregate.output_variable)
            .expect("Aggregate output variable has to be known");

        let aggregated_column_marker = *variable_translation
            .get(&aggregate.input_variables[0])
            .expect("Aggregated variable has to be known");

        let mut last_distinct_column_index = 0;
        for input_variable in &aggregate.input_variables {
            last_distinct_column_index = usize::max(
                last_distinct_column_index,
                variable_translation
                    .get(input_variable)
                    .expect("aggregate input variable has to be known")
                    .0,
            )
        }

        // Update variables available aggregation
        // Remove distinct and unused variables of the aggregate
        let mut output_markers = OperationTable::new_unique(0);
        for group_by_variable in aggregate_group_by_variables {
            output_markers.push(
                *variable_translation
                    .get(group_by_variable)
                    .expect("aggregate group-by variable has to be known"),
            );
        }
        // Add aggregate output variable
        output_markers.push(aggregate_output_column_marker);

        subnode = plan.aggregate(
            output_markers,
            subnode,
            AggregationInstructions {
                aggregate_operation: aggregate.aggregate_operation,
                group_by_column_count: aggregate_group_by_variables.len(),
                aggregated_column_index: aggregated_column_marker.0,
                last_distinct_column_index,
            },
        )
    }

    subnode
}

// Checks whether the variable order has to be changed in order to apply the aggregate using the [`TrieScanAggregate`]
fn reorder_required(
    variable_translation: &VariableTranslation,
    aggregate_group_by_variables: &HashSet<Variable>,
    aggregate: &ChaseAggregate,
) -> bool {
    let group_by_variable_count = aggregate_group_by_variables.len();

    for group_by_variable in aggregate_group_by_variables {
        // Check that the group-by variables are exactly the first variables in the variable order
        // See [`AggregationInstructions`] and [`TrieScanAggregate`] documentation
        // Otherwise the columns would need to be reordered
        if variable_translation
            .get(group_by_variable)
            .expect("aggregate group-by variable has to be known")
            .0
            >= group_by_variable_count
        {
            return true;
        }
    }

    // Check that distinct/aggregated variables are exactly the variables after the group-by variables
    // An exception are idempotent processors, because it allows the promotion of peripheral to distinct variables
    if !aggregate.aggregate_operation.idempotent() {
        let maximum_input_variable_index =
            group_by_variable_count + aggregate.input_variables.len() - 1;

        for input_variable in &aggregate.input_variables {
            if variable_translation
                .get(input_variable)
                .expect("aggregate input variable has to be known")
                .0
                > maximum_input_variable_index
            {
                return true;
            }
        }
    }

    false
}

#[cfg(test)]
mod test {
    use std::collections::HashSet;

    use nemo_physical::aggregates::operation::AggregateOperation;

    use crate::{
        execution::{
            planning::operations::aggregate::reorder_required, rule_execution::VariableTranslation,
        },
        model::{chase_model::ChaseAggregate, LogicalAggregateOperation, Variable},
    };

    #[test]
    fn required_reordering() {
        let aggregated_input_variable = Variable::Universal("aggregated".to_string());

        let mut variable_translation = VariableTranslation::new();
        variable_translation.add_marker(Variable::Universal("group_by_1".to_string()));
        variable_translation.add_marker(Variable::Universal("group_by_2".to_string()));
        variable_translation.add_marker(Variable::Universal("other_1".to_string()));
        variable_translation.add_marker(aggregated_input_variable.clone());
        variable_translation.add_marker(Variable::Universal("other_2".to_string()));

        let idempotent_aggregate = ChaseAggregate {
            aggregate_operation: AggregateOperation::Min,
            input_variables: vec![aggregated_input_variable.clone()],
            output_variable: Variable::Universal("output_1".to_string()),
            logical_aggregate_operation: LogicalAggregateOperation::MinNumber,
        };

        assert!(!reorder_required(
            &variable_translation,
            &HashSet::new(),
            &idempotent_aggregate
        ),);

        let non_idempotent_aggregate = ChaseAggregate {
            aggregate_operation: AggregateOperation::Count,
            input_variables: vec![aggregated_input_variable.clone()],
            output_variable: Variable::Universal("output_1".to_string()),
            logical_aggregate_operation: LogicalAggregateOperation::CountValues,
        };

        assert!(reorder_required(
            &variable_translation,
            &HashSet::new(),
            &non_idempotent_aggregate
        ));

        let mut group_by_variables = HashSet::from_iter(vec![
            Variable::Universal("group_by_1".to_string()),
            Variable::Universal("group_by_2".to_string()),
        ]);

        assert!(!reorder_required(
            &variable_translation,
            &group_by_variables,
            &idempotent_aggregate
        ));

        assert!(reorder_required(
            &variable_translation,
            &group_by_variables,
            &non_idempotent_aggregate
        ));

        group_by_variables.insert(Variable::Universal("other_1".to_string()));

        assert!(!reorder_required(
            &variable_translation,
            &group_by_variables,
            &non_idempotent_aggregate
        ));

        assert!(reorder_required(
            &variable_translation,
            &HashSet::from_iter(vec![Variable::Universal("other_1".to_string()),].into_iter(),),
            &idempotent_aggregate
        ));
    }
}
