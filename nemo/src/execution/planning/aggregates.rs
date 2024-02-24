use std::collections::HashSet;

use nemo_physical::management::execution_plan::ExecutionNodeRef;
use nemo_physical::tabular::operations::aggregate::AggregationInstructions;

use crate::{
    model::{chase_model::ChaseAggregate, Variable},
    program_analysis::variable_order::VariableOrder,
    table_manager::SubtableExecutionPlan,
};

pub(super) fn generate_node_aggregate(
    current_plan: &mut SubtableExecutionPlan,
    mut variable_order: VariableOrder,
    mut node: ExecutionNodeRef,
    aggregates: &Vec<ChaseAggregate>,
    aggregate_group_by_variables: &HashSet<Variable>,
) -> (ExecutionNodeRef, VariableOrder) {
    assert!(
        aggregates.len() <= 1,
        "currently only one aggregate term per rule is supported"
    );
    for aggregate in aggregates {
        if reorder_required(&variable_order, aggregate_group_by_variables, aggregate) {
            // Perform reordering of input trie scan and materialize into temporary trie

            // Build variable order
            // Start with group-by variables, followed by the aggregate input variables
            // Try to preserve as much of the relative ordering as possible, to reduce the amount of swap that need to be done during the projecting/reordering
            let input_variable_set = HashSet::from_iter(aggregate.input_variables.iter().cloned());
            let mut variable_order_after_reordering =
                variable_order.restrict_to(aggregate_group_by_variables);
            for input_variable in variable_order
                .restrict_to(&input_variable_set)
                .as_ordered_list()
            {
                variable_order_after_reordering.push(input_variable);
            }

            let reordering_column_indices: Vec<_> = variable_order_after_reordering
                .as_ordered_list()
                .iter()
                .map(|variable| *variable_order.get(variable).unwrap())
                .collect();

            current_plan.add_temporary_table(node.clone(), "Subtable Aggregate Reorder");
            node = current_plan.plan_mut().project(node, reordering);
            let reordering =
                ProjectReordering::from_vector(reordering_column_indices, variable_order.len());

            node = current_plan.plan_mut().project(node, reordering);

            current_plan.add_temporary_table(node.clone(), "Subtable Aggregate Reorder");

            // Update variable order to reordering
            variable_order = variable_order_after_reordering;
        }

        let aggregated_column_index = *variable_order
            .get(aggregate.aggregated_input_variable())
            .expect("variable that is aggregated has to be in the variable order");

        // Determine `last_distinct_column_index`
        let mut last_distinct_column_index = 0;
        for (index, variable) in variable_order.iter().enumerate() {
            if aggregate.input_variables.contains(variable) {
                last_distinct_column_index = index;
            }
        }

        node = current_plan.plan_mut().aggregate(
            node,
            AggregationInstructions {
                aggregate_operation: aggregate.aggregate_operation,
                group_by_column_count: aggregate_group_by_variables.len(),
                aggregated_column_index,
                last_distinct_column_index,
            },
        );

        // Update variable order after aggregation
        // Remove distinct and unused variables of the aggregate
        variable_order = variable_order.restrict_to(aggregate_group_by_variables);
        // Add aggregate output variable
        variable_order.push(aggregate.output_variable.clone());
    }

    (node, variable_order)
}

// Checks whether the variable order has to be changed in order to apply the aggregate using the [`TrieScanAggregate`]
fn reorder_required(
    variable_order: &VariableOrder,
    aggregate_group_by_variables: &HashSet<Variable>,
    aggregate: &ChaseAggregate,
) -> bool {
    let idempotent = aggregate.aggregate_operation.idempotent::<u64>();
    let group_by_variable_count = aggregate_group_by_variables.len();

    // Whether a peripheral column has been seen (see [`TrieScanAggregate`])
    let mut after_peripheral_variable = false;

    for (index, variable) in variable_order.iter().enumerate() {
        if index < group_by_variable_count {
            // Check that the group-by variables are exactly the first variables in the variable order
            // See [`AggregationInstructions`] and [`TrieScanAggregate`] documentation
            // Otherwise the columns would need to be reordered
            if !aggregate_group_by_variables.contains(variable) {
                return true;
            }
        } else if !idempotent {
            // Check that distinct/aggregated variables are exactly the variables after the group-by variables
            // An exception are idempotent processors, because it allows the promotion of peripheral to distinct variables
            if aggregate.input_variables.contains(variable) {
                if after_peripheral_variable {
                    return true;
                }
            } else {
                after_peripheral_variable = true;
            }
        }
    }

    false
}

// #[cfg(test)]
// mod test {
//     use std::collections::HashSet;

//     use nemo_physical::aggregates::operation::AggregateOperation;

//     use crate::{
//         execution::planning::aggregates::reorder_required,
//         model::{chase_model::ChaseAggregate, Identifier, LogicalAggregateOperation, Variable},
//         program_analysis::variable_order::VariableOrder,
//     };

//     #[test]
//     fn required_reordering() {
//         let aggregated_input_variable = Variable::Universal(Identifier("aggregated".to_string()));

//         let mut variable_order = VariableOrder::new();
//         variable_order.push(Variable::Universal(Identifier("group_by_1".to_string())));
//         variable_order.push(Variable::Universal(Identifier("group_by_2".to_string())));
//         variable_order.push(Variable::Universal(Identifier("other_1".to_string())));
//         variable_order.push(aggregated_input_variable.clone());
//         variable_order.push(Variable::Universal(Identifier("other_2".to_string())));

//         let idempotent_aggregate = ChaseAggregate {
//             aggregate_operation: AggregateOperation::Min,
//             input_variables: vec![aggregated_input_variable.clone()],
//             output_variable: Variable::Universal(Identifier("output_1".to_string())),
//             logical_aggregate_operation: LogicalAggregateOperation::MinNumber,
//         };

//         assert!(!reorder_required(
//             &variable_order,
//             &HashSet::new(),
//             &idempotent_aggregate
//         ),);

//         let non_idempotent_aggregate = ChaseAggregate {
//             aggregate_operation: AggregateOperation::Count,
//             input_variables: vec![aggregated_input_variable.clone()],
//             output_variable: Variable::Universal(Identifier("output_1".to_string())),
//             logical_aggregate_operation: LogicalAggregateOperation::CountValues,
//         };

//         assert!(reorder_required(
//             &variable_order,
//             &HashSet::new(),
//             &non_idempotent_aggregate
//         ));

//         let mut group_by_variables = HashSet::from_iter(vec![
//             Variable::Universal(Identifier("group_by_1".to_string())),
//             Variable::Universal(Identifier("group_by_2".to_string())),
//         ]);

//         assert!(!reorder_required(
//             &variable_order,
//             &group_by_variables,
//             &idempotent_aggregate
//         ));

//         assert!(reorder_required(
//             &variable_order,
//             &group_by_variables,
//             &non_idempotent_aggregate
//         ));

//         group_by_variables.insert(Variable::Universal(Identifier("other_1".to_string())));

//         assert!(!reorder_required(
//             &variable_order,
//             &group_by_variables,
//             &non_idempotent_aggregate
//         ));

//         use nemo_physical::management::execution_plan::ExecutionNodeRef;

//         use crate::{
//             model::{chase_model::ChaseAggregate, Variable},
//             program_analysis::variable_order::VariableOrder,
//             table_manager::SubtableExecutionPlan,
//         };

//         pub(super) fn generate_node_aggregate(
//             current_plan: &mut SubtableExecutionPlan,
//             mut variable_order: VariableOrder,
//             mut node: ExecutionNodeRef,
//             aggregates: &Vec<ChaseAggregate>,
//             aggregate_group_by_variables: &HashSet<Variable>,
//         ) -> (ExecutionNodeRef, VariableOrder) {
//             assert!(
//                 aggregates.len() <= 1,
//                 "currently only one aggregate term per rule is supported"
//             );
//             for aggregate in aggregates {
//                 if reorder_required(&variable_order, aggregate_group_by_variables, aggregate) {
//                     // Perform reordering of input trie scan and materialize into temporary trie

//                     // Build variable order
//                     // Start with group-by variables, followed by the aggregate input variables
//                     // Try to preserve as much of the relative ordering as possible, to reduce the amount of swap that need to be done during the projecting/reordering
//                     let input_variable_set =
//                         HashSet::from_iter(aggregate.input_variables.iter().cloned());
//                     let mut variable_order_after_reordering =
//                         variable_order.restrict_to(aggregate_group_by_variables);
//                     for input_variable in variable_order
//                         .restrict_to(&input_variable_set)
//                         .as_ordered_list()
//                     {
//                         variable_order_after_reordering.push(input_variable);
//                     }

//                     let reordering_column_indices: Vec<_> = variable_order_after_reordering
//                         .as_ordered_list()
//                         .iter()
//                         .map(|variable| *variable_order.get(variable).unwrap())
//                         .collect();

//                     let reordering = ProjectReordering::from_vector(
//                         reordering_column_indices,
//                         variable_order.len(),
//                     );

//                     node = current_plan.plan_mut().project(node, reordering);

//                     current_plan.add_temporary_table(node.clone(), "Subtable Aggregate Reorder");

//                     // Update variable order to reordering
//                     variable_order = variable_order_after_reordering;
//                 }

//                 let aggregated_column_index = *variable_order
//                     .get(aggregate.aggregated_input_variable())
//                     .expect("variable that is aggregated has to be in the variable order");

//                 // Determine `last_distinct_column_index`
//                 let mut last_distinct_column_index = 0;
//                 for (index, variable) in variable_order.iter().enumerate() {
//                     if aggregate.input_variables.contains(variable) {
//                         last_distinct_column_index = index;
//                     }
//                 }

//                 node = current_plan.plan_mut().aggregate(
//                     node,
//                     AggregationInstructions {
//                         aggregate_operation: aggregate.aggregate_operation,
//                         group_by_column_count: aggregate_group_by_variables.len(),
//                         aggregated_column_index,
//                         last_distinct_column_index,
//                     },
//                 );

//                 // Update variable order after aggregation
//                 // Remove distinct and unused variables of the aggregate
//                 variable_order = variable_order.restrict_to(aggregate_group_by_variables);
//                 // Add aggregate output variable
//                 variable_order.push(aggregate.output_variable.clone());
//             }

//             (node, variable_order)
//         }

//         // Checks whether the variable order has to be changed in order to apply the aggregate using the [`TrieScanAggregate`]
//         fn reorder_required(
//             variable_order: &VariableOrder,
//             aggregate_group_by_variables: &HashSet<Variable>,
//             aggregate: &ChaseAggregate,
//         ) -> bool {
//             let idempotent = aggregate.aggregate_operation.idempotent::<u64>();
//             let group_by_variable_count = aggregate_group_by_variables.len();

//             // Whether a peripheral column has been seen (see [`TrieScanAggregate`])
//             let mut after_peripheral_variable = false;

//             for (index, variable) in variable_order.iter().enumerate() {
//                 if index < group_by_variable_count {
//                     // Check that the group-by variables are exactly the first variables in the variable order
//                     // See [`AggregationInstructions`] and [`TrieScanAggregate`] documentation
//                     // Otherwise the columns would need to be reordered
//                     if !aggregate_group_by_variables.contains(variable) {
//                         return true;
//                     }
//                 } else if !idempotent {
//                     // Check that distinct/aggregated variables are exactly the variables after the group-by variables
//                     // An exception are idempotent processors, because it allows the promotion of peripheral to distinct variables
//                     if aggregate.input_variables.contains(variable) {
//                         if after_peripheral_variable {
//                             return true;
//                         }
//                     } else {
//                         after_peripheral_variable = true;
//                     }
//                 }
//             }

//             false
//         }

//         #[cfg(test)]
//         mod test {
//             use std::collections::HashSet;

//             use nemo_physical::aggregates::operation::AggregateOperation;

//             use crate::{
//                 execution::planning::aggregates::reorder_required,
//                 model::{
//                     chase_model::ChaseAggregate, Identifier, LogicalAggregateOperation, Variable,
//                 },
//                 program_analysis::variable_order::VariableOrder,
//             };

//             #[test]
//             fn required_reordering() {
//                 let aggregated_input_variable =
//                     Variable::Universal(Identifier("aggregated".to_string()));

//                 let mut variable_order = VariableOrder::new();
//                 variable_order.push(Variable::Universal(Identifier("group_by_1".to_string())));
//                 variable_order.push(Variable::Universal(Identifier("group_by_2".to_string())));
//                 variable_order.push(Variable::Universal(Identifier("other_1".to_string())));
//                 variable_order.push(aggregated_input_variable.clone());
//                 variable_order.push(Variable::Universal(Identifier("other_2".to_string())));

//                 let idempotent_aggregate = ChaseAggregate {
//                     aggregate_operation: AggregateOperation::Min,
//                     input_variables: vec![aggregated_input_variable.clone()],
//                     output_variable: Variable::Universal(Identifier("output_1".to_string())),
//                     logical_aggregate_operation: LogicalAggregateOperation::MinNumber,
//                 };

//                 assert!(!reorder_required(
//                     &variable_order,
//                     &HashSet::new(),
//                     &idempotent_aggregate
//                 ),);

//                 let non_idempotent_aggregate = ChaseAggregate {
//                     aggregate_operation: AggregateOperation::Count,
//                     input_variables: vec![aggregated_input_variable.clone()],
//                     output_variable: Variable::Universal(Identifier("output_1".to_string())),
//                     logical_aggregate_operation: LogicalAggregateOperation::CountValues,
//                 };

//                 assert!(reorder_required(
//                     &variable_order,
//                     &HashSet::new(),
//                     &non_idempotent_aggregate
//                 ));

//                 let mut group_by_variables = HashSet::from_iter(vec![
//                     Variable::Universal(Identifier("group_by_1".to_string())),
//                     Variable::Universal(Identifier("group_by_2".to_string())),
//                 ]);

//                 assert!(!reorder_required(
//                     &variable_order,
//                     &group_by_variables,
//                     &idempotent_aggregate
//                 ));

//                 assert!(reorder_required(
//                     &variable_order,
//                     &group_by_variables,
//                     &non_idempotent_aggregate
//                 ));

//                 group_by_variables.insert(Variable::Universal(Identifier("other_1".to_string())));

//                 assert!(!reorder_required(
//                     &variable_order,
//                     &group_by_variables,
//                     &non_idempotent_aggregate
//                 ));

//                 assert!(reorder_required(
//                     &variable_order,
//                     &HashSet::from_iter(
//                         vec![Variable::Universal(Identifier("other_1".to_string())),].into_iter(),
//                     ),
//                     &idempotent_aggregate
//                 ));
//             }
//         }

//         assert!(reorder_required(
//             &variable_order,
//             &HashSet::from_iter(
//                 vec![Variable::Universal(Identifier("other_1".to_string())),].into_iter(),
//             ),
//             &idempotent_aggregate
//         ));
//     }
// }
