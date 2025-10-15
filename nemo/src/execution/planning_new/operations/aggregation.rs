//! This module defines [GeneratorAggregation].

use nemo_physical::{
    management::execution_plan::ExecutionNodeRef,
    tabular::operations::{OperationColumnMarker, OperationTable, aggregate::AggregateAssignment},
};

use crate::{
    execution::planning_new::{
        RuntimeInformation,
        normalization::{aggregate::Aggregation, operation::Operation},
        operations::function::GeneratorFunction,
    },
    rule_model::components::term::primitive::variable::Variable,
    table_manager::SubtableExecutionPlan,
};

/// Generator of aggregation nodes in execution plans
#[derive(Debug)]
pub struct GeneratorAggregation {
    /// Aggregation
    aggregation: Aggregation,

    /// Functions
    function: Option<GeneratorFunction>,
}

impl GeneratorAggregation {
    /// Create a new [GeneratorAggregation].
    pub fn new(
        input_variables: Vec<Variable>,
        aggregation: Aggregation,
        operations: &mut Vec<Operation>,
    ) -> Self {
        let mut variables = input_variables;
        variables.push(aggregation.output_variable().clone());

        let function = GeneratorFunction::new(variables, operations);

        Self {
            aggregation,
            function: function.or_none(),
        }
    }

    /// Compute the [OperationTable] necessary for computing
    /// an aggregate from an input table.
    ///
    /// This function receives the column markers of the input table,
    /// the name of the column that contains the inputs to the aggregate (`aggregate_input_column`),
    /// the name of the column that contains the result of the aggregate (`aggregate_output_column`),
    /// as well as distinct as group by columns.
    ///
    /// For the aggreagtion, we need to reorder the input columns such that
    /// group by columns come first, then the aggregated column,
    /// and finally the distinct columns.
    ///
    /// The return value is a pair of [OperationTable],
    /// where the first entry is the required ordering of the input table
    /// needed to perform an aggregation
    /// and the second entry is the ordering of the table after applying the aggregation.
    fn order_input_output(
        input: &OperationTable,
        aggregate_input_column: &OperationColumnMarker,
        aggregate_output_column: &OperationColumnMarker,
        distinct_columns: &[OperationColumnMarker],
        group_by_columns: &[OperationColumnMarker],
    ) -> (OperationTable, OperationTable) {
        let mut ordered_input = OperationTable::default();
        let mut ordered_output = OperationTable::default();

        for column in input.iter() {
            if group_by_columns.contains(column) {
                ordered_input.push_distinct(*column);
                ordered_output.push_distinct(*column);
            }
        }

        ordered_input.push_distinct(*aggregate_input_column);
        ordered_output.push_distinct(*aggregate_output_column);

        for column in input.iter() {
            if distinct_columns.contains(column) {
                ordered_input.push_distinct(*column);
            }
        }

        (ordered_input, ordered_output)
    }

    /// Append this operation to the plan.
    pub fn create_plan(
        &self,
        plan: &mut SubtableExecutionPlan,
        input_node: ExecutionNodeRef,
        runtime: &RuntimeInformation,
    ) -> ExecutionNodeRef {
        let aggregation_input_column = *runtime
            .translation
            .get(self.aggregation.input_variable())
            .expect("aggregated variable has to be known");

        let aggregation_output_column = *runtime
            .translation
            .get(self.aggregation.output_variable())
            .expect("aggregate output has to be known");

        let distinct_columns: Vec<_> = runtime
            .translation
            .operation_table(self.aggregation.distinct_variables().iter())
            .to_vec();
        let group_by_columns = runtime
            .translation
            .operation_table(self.aggregation.group_by_variables().iter())
            .to_vec();

        let unordered_input_markers = input_node.markers_cloned();
        let (ordered_input_markers, output_markers) = Self::order_input_output(
            &unordered_input_markers,
            &aggregation_input_column,
            &aggregation_output_column,
            &distinct_columns,
            &group_by_columns,
        );

        let aggregate_input_node = plan
            .plan_mut()
            .projectreorder(ordered_input_markers, input_node);

        let aggregate_assignment = AggregateAssignment {
            aggregate_operation: self.aggregation.aggregate_kind().into(),
            distinct_columns,
            group_by_columns,
            aggregated_column: aggregation_input_column,
        };

        let mut node_result =
            plan.plan_mut()
                .aggregate(output_markers, aggregate_input_node, aggregate_assignment);

        if let Some(generator) = self.function.as_ref() {
            node_result = generator.create_plan(plan, node_result, runtime);
        }

        node_result
    }
}
