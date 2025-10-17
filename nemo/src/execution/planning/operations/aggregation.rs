//! This module defines [GeneratorAggregation].

use std::collections::HashSet;

use nemo_physical::{
    management::execution_plan::ExecutionNodeRef,
    tabular::operations::aggregate::AggregateAssignment,
};

use crate::{
    execution::planning::{
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
    /// Input variables
    aggregation_input_variables: Vec<Variable>,

    /// Aggregation
    aggregation: Aggregation,

    /// Output variables
    aggregation_output_variables: Vec<Variable>,

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
        let (aggregation_input_variables, aggregation_output_variables) = Self::order_input_output(
            &input_variables,
            aggregation.input_variable(),
            aggregation.output_variable(),
            aggregation.distinct_variables(),
            aggregation.group_by_variables(),
        );

        let function = GeneratorFunction::new(aggregation_output_variables.clone(), operations);

        Self {
            aggregation_input_variables,
            aggregation,
            function: function.or_none(),
            aggregation_output_variables,
        }
    }

    /// Compute the variable labels of the input table to the aggregation
    /// and the labels after computing the aggregate.
    fn order_input_output(
        input: &[Variable],
        aggregate_input: &Variable,
        aggregate_output: &Variable,
        distinct: &[Variable],
        group_by: &[Variable],
    ) -> (Vec<Variable>, Vec<Variable>) {
        let mut ordered_input = Vec::<Variable>::default();
        let mut ordered_output = Vec::<Variable>::default();

        for variable in input {
            if group_by.contains(variable) {
                ordered_input.push(variable.clone());
                ordered_output.push(variable.clone());
            }
        }

        ordered_input.push(aggregate_input.clone());
        ordered_output.push(aggregate_output.clone());

        for variable in input {
            if distinct.contains(variable) {
                ordered_input.push(variable.clone());
            }
        }

        let mut seen = HashSet::<Variable>::new();
        ordered_input.retain(|variable| seen.insert(variable.clone()));

        seen.clear();
        ordered_output.retain(|variable| seen.insert(variable.clone()));

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

        let distinct_columns: Vec<_> = runtime
            .translation
            .operation_table(self.aggregation.distinct_variables().iter())
            .to_vec();
        let group_by_columns = runtime
            .translation
            .operation_table(self.aggregation.group_by_variables().iter())
            .to_vec();

        let ordered_input_markers = runtime
            .translation
            .operation_table(self.aggregation_input_variables.iter());

        let aggregate_input_node = plan
            .plan_mut()
            .projectreorder(ordered_input_markers, input_node);

        let aggregate_assignment = AggregateAssignment {
            aggregate_operation: self.aggregation.aggregate_kind().into(),
            distinct_columns,
            group_by_columns,
            aggregated_column: aggregation_input_column,
        };

        let output_markers = runtime
            .translation
            .operation_table(self.aggregation_output_variables.iter());

        let mut node_result =
            plan.plan_mut()
                .aggregate(output_markers, aggregate_input_node, aggregate_assignment);

        if let Some(generator) = self.function.as_ref() {
            node_result = generator.create_plan(plan, node_result, runtime);
        }

        node_result
    }
}
