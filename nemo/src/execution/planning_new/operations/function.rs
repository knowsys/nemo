//! This module defines [GeneratorFunction].

use nemo_physical::{
    management::execution_plan::ExecutionNodeRef, tabular::operations::FunctionAssignment,
};

use crate::{
    execution::planning_new::{
        normalization::operation::Operation, operations::RuntimeInformation,
    },
    rule_model::components::term::primitive::variable::Variable,
    table_manager::SubtableExecutionPlan,
};

/// Generator of function nodes in execution plans
#[derive(Debug)]
pub struct GeneratorFunction {
    /// Output variables
    variables: Vec<Variable>,

    /// Applied functions and their output variable
    functions: Vec<(Variable, Operation)>,
}

impl GeneratorFunction {
    /// Create a new [GeneratorFunction].
    ///
    /// Uses exactly those `operation` that bind a new variable and
    /// whose result can be determined
    /// by bound values of the input or other function values
    /// and removes them from the `operations` list.
    pub fn new(input_variables: Vec<Variable>, operations: &mut Vec<Operation>) -> Self {
        let mut variables = input_variables;
        let mut keep = vec![true; operations.len()];
        let mut functions = Vec::<(Variable, Operation)>::default();

        let mut variables_len = variables.len();

        loop {
            for (index, function) in operations.iter().enumerate() {
                if let Some((variable, operation)) = function.variable_assignment() {
                    if operation
                        .variables()
                        .all(|variable| variables.contains(variable))
                    {
                        keep[index] = false;
                        variables.push(variable.clone());
                        functions.push((variable.clone(), operation.clone()));
                    }
                }
            }

            if variables.len() == variables_len {
                break;
            }

            variables_len = variables.len();
        }

        let mut keep_iter = keep.iter();
        operations.retain(|_| *keep_iter.next().expect("keep is as long as operations"));

        Self {
            variables,
            functions,
        }
    }

    /// Append this operation to the plan.
    pub fn create_plan(
        &self,
        plan: &mut SubtableExecutionPlan,
        input_node: ExecutionNodeRef,
        runtime: &RuntimeInformation,
    ) -> ExecutionNodeRef {
        let markers_output = runtime.translation.operation_table(self.variables.iter());
        let mut assignments = FunctionAssignment::new();

        for (variable, operation) in &self.functions {
            let marker_variable = *runtime
                .translation
                .get(variable)
                .expect("all variables are known");
            let function_tree = operation.function_tree(&runtime.translation);
            assignments.insert(marker_variable, function_tree);
        }

        plan.plan_mut()
            .function(markers_output, input_node.clone(), assignments)
    }

    /// Return the variables marking the column of the node
    /// created by `create_plan`.
    pub fn output_variables(&self) -> Vec<Variable> {
        self.variables.clone()
    }
}
