//! This module defines [GeneratorJoinImports].

use std::collections::HashMap;

use nemo_physical::management::execution_plan::ExecutionNodeRef;

use crate::{
    execution::planning::{
        RuntimeInformation,
        operations::{
            join_imports_general::GeneratorJoinImportsGeneral,
            join_imports_simple::GeneratorJoinImportsSimple,
        },
    },
    rule_model::components::{tag::Tag, term::primitive::variable::Variable},
    table_manager::SubtableExecutionPlan,
};

/// Generator for execution plan nodes
/// that join incrementally imported tables.
#[derive(Debug)]
pub enum GeneratorJoinImports {
    /// Simple case with one factor
    Simple(GeneratorJoinImportsSimple),
    /// General case with many cartesian factors
    General(GeneratorJoinImportsGeneral),
}

impl GeneratorJoinImports {
    /// Append this operation to the plan.
    pub fn create_plan(
        &self,
        plan: &mut SubtableExecutionPlan,
        node_positive: Option<ExecutionNodeRef>,
        new_imports: HashMap<Tag, ExecutionNodeRef>,
        runtime: &RuntimeInformation,
    ) -> ExecutionNodeRef {
        match self {
            GeneratorJoinImports::Simple(generator) => {
                generator.create_plan(plan, node_positive.expect(""), new_imports, runtime)
            }
            GeneratorJoinImports::General(generator) => {
                generator.create_plan(plan, new_imports, runtime)
            }
        }
    }

    /// Return the variables marking the column of the node
    /// created by `create_plan`.
    pub fn output_variables(&self) -> Vec<Variable> {
        let output = match self {
            GeneratorJoinImports::Simple(generator) => generator.output_variables(),
            GeneratorJoinImports::General(generator) => generator.output_variables(),
        };

        output
    }
}
