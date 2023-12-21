//! Module defining the trait to be implemented by each strategy that computes
//! all the matches for a rule application.

use nemo_physical::management::execution_plan::ExecutionNodeRef;

use crate::{
    execution::execution_engine::RuleInfo,
    program_analysis::variable_order::VariableOrder,
    table_manager::{SubtableExecutionPlan, TableManager},
};

use std::fmt::Debug;

/// Strategies for calculating all matches for a rule application.
pub(crate) trait BodyStrategy: Debug {
    /// Calculate the concrete plan given a variable order.
    /// Returns the root node of the tree that represents the calculation for the body.
    /// Updates the variable order according to changes by e.g. aggregates and arithmetic operations.
    fn add_plan_body(
        &self,
        table_manager: &TableManager,
        current_plan: &mut SubtableExecutionPlan,
        rule_info: &RuleInfo,
        variable_order: &mut VariableOrder,
        step_number: usize,
    ) -> ExecutionNodeRef;
}
