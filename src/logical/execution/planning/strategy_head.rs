//! Module defining the trait to be implemented by each strategy that computes
//! the new facts from a rule application.

use crate::{
    logical::{
        execution::execution_engine::RuleInfo, program_analysis::variable_order::VariableOrder,
        table_manager::SubtableExecutionPlan, TableManager,
    },
    physical::management::execution_plan::ExecutionNodeRef,
};
use std::fmt::Debug;

/// Strategies for calculating the newly derived tables.
pub trait HeadStrategy: Debug {
    /// Calculate the concrete plan given a variable order.
    fn add_plan_head(
        &self,
        table_manager: &TableManager,
        current_plan: &mut SubtableExecutionPlan,
        body: ExecutionNodeRef,
        rule_info: &RuleInfo,
        variable_order: VariableOrder,
        step_number: usize,
    );
}
