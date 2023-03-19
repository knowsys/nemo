//! Module defining the trait to be implemented by each strategy that computes
//! the new facts from a rule application.

use crate::{
    logical::{
        execution::execution_engine::RuleInfo, program_analysis::variable_order::VariableOrder,
        table_manager::SubtableExecutionPlan, TableManager,
    },
    physical::dictionary::Dictionary,
};
use std::fmt::Debug;

/// Strategies for calculating the newly derived tables.
pub trait HeadStrategy<Dict: Dictionary>: Debug {
    /// Calculate the concrete plan given a variable order.
    fn add_head_trees(
        &self,
        table_manager: &TableManager<Dict>,
        current_plan: &mut SubtableExecutionPlan,
        body_id: usize,
        rule_info: &RuleInfo,
        variable_order: VariableOrder,
        step_number: usize,
    );
}
