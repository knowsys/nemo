//! Module defining the trait to be implemented by each strategy that computes
//! all the matches for a rule application.

use crate::{
    logical::{
        execution::execution_engine::RuleInfo, program_analysis::variable_order::VariableOrder,
        table_manager::SubtableExecutionPlan, TableManager,
    },
    physical::dictionary::Dictionary,
};
use std::fmt::Debug;

/// Strategies for calculating all matches for a rule application.
pub trait BodyStrategy<Dict: Dictionary>: Debug {
    /// Calculate the concrete plan given a variable order.
    fn add_body_tree(
        &self,
        table_manager: &TableManager<Dict>,
        current_pan: &mut SubtableExecutionPlan,
        rule_info: &RuleInfo,
        variable_order: VariableOrder,
        step_number: usize,
    ) -> usize;
}
