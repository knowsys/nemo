//! Module defining the trait to be implemented by each strategy that computes
//! all the matches for a rule application.

use crate::{
    logical::{
        execution::execution_engine::RuleInfo, program_analysis::variable_order::VariableOrder,
        table_manager::TableKey, TableManager,
    },
    physical::{dictionary::Dictionary, management::execution_plan::ExecutionTree},
};
use std::fmt::Debug;

/// Strategies for calculating all matches for a rule application.
pub trait BodyStrategy<'a, Dict: Dictionary>: Debug {
    /// Calculate the concrete plan given a variable order.
    fn execution_tree(
        &self,
        table_manager: &TableManager<Dict>,
        rule_info: &RuleInfo,
        variable_order: VariableOrder,
        step_number: usize,
    ) -> ExecutionTree<TableKey>;
}
