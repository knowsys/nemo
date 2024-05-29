//! Module defining the trait to be implemented by each strategy that computes
//! the new facts from a rule application.

use nemo_physical::management::execution_plan::ExecutionNodeRef;

use crate::{
    execution::{execution_engine::RuleInfo, rule_execution::VariableTranslation},
    table_manager::{SubtableExecutionPlan, TableManager},
};

use std::fmt::Debug;

/// Strategies for calculating the newly derived tables.
pub(crate) trait HeadStrategy: Debug {
    /// Calculate the concrete plan given a variable order.
    #[allow(clippy::too_many_arguments)]
    fn add_plan_head(
        &self,
        table_manager: &TableManager,
        current_plan: &mut SubtableExecutionPlan,
        variable_translation: &VariableTranslation,
        body: ExecutionNodeRef,
        aggregate: Option<ExecutionNodeRef>,
        rule_info: &RuleInfo,
        step_number: usize,
    );
}
