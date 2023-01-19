//! Module defining the strategies used to
//! derive the new facts for a rule application with existential variables in the head.

use crate::{
    logical::{model::Rule, program_analysis::analysis::RuleAnalysis},
    physical::dictionary::Dictionary,
};

use super::HeadStrategy;

#[derive(Debug, Copy, Clone)]
pub struct RestrictedChaseStrategy {}

impl RestrictedChaseStrategy {
    /// Create a new [`RestrictedChaseStrategy`] object.
    pub fn initialize(rule: &Rule, analysis: &RuleAnalysis) -> Self {
        todo!()
    }
}

impl<Dict: Dictionary> HeadStrategy<Dict> for RestrictedChaseStrategy {
    fn execution_tree(
        &self,
        table_manager: &crate::logical::TableManager<Dict>,
        rule_info: &crate::logical::execution::execution_engine::RuleInfo,
        variable_order: crate::logical::program_analysis::variable_order::VariableOrder,
        step_number: usize,
    ) -> Vec<
        crate::physical::management::execution_plan::ExecutionTree<
            crate::logical::table_manager::TableKey,
        >,
    > {
        todo!()
    }
}
