//! Module defining the strategy for calculating all body matches for a rule application.

use std::collections::HashSet;

use crate::{
    logical::{
        execution::execution_engine::RuleInfo,
        model::{Rule, Variable},
        program_analysis::{analysis::RuleAnalysis, variable_order::VariableOrder},
        table_manager::SubtableExecutionPlan,
        TableManager,
    },
    physical::management::execution_plan::ExecutionNodeRef,
};

use super::{plan_util::cut_last_layers, BodyStrategy, SeminaiveJoinGenerator};

/// Implementation of the semi-naive existential rule evaluation strategy.
#[derive(Debug)]
pub struct SeminaiveStrategy {
    used_variables: HashSet<Variable>,
    join_generator: SeminaiveJoinGenerator,
}

impl SeminaiveStrategy {
    /// Create new [`SeminaiveStrategy`] object.
    pub fn initialize(rule: &Rule, analysis: &RuleAnalysis) -> Self {
        let used_variables = analysis.head_variables.clone();

        let join_generator = SeminaiveJoinGenerator {
            atoms: rule.positive_body().clone(),
            filters: rule.positive_filters().clone(),
            variables: analysis.positive_body_variables.clone(),
            variable_types: analysis.variable_types.clone(),
        };

        Self {
            used_variables,
            join_generator,
        }
    }
}

impl BodyStrategy for SeminaiveStrategy {
    fn add_plan_body(
        &self,
        table_manager: &TableManager,
        current_plan: &mut SubtableExecutionPlan,
        rule_info: &RuleInfo,
        variable_order: VariableOrder,
        step_number: usize,
    ) -> ExecutionNodeRef {
        let node_seminaive = self.join_generator.seminaive_join(
            current_plan.plan_mut(),
            table_manager,
            rule_info.step_last_applied,
            step_number,
            &variable_order,
        );

        let cut = cut_last_layers(&variable_order, &self.used_variables);
        current_plan.add_temporary_table_cut(node_seminaive.clone(), "Body Join", cut);

        node_seminaive
    }
}
