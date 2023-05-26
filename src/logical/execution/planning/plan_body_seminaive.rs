//! Module defining the strategy for calculating all body matches for a rule application.

use std::collections::HashSet;

use crate::{
    logical::{
        execution::execution_engine::RuleInfo,
        model::{chase_model::ChaseRule, Variable},
        program_analysis::{analysis::RuleAnalysis, variable_order::VariableOrder},
        table_manager::SubtableExecutionPlan,
        TableManager,
    },
    physical::management::execution_plan::ExecutionNodeRef,
};

use super::{
    negation::NegationGenerator, plan_util::cut_last_layers, BodyStrategy, SeminaiveJoinGenerator,
};

/// Implementation of the semi-naive existential rule evaluation strategy.
#[derive(Debug)]
pub struct SeminaiveStrategy {
    used_variables: HashSet<Variable>,
    join_generator: SeminaiveJoinGenerator,
    negation_generator: Option<NegationGenerator>,
}

impl SeminaiveStrategy {
    /// Create new [`SeminaiveStrategy`] object.
    pub fn initialize(rule: &ChaseRule, analysis: &RuleAnalysis) -> Self {
        let used_variables = analysis.head_variables.clone();

        let join_generator = SeminaiveJoinGenerator {
            atoms: rule.positive_body().clone(),
            filters: rule.positive_filters().clone(),
            variable_types: analysis.variable_types.clone(),
        };

        let negation_generator = if !rule.negative_body().is_empty() {
            Some(NegationGenerator {
                variable_types: analysis.variable_types.clone(),
                atoms: rule.negative_body().clone(),
                filters: rule.negative_filters().clone(),
            })
        } else {
            None
        };

        Self {
            used_variables,
            join_generator,
            negation_generator,
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
        let mut node_seminaive = self.join_generator.seminaive_join(
            current_plan.plan_mut(),
            table_manager,
            rule_info.step_last_applied,
            step_number,
            &variable_order,
        );

        if let Some(generator) = &self.negation_generator {
            node_seminaive = generator.generate_plan(
                current_plan,
                table_manager,
                node_seminaive,
                &variable_order,
                step_number,
            )
        }

        let cut = cut_last_layers(&variable_order, &self.used_variables);
        current_plan.add_temporary_table_cut(node_seminaive.clone(), "Body Join", cut);

        node_seminaive
    }
}
