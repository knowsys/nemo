//! This module contains functionailty for applying a rule.

use crate::{
    logical::{
        model::{Program, Rule},
        program_analysis::analysis::{CopyRuleAnalysis, NormalRuleAnalysis, RuleAnalysis},
        table_manager::TableKey,
        TableManager,
    },
    meta::logging::{log_avaiable_variable_order, log_choose_variable_order},
    physical::management::ExecutionPlan,
};

use super::{
    execution_engine::RuleInfo,
    planning::{
        plan_normal_body::{BodyStrategy, SeminaiveStrategy},
        plan_normal_head::{DatalogStrategy, HeadStrategy},
    },
};

/// Object responsible for executing a "normal" rule.
#[derive(Debug)]
pub struct NormalRuleExecution<'a> {
    analysis: &'a NormalRuleAnalysis,

    body_strategy: SeminaiveStrategy<'a>,
    head_strategy: DatalogStrategy<'a>,
}

impl<'a> NormalRuleExecution<'a> {
    /// Create new [`NormalRuleExecution`].
    pub fn initialize(rule: &'a Rule, analysis: &'a NormalRuleAnalysis) -> Self {
        Self {
            analysis,
            body_strategy: SeminaiveStrategy::initialize(rule, analysis),
            head_strategy: DatalogStrategy::initialize(rule, analysis),
        }
    }

    /// Execute the current rule.
    /// Returns whether a new (non-empty) table has been created.
    pub fn execute(
        &self,
        table_manager: &mut TableManager,
        rule_info: &RuleInfo,
        step_number: usize,
    ) -> bool {
        // TODO: Just because its the first doesn't mean its the best
        let best_variable_order = &self.analysis.promising_variable_orders[0];
        log_choose_variable_order(0);

        let mut execution_plan = ExecutionPlan::<TableKey>::new();
        let tree_body = self.body_strategy.execution_tree(
            table_manager,
            rule_info,
            best_variable_order.clone(),
            step_number,
        );
        let trees_head = self.head_strategy.execution_tree(
            table_manager,
            rule_info,
            best_variable_order.clone(),
            step_number,
        );

        execution_plan.push(tree_body);
        execution_plan.append(trees_head);

        table_manager.execute_plan(execution_plan)
    }
}

/// Object responsible for executing a "copy" rule.
#[derive(Debug)]
pub struct CopyRuleExecution<'a> {
    rule: &'a Rule,
    analysis: &'a CopyRuleAnalysis,
}

impl<'a> CopyRuleExecution<'a> {
    /// Create new [`NormalRuleExecution`].
    pub fn initialize(rule: &'a Rule, analysis: &'a CopyRuleAnalysis) -> Self {
        Self { rule, analysis }
    }

    /// Execute the current rule.
    /// Returns whether a new (non-empty) table has been created.
    pub fn execute(
        &self,
        table_manager: &mut TableManager,
        rule_info: &RuleInfo,
        step_number: usize,
    ) -> bool {
        for (head_index, (body_index, reordering)) in &self.analysis.head_to_body {
            let head_predicate = self.rule.head()[*head_index].predicate();
            let body_predicate = self.rule.body()[*body_index].predicate();

            let old_table_range = rule_info.step_last_applied..step_number;
            let new_table_range = step_number..(step_number + 1);

            let new_key_opt = table_manager.add_union_table(
                body_predicate,
                old_table_range.clone(),
                body_predicate,
                old_table_range,
                None,
            );

            if let Some(new_key) = new_key_opt {
                table_manager.add_reference(
                    head_predicate,
                    new_table_range,
                    new_key.name,
                    reordering.clone(),
                );
            }
        }

        false
    }
}

/// Object responsible for executing a rule.
#[derive(Debug)]
pub enum RuleExecution<'a> {
    /// Behavior for "normal" rules.
    NormalRule(NormalRuleExecution<'a>),
    /// Behavior for "copy" rules.
    CopyRule(CopyRuleExecution<'a>),
}

impl<'a> RuleExecution<'a> {
    /// Create new [`RuleExecution`].
    pub fn initialize(rule: &'a Rule, analysis: &'a RuleAnalysis) -> Self {
        match analysis {
            RuleAnalysis::Copy(copy_analysis) => {
                Self::CopyRule(CopyRuleExecution::initialize(rule, copy_analysis))
            }
            RuleAnalysis::Normal(normal_analysis) => {
                Self::NormalRule(NormalRuleExecution::initialize(rule, normal_analysis))
            }
        }
    }

    /// Execute the current rule.
    /// Returns whether a new (non-empty) table has been created.
    pub fn execute(
        &self,
        program: &Program,
        table_manager: &mut TableManager,
        rule_info: &RuleInfo,
        step_number: usize,
    ) -> bool {
        match self {
            RuleExecution::NormalRule(execution) => {
                log_avaiable_variable_order(program, execution.analysis);

                execution.execute(table_manager, rule_info, step_number)
            }
            RuleExecution::CopyRule(execution) => {
                execution.execute(table_manager, rule_info, step_number)
            }
        }
    }
}
