//! This module contains functionality for applying a rule.

use crate::{
    error::Error,
    logical::{
        model::{Identifier, chase_model::ChaseRule},
        program_analysis::{analysis::RuleAnalysis, variable_order::VariableOrder},
        table_manager::SubtableExecutionPlan,
        TableManager,
    },
};

use super::{
    execution_engine::RuleInfo,
    planning::{
        plan_body_seminaive::SeminaiveStrategy, plan_head_datalog::DatalogStrategy,
        plan_head_restricted::RestrictedChaseStrategy, BodyStrategy, HeadStrategy,
    },
};

/// Object responsible for executing a "normal" rule.
#[derive(Debug)]
pub struct RuleExecution {
    promising_variable_orders: Vec<VariableOrder>,

    body_strategy: Box<dyn BodyStrategy>,
    head_strategy: Box<dyn HeadStrategy>,
}

impl RuleExecution {
    /// Create new [`RuleExecution`].
    pub fn initialize(rule: &ChaseRule, analysis: &RuleAnalysis) -> Self {
        let body_strategy = Box::new(SeminaiveStrategy::initialize(rule, analysis));
        let head_strategy: Box<dyn HeadStrategy> = if analysis.is_existential {
            Box::new(RestrictedChaseStrategy::initialize(rule, analysis))
        } else {
            Box::new(DatalogStrategy::initialize(rule, analysis))
        };
        let promising_variable_orders = analysis.promising_variable_orders.clone();
        Self {
            promising_variable_orders,
            body_strategy,
            head_strategy,
        }
    }

    /// Execute the current rule.
    /// Returns the predicates which received new elements.
    pub fn execute(
        &self,
        table_manager: &mut TableManager,
        rule_info: &RuleInfo,
        step_number: usize,
    ) -> Result<Vec<Identifier>, Error> {
        log::info!(
            "Available orders: {}",
            self.promising_variable_orders.iter().enumerate().fold(
                "".to_string(),
                |acc, (index, promising_order)| {
                    format!("{}\n   ({}) {})", acc, index, promising_order.debug())
                }
            )
        );
        // TODO: Just because its the first doesn't mean its the best
        let best_variable_order = &self.promising_variable_orders[0];

        let mut subtable_execution_plan = SubtableExecutionPlan::default();
        let body_tree = self.body_strategy.add_plan_body(
            table_manager,
            &mut subtable_execution_plan,
            rule_info,
            best_variable_order.clone(),
            step_number,
        );

        self.head_strategy.add_plan_head(
            table_manager,
            &mut subtable_execution_plan,
            body_tree,
            rule_info,
            best_variable_order.clone(),
            step_number,
        );

        table_manager.execute_plan(subtable_execution_plan)
    }
}
