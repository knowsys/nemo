//! This module contains functionality for applying a rule.

use std::collections::HashSet;

use crate::{
    error::Error,
    logical::{
        model::{Identifier, Program, Rule},
        program_analysis::analysis::RuleAnalysis,
        table_manager::TableKey,
        TableManager,
    },
    physical::{dictionary::Dictionary, management::ExecutionPlan},
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
pub struct RuleExecution<'a, Dict: Dictionary> {
    analysis: &'a RuleAnalysis,

    body_strategy: Box<dyn BodyStrategy<'a, Dict> + 'a>,
    head_strategy: Box<dyn HeadStrategy<Dict> + 'a>,
}

impl<'a, Dict: Dictionary> RuleExecution<'a, Dict> {
    /// Create new [`RuleExecution`].
    pub fn initialize(rule: &'a Rule, analysis: &'a RuleAnalysis) -> Self {
        Self {
            analysis,
            body_strategy: Box::new(SeminaiveStrategy::initialize(rule, analysis)),
            head_strategy: if analysis.is_existential {
                Box::new(RestrictedChaseStrategy::initialize(rule, analysis))
            } else {
                Box::new(DatalogStrategy::initialize(rule, analysis))
            },
        }
    }

    /// Execute the current rule.
    /// Returns the predicates which received new elements.
    pub fn execute(
        &self,
        program: &Program<Dict>,
        table_manager: &mut TableManager<Dict>,
        rule_info: &RuleInfo,
        step_number: usize,
    ) -> Result<HashSet<Identifier>, Error> {
        log::info!(
            "Available orders: {}",
            self.analysis
                .promising_variable_orders
                .iter()
                .enumerate()
                .fold("".to_string(), |acc, (index, promising_order)| {
                    format!(
                        "{}\n   ({}) {})",
                        acc,
                        index,
                        promising_order.debug(program.get_names())
                    )
                })
        );
        // TODO: Just because its the first doesn't mean its the best
        let best_variable_order = &self.analysis.promising_variable_orders[0];

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

        table_manager.execute_plan_optimized(execution_plan)
    }
}
