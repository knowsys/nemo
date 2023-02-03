//! This module contains functionality for applying a rule.

use std::{collections::HashSet, marker::PhantomData};

use crate::{
    error::Error,
    logical::{
        model::{Identifier, Program, Rule},
        program_analysis::analysis::RuleAnalysis,
        table_manager::TableKey,
        types::LogicalTypeCollection,
        TableManager,
    },
    meta::logging::{log_available_variable_order, log_choose_variable_order},
    physical::{dictionary::Dictionary, management::ExecutionPlan},
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
pub struct RuleExecution<'a, Dict: Dictionary, LogicalTypes: LogicalTypeCollection> {
    analysis: &'a RuleAnalysis,

    body_strategy: SeminaiveStrategy<'a, LogicalTypes>,
    head_strategy: DatalogStrategy,
    _dict: PhantomData<Dict>,
}

impl<'a, Dict: Dictionary, LogicalTypes: LogicalTypeCollection>
    RuleExecution<'a, Dict, LogicalTypes>
{
    /// Create new [`RuleExecution`].
    pub fn initialize(rule: &'a Rule<LogicalTypes>, analysis: &'a RuleAnalysis) -> Self {
        Self {
            analysis,
            body_strategy:
                <SeminaiveStrategy<LogicalTypes> as BodyStrategy<Dict, LogicalTypes>>::initialize(
                    rule, analysis,
                ),
            head_strategy: <DatalogStrategy as HeadStrategy<Dict, LogicalTypes>>::initialize(
                rule, analysis,
            ),
            _dict: PhantomData {},
        }
    }

    /// Execute the current rule.
    /// Returns the predicates which received new elements.
    pub fn execute(
        &self,
        program: &Program<Dict, LogicalTypes>,
        table_manager: &mut TableManager<Dict, LogicalTypes>,
        rule_info: &RuleInfo,
        step_number: usize,
    ) -> Result<HashSet<Identifier>, Error> {
        log_available_variable_order(program, self.analysis);
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

        table_manager.execute_plan_optimized(execution_plan)
    }
}
