//! This module contains functionality for applying a rule.

use std::error::Error;

use nemo_physical::tabular::operations::OperationTableGenerator;

use crate::{
    model::{chase_model::ChaseRule, Identifier, Variable},
    program_analysis::{analysis::RuleAnalysis, variable_order::VariableOrder},
    table_manager::{SubtableExecutionPlan, TableManager},
};

use super::{
    execution_engine::RuleInfo,
    planning::{
        plan_body_seminaive::SeminaiveStrategy, plan_head_datalog::DatalogStrategy,
        plan_head_restricted::RestrictedChaseStrategy, BodyStrategy, HeadStrategy,
    },
};

/// Translation of a [Variable] into an [OperationTable][nemo_physical::tabular::operations::OperationColumnMarker],
/// which is used for constructing [ExecutionPlan][nemo_physical::management::execution_plan::ExecutionPlan]s
pub(crate) type VariableTranslation = OperationTableGenerator<Variable>;

/// Object responsible for executing a "normal" rule.
#[derive(Debug)]
pub(crate) struct RuleExecution {
    /// Translation of variables into markers used for creating execution plans
    variable_translation: VariableTranslation,
    /// List of variable orders which might be considered for this rule
    promising_variable_orders: Vec<VariableOrder>,

    /// Object for generating an execution plan,
    /// which evaluates the body expression of the rule
    body_strategy: Box<dyn BodyStrategy>,
    /// Object for generating an execution plan,
    /// which evaluates the head expression of the rule
    head_strategy: Box<dyn HeadStrategy>,
}

impl RuleExecution {
    /// Create new [`RuleExecution`].
    pub(crate) fn initialize(rule: &ChaseRule, analysis: &RuleAnalysis) -> Self {
        let mut variable_translation = VariableTranslation::new();
        for variable in rule.all_variables() {
            variable_translation.add_marker(variable);
        }

        let body_strategy = Box::new(SeminaiveStrategy::initialize(rule, analysis));
        let head_strategy: Box<dyn HeadStrategy> = if analysis.is_existential {
            Box::new(RestrictedChaseStrategy::initialize(rule, analysis))
        } else {
            Box::new(DatalogStrategy::initialize(rule, analysis))
        };
        let promising_variable_orders = analysis.promising_variable_orders.clone();
        Self {
            promising_variable_orders,
            variable_translation,
            body_strategy,
            head_strategy,
        }
    }

    /// Execute the current rule.
    /// Returns the predicates which received new elements.
    pub(crate) fn execute(
        &self,
        table_manager: &mut TableManager,
        rule_info: &RuleInfo,
        step_number: usize,
    ) -> Result<Vec<Identifier>, Box<dyn Error>> {
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
        let mut best_variable_order = self.promising_variable_orders[0].clone();

        let mut subtable_execution_plan = SubtableExecutionPlan::default();
        let body_tree = self.body_strategy.add_plan_body(
            table_manager,
            &mut subtable_execution_plan,
            &self.variable_translation,
            rule_info,
            &mut best_variable_order, // Variable order possibly gets updated
            step_number,
        );

        self.head_strategy.add_plan_head(
            table_manager,
            &mut subtable_execution_plan,
            &self.variable_translation,
            body_tree,
            rule_info,
            step_number,
        );

        table_manager.execute_plan(subtable_execution_plan)
    }
}
