//! This module defines the strategy for the (forward) execution of rules.

use crate::{
    chase_model::analysis::variable_order::VariableOrder,
    execution::{
        planning_new::{
            RuntimeInformation, normalization::rule::NormalizedRule,
            operations::aggregation::GeneratorAggregation, strategy::forward::body::StrategyBody,
        },
        rule_execution::VariableTranslation,
    },
    io::ImportManager,
    table_manager::{SubtableExecutionPlan, TableManager},
};

pub mod body;
pub mod head;
pub mod restricted;

/// Strategy for creating an execution for the
/// (forward) evaluation of a rule.
#[derive(Debug)]
pub struct StrategyForward {
    /// Generator for the body operations
    body: StrategyBody,
    /// Generator for the aggregation
    aggregation: Option<GeneratorAggregation>,
    // head: StrategyHead
    /// Variable order
    order: VariableOrder,
    /// Variable translation
    translation: VariableTranslation,
}

impl StrategyForward {
    /// Create a new [StrategyForward].
    pub fn new(rule: &NormalizedRule) -> Self {
        let positive = rule.positive().clone();
        let negative = rule.negative().clone();
        let operations = rule.operations().clone();
        let imports = rule.imports().clone();

        let body = StrategyBody::new(
            rule.variable_order().clone(),
            positive,
            negative,
            imports,
            operations,
        );

        let aggregation = rule
            .aggregate()
            .cloned()
            .map(|aggregation| GeneratorAggregation::new(aggregation));

        let mut translation = VariableTranslation::new();
        for variable in rule.variables() {
            translation.add_marker(variable.clone());
        }

        Self {
            body,
            aggregation,
            order: rule.variable_order().clone(),
            translation,
        }
    }

    /// Create an execution plan for evaluating a rule.
    pub async fn create_plan<'a>(
        &self,
        table_manager: &'a TableManager,
        import_manager: &'a ImportManager,
        step_current: usize,
        step_last_application: usize,
    ) -> SubtableExecutionPlan {
        let mut plan = SubtableExecutionPlan::default();

        let runtime = RuntimeInformation {
            step_last_application,
            step_current,
            table_manager,
            import_manager,
            order: self.order.clone(),
            translation: self.translation.clone(),
        };

        let node_body = self.body.create_plan(&mut plan, &runtime).await;

        let node_aggregation = self
            .aggregation
            .as_ref()
            .map(|generator| generator.create_plan(&mut plan, node_body.clone(), &runtime));

        plan
    }
}
