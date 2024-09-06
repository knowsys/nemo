//! Module defining the strategy for calculating all body matches for a rule application.

use nemo_physical::management::execution_plan::ExecutionNodeRef;

use crate::{
    chase_model::{
        analysis::program_analysis::RuleAnalysis,
        components::{
            aggregate::ChaseAggregate, filter::ChaseFilter, operation::ChaseOperation,
            rule::ChaseRule,
        },
    },
    execution::rule_execution::VariableTranslation,
    table_manager::SubtableExecutionPlan,
};

use super::operations::{
    aggregate::node_aggregate, filter::node_filter, functions::node_functions,
};

/// Implementation of the semi-naive existential rule evaluation strategy.
#[derive(Debug)]
pub(crate) struct AggregateStategy {
    aggregate: ChaseAggregate,
    aggregate_operation: Vec<ChaseOperation>,
    aggregate_filters: Vec<ChaseFilter>,
}

impl AggregateStategy {
    /// Create new [SeminaiveStrategy] object.
    pub(crate) fn initialize(rule: &ChaseRule, _analysis: &RuleAnalysis) -> Self {
        Self {
            aggregate: rule
                .aggregate()
                .cloned()
                .expect("do not call this if there is no aggregate"),
            aggregate_operation: rule.aggregate_operations().clone(),
            aggregate_filters: rule.aggregate_filters().clone(),
        }
    }

    pub(crate) fn add_plan_aggregate(
        &self,
        current_plan: &mut SubtableExecutionPlan,
        variable_translation: &VariableTranslation,
        subnode: ExecutionNodeRef,
    ) -> ExecutionNodeRef {
        let node_aggregation = node_aggregate(
            current_plan.plan_mut(),
            variable_translation,
            subnode,
            &self.aggregate,
        );

        let node_aggregate_functions = node_functions(
            current_plan.plan_mut(),
            variable_translation,
            node_aggregation,
            &self.aggregate_operation,
        );

        let node_result = node_filter(
            current_plan.plan_mut(),
            variable_translation,
            node_aggregate_functions,
            &self.aggregate_filters,
        );

        current_plan.add_temporary_table(node_result.clone(), "Aggregation");
        node_result
    }
}
