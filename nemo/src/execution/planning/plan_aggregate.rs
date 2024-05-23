//! Module defining the strategy for calculating all body matches for a rule application.

use nemo_physical::management::execution_plan::ExecutionNodeRef;

use crate::{
    execution::rule_execution::VariableTranslation,
    model::{
        chase_model::{ChaseAggregate, ChaseRule, Constructor},
        Constraint,
    },
    program_analysis::analysis::RuleAnalysis,
    table_manager::SubtableExecutionPlan,
};

use super::operations::{
    aggregate::node_aggregate, filter::node_filter, functions::node_functions,
};

/// Implementation of the semi-naive existential rule evaluation strategy.
#[derive(Debug)]
pub(crate) struct AggregateStategy {
    aggregate: ChaseAggregate,
    aggregate_constructors: Vec<Constructor>,
    aggregate_constraints: Vec<Constraint>,
}

impl AggregateStategy {
    /// Create new [SeminaiveStrategy] object.
    pub(crate) fn initialize(rule: &ChaseRule, _analysis: &RuleAnalysis) -> Self {
        Self {
            aggregate: rule
                .aggregate()
                .clone()
                .expect("do not call this if there is no aggregate"),
            aggregate_constructors: rule.aggregate_constructors().clone(),
            aggregate_constraints: rule.aggregate_constraints().clone(),
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
            &self.aggregate_constructors,
        );

        let node_result = node_filter(
            current_plan.plan_mut(),
            variable_translation,
            node_aggregate_functions,
            &self.aggregate_constraints,
        );

        current_plan.add_temporary_table(node_result.clone(), "Aggregation");
        node_result
    }
}
