//! Module defining the strategy for calculating all body matches for a rule application.

use nemo_physical::management::execution_plan::ExecutionNodeRef;

use crate::{
    chase_model::{
        analysis::{program_analysis::RuleAnalysis, variable_order::VariableOrder},
        components::{
            atom::variable_atom::VariableAtom, filter::ChaseFilter, operation::ChaseOperation,
            rule::ChaseRule,
        },
    },
    execution::{execution_engine::RuleInfo, rule_execution::VariableTranslation},
    table_manager::{SubtableExecutionPlan, TableManager},
};

use super::{
    operations::{
        filter::node_filter, functions::node_functions, join::node_join, negation::node_negation,
    },
    BodyStrategy,
};

/// Implementation of the semi-naive existential rule evaluation strategy.
#[derive(Debug)]
pub(crate) struct SeminaiveStrategy {
    positive_atoms: Vec<VariableAtom>,
    positive_filters: Vec<ChaseFilter>,
    positive_operations: Vec<ChaseOperation>,

    negative_atoms: Vec<VariableAtom>,
    negative_filters: Vec<Vec<ChaseFilter>>,
}

impl SeminaiveStrategy {
    /// Create new [SeminaiveStrategy] object.
    pub(crate) fn initialize(rule: &ChaseRule, _analysis: &RuleAnalysis) -> Self {
        Self {
            positive_atoms: rule.positive_body().clone(),
            positive_filters: rule.positive_filters().clone(),
            negative_atoms: rule.negative_body().clone(),
            negative_filters: rule.negative_filters().clone(),
            positive_operations: rule.positive_operations().clone(),
        }
    }
}

impl BodyStrategy for SeminaiveStrategy {
    fn add_plan_body(
        &self,
        table_manager: &TableManager,
        current_plan: &mut SubtableExecutionPlan,
        variable_translation: &VariableTranslation,
        rule_info: &RuleInfo,
        variable_order: &mut VariableOrder,
        step_number: usize,
    ) -> ExecutionNodeRef {
        let join_output_markers = variable_translation.operation_table(variable_order.iter());
        let node_join = node_join(
            current_plan.plan_mut(),
            table_manager,
            variable_translation,
            rule_info.step_last_applied,
            step_number,
            &self.positive_atoms,
            join_output_markers,
        );

        let node_body_functions = node_functions(
            current_plan.plan_mut(),
            variable_translation,
            node_join,
            &self.positive_operations,
        );

        let node_body_filter = node_filter(
            current_plan.plan_mut(),
            variable_translation,
            node_body_functions,
            &self.positive_filters,
        );

        let node_negation = node_negation(
            current_plan.plan_mut(),
            table_manager,
            variable_translation,
            node_body_filter,
            step_number,
            &self.negative_atoms,
            &self.negative_filters,
        );

        let node_result = node_negation;

        current_plan.add_temporary_table(node_result.clone(), "Body");
        node_result
    }
}
