//! Module defining the strategy for calculating all body matches for a rule application.

use nemo_physical::management::execution_plan::ExecutionNodeRef;

use crate::{
    execution::{execution_engine::RuleInfo, rule_execution::VariableTranslation},
    model::{
        chase_model::{ChaseRule, Constructor, VariableAtom},
        Constraint,
    },
    program_analysis::{analysis::RuleAnalysis, variable_order::VariableOrder},
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
    positive_constraints: Vec<Constraint>,
    positive_constructors: Vec<Constructor>,

    negative_atoms: Vec<VariableAtom>,
    negative_constraints: Vec<Vec<Constraint>>,
}

impl SeminaiveStrategy {
    /// Create new [SeminaiveStrategy] object.
    pub(crate) fn initialize(rule: &ChaseRule, _analysis: &RuleAnalysis) -> Self {
        Self {
            positive_atoms: rule.positive_body().clone(),
            positive_constraints: rule.positive_constraints().clone(),
            negative_atoms: rule.negative_body().clone(),
            negative_constraints: rule.negative_constraints().clone(),
            positive_constructors: rule.positive_constructors().clone(),
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
            &self.positive_constructors,
        );

        let node_body_filter = node_filter(
            current_plan.plan_mut(),
            variable_translation,
            node_body_functions,
            &self.positive_constraints,
        );

        let node_negation = node_negation(
            current_plan.plan_mut(),
            table_manager,
            variable_translation,
            node_body_filter,
            step_number,
            &self.negative_atoms,
            &self.negative_constraints,
        );

        let node_result = node_negation;

        current_plan.add_temporary_table(node_result.clone(), "Body");
        node_result
    }
}
