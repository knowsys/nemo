//! Module defining the strategy for calculating all body matches for a rule application.

use nemo_physical::management::execution_plan::ExecutionNodeRef;

use crate::{
    execution::{execution_engine::RuleInfo, rule_execution::VariableTranslation},
    model::{
        chase_model::{ChaseAggregate, ChaseRule, Constructor, VariableAtom},
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

    negative_atoms: Vec<VariableAtom>,
    negatie_constraints: Vec<Constraint>,

    constructors: Vec<Constructor>,

    _aggregates: Vec<ChaseAggregate>,
    // TODO: Reimplement aggregation
    // aggregate_group_by_variables: Option<HashSet<Variable>>,
}

impl SeminaiveStrategy {
    /// Create new [`SeminaiveStrategy`] object.
    pub(crate) fn initialize(rule: &ChaseRule, analysis: &RuleAnalysis) -> Self {
        Self {
            positive_atoms: rule.positive_body().clone(),
            positive_constraints: rule.positive_constraints().clone(),
            negative_atoms: rule.negative_body().clone(),
            negatie_constraints: rule.negative_constraints().clone(),
            constructors: rule.constructors().clone(),
            _aggregates: rule.aggregates().clone(),
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

        let node_filter = node_filter(
            current_plan.plan_mut(),
            variable_translation,
            node_join,
            &self.positive_constraints,
        );

        let node_negation = node_negation(
            current_plan.plan_mut(),
            table_manager,
            variable_translation,
            node_filter,
            step_number,
            &self.negative_atoms,
            &self.negatie_constraints,
        );

        // TODO: Reimplement aggregation
        // if let Some(aggregate_group_by_variables) = &self.aggregate_group_by_variables {
        //     // Perform aggregate operations
        //     // This updates the variable order with the aggregate placeholder variables replacing the aggregate input variables
        //     (node_seminaive, *variable_order) = generate_node_aggregate(
        //         current_plan,
        //         variable_order.clone(),
        //         node_seminaive,
        //         &self.aggregates,
        //         aggregate_group_by_variables,
        //     );
        // }

        let node_functions = node_functions(
            current_plan.plan_mut(),
            variable_translation,
            node_negation,
            &self.constructors,
        );

        current_plan.add_temporary_table(node_functions.clone(), "Body Join");

        node_functions
    }
}
