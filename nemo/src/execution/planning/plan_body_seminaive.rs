//! Module defining the strategy for calculating all body matches for a rule application.

use std::collections::HashSet;

use nemo_physical::management::execution_plan::ExecutionNodeRef;

use crate::{
    execution::{execution_engine::RuleInfo, rule_execution::VariableTranslation},
    model::{
        chase_model::{
            variable::is_aggregate_variable, ChaseAggregate, ChaseRule, Constructor, VariableAtom,
        },
        Constraint, Variable,
    },
    program_analysis::{analysis::RuleAnalysis, variable_order::VariableOrder},
    table_manager::{SubtableExecutionPlan, TableManager},
};

use super::{
    operations::{
        aggregate::node_aggregate, filter::node_filter, functions::node_functions, join::node_join,
        negation::node_negation,
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

    aggregates: Vec<ChaseAggregate>,
    aggregate_group_by_variables: Option<HashSet<Variable>>,
}

impl SeminaiveStrategy {
    /// Create new [`SeminaiveStrategy`] object.
    pub(crate) fn initialize(rule: &ChaseRule, analysis: &RuleAnalysis) -> Self {
        let mut used_variables_before_arithmetic_operations = HashSet::<Variable>::new();

        for variable in &analysis.head_variables {
            if let Some(constructor) = rule
                .constructors()
                .iter()
                .find(|c| *c.variable() == *variable)
            {
                used_variables_before_arithmetic_operations
                    .extend(constructor.term().variables().cloned());
            } else {
                used_variables_before_arithmetic_operations.insert(variable.clone());
            }
        }

        let aggregate_group_by_variables: Option<HashSet<_>> = if rule.aggregates().is_empty() {
            None
        } else {
            // Compute group-by variables for all aggregates in the rule
            // This is the set of all universal variables in the head (before any arithmetic operations) except for the aggregated variables
            Some(used_variables_before_arithmetic_operations.iter().filter(|variable| match variable {
                Variable::Universal(_) => !is_aggregate_variable(variable),
                Variable::Existential(_) => panic!("existential head variables are currently not supported together with aggregates"),
                Variable::UnnamedUniversal(_) => !is_aggregate_variable(variable),
            }).cloned().collect())
        };

        Self {
            positive_atoms: rule.positive_body().clone(),
            positive_constraints: rule.positive_constraints().clone(),
            negative_atoms: rule.negative_body().clone(),
            negatie_constraints: rule.negative_constraints().clone(),
            constructors: rule.constructors().clone(),
            aggregates: rule.aggregates().clone(),
            aggregate_group_by_variables,
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

        // Perform aggregate operations
        // This updates the variable order with the aggregate placeholder variables replacing the aggregate input variables
        let node_aggregation = node_aggregate(
            current_plan.plan_mut(),
            variable_translation,
            node_negation,
            &self.aggregates,
            &self.aggregate_group_by_variables,
        );

        let node_functions = node_functions(
            current_plan.plan_mut(),
            variable_translation,
            node_aggregation,
            &self.constructors,
        );

        current_plan.add_temporary_table(node_functions.clone(), "Body Join");

        node_functions
    }
}
