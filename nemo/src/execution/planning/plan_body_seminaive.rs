//! Module defining the strategy for calculating all body matches for a rule application.

use std::collections::HashSet;

use nemo_physical::management::execution_plan::ExecutionNodeRef;

use crate::{
    execution::execution_engine::RuleInfo,
    model::{
        chase_model::{variable::is_aggregate_variable, ChaseAggregate, ChaseRule, Constructor},
        Variable,
    },
    program_analysis::{analysis::RuleAnalysis, variable_order::VariableOrder},
    table_manager::{SubtableExecutionPlan, TableManager},
};

use super::{
    aggregates::generate_node_aggregate, arithmetic::generate_node_arithmetic,
    negation::NegationGenerator, plan_util::cut_last_layers, BodyStrategy, SeminaiveJoinGenerator,
};

/// Implementation of the semi-naive existential rule evaluation strategy.
#[derive(Debug)]
pub(crate) struct SeminaiveStrategy {
    /// Variables still in use after aggregations have been performed.
    ///
    /// This includes normal variables in the head and variables that are used by arithmetic operations in the head.
    /// Furthermore, this includes aggregate output variables (but not their input variables, except if they are output variables too).
    used_variables_before_arithmetic_operations: HashSet<Variable>,
    constructors: Vec<Constructor>,
    join_generator: SeminaiveJoinGenerator,
    negation_generator: Option<NegationGenerator>,
    aggregates: Vec<ChaseAggregate>,
    aggregate_group_by_variables: Option<HashSet<Variable>>,
}

impl SeminaiveStrategy {
    /// Create new [`SeminaiveStrategy`] object.
    pub(crate) fn initialize(rule: &ChaseRule, analysis: &RuleAnalysis) -> Self {
        let constructors = rule.constructors().clone();

        let used_variables_before_arithmetic_operations =
            Self::used_variables_before_arithmetic_operations(
                &analysis.head_variables,
                &constructors,
            );

        let join_generator = SeminaiveJoinGenerator {
            atoms: rule.positive_body().clone(),
            constraints: rule.positive_constraints().clone(),
            variable_types: analysis.variable_types.clone(),
        };

        let negation_generator = if !rule.negative_body().is_empty() {
            Some(NegationGenerator {
                variable_types: analysis.variable_types.clone(),
                atoms: rule.negative_body().clone(),
                constraints: rule.negative_constraints().clone(),
            })
        } else {
            None
        };

        let aggregate_group_by_variables: Option<HashSet<_>> = if rule.aggregates().is_empty() {
            None
        } else {
            // Compute group-by variables for all aggregates in the rule
            // This is the set of all universal variables in the head (before any arithmetic operations) except for the aggregated variables
            Some(used_variables_before_arithmetic_operations.iter().filter(|variable| match variable {
                Variable::Universal(_) => !is_aggregate_variable(variable),
                Variable::Existential(_) => panic!("existential head variables are currently not supported together with aggregates"),
            }).cloned().collect())
        };

        Self {
            used_variables_before_arithmetic_operations,
            constructors,
            join_generator,
            negation_generator,
            aggregates: rule.aggregates().clone(),
            aggregate_group_by_variables,
        }
    }

    /// Returns used head variables before aggregates and constructors are computed.
    fn used_variables_before_arithmetic_operations(
        head_variables: &HashSet<Variable>,
        constructors: &[Constructor],
    ) -> HashSet<Variable> {
        let mut used_variables = HashSet::<Variable>::new();

        for variable in head_variables {
            if let Some(constructor) = constructors.iter().find(|c| c.variable() == variable) {
                used_variables.extend(constructor.term().variables().cloned());
            } else {
                used_variables.insert(variable.clone());
            }
        }

        used_variables
    }
}

impl BodyStrategy for SeminaiveStrategy {
    fn add_plan_body(
        &self,
        table_manager: &TableManager,
        current_plan: &mut SubtableExecutionPlan,
        rule_info: &RuleInfo,
        variable_order: &mut VariableOrder,
        step_number: usize,
    ) -> ExecutionNodeRef {
        let mut node_seminaive = self.join_generator.seminaive_join(
            current_plan.plan_mut(),
            table_manager,
            rule_info.step_last_applied,
            step_number,
            variable_order,
        );

        if let Some(generator) = &self.negation_generator {
            node_seminaive = generator.generate_plan(
                current_plan,
                table_manager,
                node_seminaive,
                variable_order,
                step_number,
            )
        }

        if let Some(aggregate_group_by_variables) = &self.aggregate_group_by_variables {
            // Perform aggregate operations
            // This updates the variable order with the aggregate placeholder variables replacing the aggregate input variables
            (node_seminaive, *variable_order) = generate_node_aggregate(
                current_plan,
                variable_order.clone(),
                node_seminaive,
                &self.aggregates,
                aggregate_group_by_variables,
            );

            // This check can be removed when [`nemo_physical::tabular::operations::triescan_aggregate::TrieScanAggregateWrapper`] is removed
            // Currently, this wrapper can only be turned into a partial trie scan using materialization
            if !self.constructors.is_empty() {
                current_plan
                    .add_temporary_table(node_seminaive.clone(), "Subtable Aggregate Arithmetics");
            }
        }

        // Cut away layers not used after arithmetic operations
        let (last_used, cut) = cut_last_layers(
            variable_order,
            &self.used_variables_before_arithmetic_operations,
        );

        let types = &self.join_generator.variable_types;
        // Perform arithmetic operations
        (node_seminaive, *variable_order) = generate_node_arithmetic(
            current_plan.plan_mut(),
            variable_order,
            node_seminaive,
            last_used,
            &self.constructors,
            types,
        );

        current_plan.add_temporary_table_cut(node_seminaive.clone(), "Body Join", cut);

        node_seminaive
    }
}
