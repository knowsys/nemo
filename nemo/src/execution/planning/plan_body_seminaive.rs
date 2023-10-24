//! Module defining the strategy for calculating all body matches for a rule application.

use std::collections::HashSet;

use nemo_physical::{
    management::execution_plan::ExecutionNodeRef,
    tabular::operations::triescan_aggregate::AggregationInstructions,
};

use crate::{
    execution::execution_engine::RuleInfo,
    model::{
        chase_model::{ChaseAggregate, ChaseRule, Constructor, AGGREGATE_VARIABLE_PREFIX},
        Variable,
    },
    program_analysis::{analysis::RuleAnalysis, variable_order::VariableOrder},
    table_manager::{SubtableExecutionPlan, TableManager},
};

use super::{
    arithmetic::generate_node_arithmetic, negation::NegationGenerator, plan_util::cut_last_layers,
    BodyStrategy, SeminaiveJoinGenerator,
};

/// Implementation of the semi-naive existential rule evaluation strategy.
#[derive(Debug)]
pub struct SeminaiveStrategy {
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
    pub fn initialize(rule: &ChaseRule, analysis: &RuleAnalysis) -> Self {
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
            // This is the set off all universal variables in the head except for the aggregated variables
            Some(analysis.head_variables.iter().filter(|variable| match variable {
                Variable::Universal(identifier) => !identifier.0.starts_with(AGGREGATE_VARIABLE_PREFIX),
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

        // Perform aggregate operations
        assert!(
            self.aggregates.len() <= 1,
            "currently only one aggregate term per rule is supported"
        );
        for aggregate in &self.aggregates {
            let aggregate_group_by_variables = self.aggregate_group_by_variables.as_ref().unwrap();

            let aggregated_column_index = *variable_order
                .get(
                    &aggregate
                        .variables
                        .first()
                        .expect("min aggregate requires exactly one variable")
                        .clone(),
                )
                .expect("variable that is aggregated has to be in the variable order");

            // Check that the group-by variables are exactly the first variables in the variable order
            // See [`AggregationInstructions`] and [`TrieScanAggregate`] documentation
            // Otherwise the columns would need to be reordered, which is not supported yet
            for group_by_variable in aggregate_group_by_variables {
                let index = variable_order.get(group_by_variable).expect("aggregate group-by variable is not in variable order, even though it should be a head variable and thus also safe and in the variable order");
                if index >= &aggregate_group_by_variables.len() {
                    panic!("aggregate group by variable {group_by_variable} is at an invalid position in the variable order to allow for aggregation without projection/reorder (index is {index}, but should be smaller than {}).", aggregate_group_by_variables.len());
                }
            }

            let processor = aggregate.aggregate_operation.create_processor::<u64>();
            if !processor.idempotent() {
                // Check that the aggregate variable is directly behind the group-by variables (because there are no distinct variables)
                // This is only required when the operation is not idempotent,
                // because otherwise the the result would not change by intermediate columns in the variable order
                if aggregated_column_index != aggregate_group_by_variables.len() {
                    panic!("aggregated variable {} is at an invalid position in the variable order to allow for aggregation without projection/reorder (index is {aggregated_column_index}, but should equal {}).", aggregate
                    .variables.first().unwrap(), aggregate_group_by_variables.len());
                }
            }

            node_seminaive = current_plan.plan_mut().aggregate(
                node_seminaive,
                AggregationInstructions {
                    aggregate_operation: aggregate.aggregate_operation,
                    group_by_column_count: aggregate_group_by_variables.len(),
                    aggregated_column_index,
                    last_distinct_column_index: aggregated_column_index,
                },
            );

            // Update variable order
            {
                // Remove distinct and unused variables of the aggregate
                *variable_order = variable_order.restrict_to(aggregate_group_by_variables);
                // Add aggregate output variable
                variable_order.push(aggregate.output_variable.clone());
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
