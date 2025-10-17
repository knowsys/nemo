//! This module defines [StrategyTracing].

use std::collections::{HashMap, HashSet};

use nemo_physical::{datavalues::AnyDataValue, tabular::trie::Trie};

use crate::{
    error::Error,
    execution::planning::{
        RuntimeInformation, VariableTranslation,
        analysis::variable_order::VariableOrder,
        normalization::{operation::Operation, rule::NormalizedRule},
        operations::{
            function_filter_negation::GeneratorFunctionFilterNegation, join::GeneratorJoin,
            union::UnionRange,
        },
    },
    io::{ImportManager, resource_providers::ResourceProviders},
    rule_model::components::{
        tag::Tag,
        term::primitive::{ground::GroundTerm, variable::Variable},
    },
    table_manager::{SubtableExecutionPlan, SubtableIdentifier, TableManager},
};

/// Strategy for creating an execution plan
/// for tracing facts
#[derive(Debug)]
pub struct StrategyTracing {
    /// Join
    join: GeneratorJoin,
    /// Filter
    filter: GeneratorFunctionFilterNegation,

    /// Variable order
    order: VariableOrder,
    /// Variable translation
    translation: VariableTranslation,
}

impl StrategyTracing {
    /// Creata a new [StrategyTracing].
    pub fn new(rule: &NormalizedRule, grounding: HashMap<Variable, AnyDataValue>) -> Self {
        let positive = rule.positive_all().clone();
        let ranges = vec![UnionRange::Old; positive.len()];
        let mut negative = rule.negative().clone();
        let mut operations = rule.operations().clone();
        let order = rule.variable_order();

        for (variable, value) in grounding {
            let new_operation =
                Operation::new_assignment(variable, Operation::new_ground(GroundTerm::from(value)));
            operations.push(new_operation);
        }

        let mut translation = VariableTranslation::new();
        for variable in rule.variables() {
            translation.add_marker(variable.clone());
        }

        let join = GeneratorJoin::new(positive, ranges, order);
        let filter = GeneratorFunctionFilterNegation::new(
            join.output_variables(),
            &mut operations,
            &mut negative,
        );

        let output_variables = filter
            .output_variables()
            .into_iter()
            .collect::<HashSet<_>>();
        let order = order.restrict_to(&output_variables);

        Self {
            join,
            filter,
            order,
            translation,
        }
    }

    /// Return the variables marking the column of the node
    /// created by `create_plan`.
    pub fn output_variables(&self) -> Vec<Variable> {
        self.order.as_ordered_list()
    }

    /// Creates an execution plan.
    fn crate_plan(&self, table_manager: &mut TableManager, step: usize) -> SubtableExecutionPlan {
        let mut plan = SubtableExecutionPlan::default();

        let default_import_manager = ImportManager::new(ResourceProviders::default());

        let runtime = RuntimeInformation {
            step_last_application: step,
            step_current: step,
            table_manager,
            import_manager: &default_import_manager,
            translation: self.translation.clone(),
        };

        let node_join = self.join.create_plan(&mut plan, &runtime);
        let node_filter = self.filter.create_plan(&mut plan, node_join, &runtime);

        plan.add_permanent_table(
            node_filter.clone(),
            "Tracing Query",
            "Tracing Query",
            SubtableIdentifier::new(Tag::new(String::from("_TRACING")), step),
        );

        plan
    }

    /// Create and execute the execution plan defined by this strategy.
    ///
    /// This function returns the first match, if it exists.
    pub async fn execute(
        &self,
        table_manager: &mut TableManager,
        step: usize,
    ) -> Option<Vec<AnyDataValue>> {
        let plan = self.crate_plan(table_manager, step);
        table_manager.execute_plan_first_match(plan).await
    }

    /// Create an execute the execution plan defined by this strategy.
    ///
    /// This function returns a [Trie] of all matches.
    pub async fn execute_all(
        &self,
        table_manager: &mut TableManager,
        step: usize,
    ) -> Result<Vec<Trie>, Error> {
        let plan = self.crate_plan(table_manager, step);
        table_manager.execute_plan_trie(plan).await
    }
}
