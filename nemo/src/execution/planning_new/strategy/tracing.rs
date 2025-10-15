//! This module defines [StrategyTracing].

use std::collections::HashMap;

use nemo_physical::datavalues::AnyDataValue;

use crate::{
    chase_model::analysis::variable_order::VariableOrder,
    execution::{
        planning_new::{
            RuntimeInformation,
            normalization::{operation::Operation, rule::NormalizedRule},
            operations::{
                function_filter_negation::GeneratorFunctionFilterNegation, join::GeneratorJoin,
                union::UnionRange,
            },
        },
        rule_execution::VariableTranslation,
    },
    io::{ImportManager, resource_providers::ResourceProviders},
    rule_model::components::term::primitive::{ground::GroundTerm, variable::Variable},
    table_manager::{SubtableExecutionPlan, TableManager},
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
        let rule = rule.prepare_tracing();

        let positive = rule.positive().clone();
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

        Self {
            join,
            filter,
            order: order.clone(),
            translation,
        }
    }

    /// Create an execution plan for evaluating a rule.
    pub fn create_plan(
        &self,
        table_manager: &TableManager,
        step_current: usize,
        step_application: usize,
    ) -> SubtableExecutionPlan {
        let mut plan = SubtableExecutionPlan::default();

        let default_import_manager = ImportManager::new(ResourceProviders::default());

        let runtime = RuntimeInformation {
            step_last_application: step_application,
            step_current,
            table_manager,
            import_manager: &default_import_manager,
            order: self.order.clone(),
            translation: self.translation.clone(),
        };

        let node = self.join.create_plan(&mut plan, &runtime);
        self.filter.create_plan(&mut plan, node, &runtime);

        plan
    }
}
