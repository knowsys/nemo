//! Module defining the strategy for calculating all body matches for a rule application.

use std::collections::HashMap;

use nemo_physical::{datavalues::AnyDataValue, management::execution_plan::ExecutionNodeRef};

use crate::{
    chase_model::{
        analysis::variable_order::VariableOrder,
        components::{
            atom::variable_atom::VariableAtom,
            filter::ChaseFilter,
            operation::ChaseOperation,
            rule::ChaseRule,
            term::operation_term::{Operation, OperationTerm},
        },
    },
    execution::{rule_execution::VariableTranslation, tracing::error::TracingError},
    rule_model::components::{
        tag::Tag,
        term::{
            aggregate::AggregateKind,
            operation::operation_kind::OperationKind,
            primitive::{variable::Variable, Primitive},
        },
        IterableVariables,
    },
    table_manager::{SubtableExecutionPlan, SubtableIdentifier, TableManager},
};

use super::operations::{
    filter::node_filter, functions::node_functions, join::node_join, negation::node_negation,
};

/// Implementation of the semi-naive existential rule evaluation strategy.
#[derive(Debug)]
pub(crate) struct TracingStrategy {
    positive_atoms: Vec<VariableAtom>,
    positive_filters: Vec<ChaseFilter>,
    positive_operations: Vec<ChaseOperation>,

    negative_atoms: Vec<VariableAtom>,
    negative_filters: Vec<Vec<ChaseFilter>>,

    variable_translation: VariableTranslation,
}

impl TracingStrategy {
    /// Create new [TracingStrategy] object.
    pub(crate) fn initialize(
        rule: &ChaseRule,
        grounding: HashMap<Variable, AnyDataValue>,
    ) -> Result<Self, TracingError> {
        if let Some(aggregate) = rule.aggregate() {
            match aggregate.aggregate_kind() {
                AggregateKind::CountValues | AggregateKind::SumOfNumbers => {
                    return Err(TracingError::UnsupportedFeatureNonMinMaxAggregation)
                }
                AggregateKind::MinNumber | AggregateKind::MaxNumber => {}
            }
        }

        if !rule.aggregate_operations().is_empty() || !rule.aggregate_filters().is_empty() {
            return Err(TracingError::UnsupportedFeatureComplexAggregates);
        }

        let mut variable_translation = VariableTranslation::new();
        for variable in rule.variables().cloned() {
            variable_translation.add_marker(variable);
        }

        let mut positive_filters = rule.positive_filters().clone();
        let positive_operations = rule.positive_operations().clone();

        let operations = rule
            .positive_operations()
            .iter()
            .map(|operation| (operation.variable().clone(), operation.operation().clone()))
            .collect::<HashMap<Variable, OperationTerm>>();

        for (mut variable, value) in grounding {
            if let Some(term) = operations.get(&variable) {
                let filter = ChaseFilter::new(OperationTerm::Operation(Operation::new(
                    OperationKind::Equal,
                    vec![
                        OperationTerm::Primitive(Primitive::from(value)),
                        term.clone(),
                    ],
                )));
                positive_filters.push(filter);
            } else {
                if let Some(aggregate) = rule.aggregate() {
                    if &variable == aggregate.output_variable() {
                        variable = aggregate.input_variable().clone();
                    }
                }

                let filter = ChaseFilter::new(OperationTerm::Operation(Operation::new(
                    OperationKind::Equal,
                    vec![
                        OperationTerm::Primitive(Primitive::from(variable)),
                        OperationTerm::Primitive(Primitive::from(value)),
                    ],
                )));

                positive_filters.push(filter);
            }
        }

        Ok(Self {
            positive_atoms: rule.positive_body().clone(),
            positive_filters,
            positive_operations,
            negative_atoms: rule.negative_body().clone(),
            negative_filters: rule.negative_filters().clone(),
            variable_translation,
        })
    }

    pub(crate) fn add_plan(
        &self,
        table_manager: &TableManager,
        current_plan: &mut SubtableExecutionPlan,
        variable_order: &mut VariableOrder,
        step_number: usize,
    ) -> ExecutionNodeRef {
        let join_output_markers = self
            .variable_translation
            .operation_table(variable_order.iter());
        let node_join = node_join(
            current_plan.plan_mut(),
            table_manager,
            &self.variable_translation,
            0,
            step_number,
            &self.positive_atoms,
            join_output_markers,
        );

        let node_body_functions = node_functions(
            current_plan.plan_mut(),
            &self.variable_translation,
            node_join,
            &self.positive_operations,
        );

        let node_filter = node_filter(
            current_plan.plan_mut(),
            &self.variable_translation,
            node_body_functions,
            &self.positive_filters,
        );

        let node_negation = node_negation(
            current_plan.plan_mut(),
            table_manager,
            &self.variable_translation,
            node_filter,
            step_number,
            &self.negative_atoms,
            &self.negative_filters,
        );

        current_plan.add_permanent_table(
            node_negation.clone(),
            "Tracing Query",
            "Tracing Query",
            SubtableIdentifier::new(Tag::new(String::from("_TRACING")), step_number),
        );

        *variable_order = VariableOrder::default();
        for marker in node_negation.markers_cloned() {
            if let Some(variable) = self.variable_translation.find(&marker) {
                variable_order.push(variable.clone());
            }
        }

        node_negation
    }
}
