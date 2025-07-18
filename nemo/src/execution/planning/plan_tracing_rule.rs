//! Module defining the strategy for

use std::ops::Range;

use nemo_physical::{
    management::execution_plan::{ExecutionNodeRef, ExecutionPlan},
    tabular::operations::OperationTable,
};

use crate::{
    chase_model::{
        analysis::variable_order::VariableOrder,
        components::{
            atom::{primitive_atom::PrimitiveAtom, variable_atom::VariableAtom},
            filter::ChaseFilter,
            operation::ChaseOperation,
            rule_tracing::{TracingChaseRule, VariableRuleAtom},
        },
        ChaseAtom,
    },
    execution::{
        planning::operations::union::subplan_union, rule_execution::VariableTranslation,
        tracing::error::TracingError,
    },
    rule_model::components::{tag::Tag, term::primitive::Primitive, IterableVariables},
    table_manager::{SubtableExecutionPlan, SubtableIdentifier, TableManager},
};

use super::operations::{filter::node_filter, functions::node_functions, negation::node_negation};

/// Implementation of the semi-naive existential rule evaluation strategy.
#[derive(Debug)]
pub(crate) struct RuleTracingStrategy {
    head_atoms: Vec<PrimitiveAtom>,

    positive_atoms: Vec<VariableRuleAtom>,
    positive_filters: Vec<ChaseFilter>,
    positive_operations: Vec<ChaseOperation>,

    negative_atoms: Vec<VariableAtom>,
    negative_filters: Vec<Vec<ChaseFilter>>,

    variable_translation: VariableTranslation,

    rules: Vec<usize>,
}

impl RuleTracingStrategy {
    /// Create new [TracingStrategy] object.
    pub(crate) fn initialize(
        rule: &TracingChaseRule,
        rules: Vec<usize>,
    ) -> Result<Self, TracingError> {
        let mut variable_translation = VariableTranslation::new();
        for variable in rule.variables().cloned() {
            variable_translation.add_marker(variable);
        }

        let positive_filters = rule.positive_filters().clone();
        let positive_operations = rule.positive_operations().clone();

        Ok(Self {
            head_atoms: rule.head().clone(),
            positive_atoms: rule.positive_body().clone(),
            positive_filters,
            positive_operations,
            negative_atoms: rule.negative_body().clone(),
            negative_filters: rule.negative_filters().clone(),
            variable_translation,
            rules,
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
        let node_join = node_join_rule(
            current_plan.plan_mut(),
            table_manager,
            &self.variable_translation,
            0,
            step_number,
            &self.positive_atoms,
            join_output_markers,
            &self.rules,
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

        current_plan.add_temporary_table(node_negation.clone(), "Tracing Body");

        for head in &self.head_atoms {
            let head_markers =
                self.variable_translation
                    .operation_table(head.terms().map(|term| match term {
                        Primitive::Variable(variable) => variable,
                        Primitive::Ground(_) => unreachable!(),
                    }));

            let node_project = current_plan
                .plan_mut()
                .projectreorder(head_markers, node_negation.clone());

            current_plan.add_permanent_table(
                node_project,
                "Tracing",
                "Tracing",
                SubtableIdentifier::new(head.predicate(), step_number),
            );
        }

        *variable_order = VariableOrder::default();
        for marker in node_negation.markers_cloned() {
            if let Some(variable) = self.variable_translation.find(&marker) {
                variable_order.push(variable.clone());
            }
        }

        node_negation
    }
}

pub(crate) fn subplan_union_rule(
    plan: &mut ExecutionPlan,
    table_manager: &TableManager,
    predicate: &Tag,
    steps: Range<usize>,
    rules: &[usize],
    rule: usize,
    output_markers: OperationTable,
) -> ExecutionNodeRef {
    let tables = table_manager.tables_in_range_rule(predicate, steps, rules, rule);
    println!("tables: {}", tables.len());

    let subtables = tables
        .into_iter()
        .map(|id| plan.fetch_table(OperationTable::default(), id))
        .collect();

    plan.union(output_markers, subtables)
}

/// Compute the appropriate execution plan to perform a join with the seminaive evaluation strategy.
pub(crate) fn node_join_rule(
    plan: &mut ExecutionPlan,
    table_manager: &TableManager,
    variable_translation: &VariableTranslation,
    _step_last_applied: usize,
    current_step_number: usize,
    input_atoms: &[VariableRuleAtom],
    output_markers: OperationTable,
    rules: &[usize],
) -> ExecutionNodeRef {
    let mut node_result = plan.join_empty(output_markers.clone());
    println!("step: {}", current_step_number);

    for atom in input_atoms {
        let atom_markers = variable_translation.operation_table(atom.variables());

        let subnode = if let Some(rule) = atom.rule() {
            subplan_union_rule(
                plan,
                table_manager,
                &atom.predicate(),
                0..current_step_number,
                rules,
                rule,
                atom_markers,
            )
        } else {
            subplan_union(
                plan,
                table_manager,
                &atom.predicate(),
                0..current_step_number,
                atom_markers,
            )
        };

        node_result.add_subnode(subnode);
    }

    node_result
}
