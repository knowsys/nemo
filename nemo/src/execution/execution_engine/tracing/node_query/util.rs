//! This module contains various helper functions
//! for evaluating tree node queries.

use std::collections::{HashMap, HashSet};

use itertools::Itertools;
use nemo_physical::{
    management::{
        database::{
            DatabaseInstance,
            id::{ExecutionId, PermanentTableId},
        },
        execution_plan::ExecutionPlan,
    },
    tabular::operations::OperationTable,
};

use crate::{
    chase_model::{
        ChaseAtom, analysis::variable_order::VariableOrder, components::rule::ChaseRule,
    },
    execution::{
        execution_engine::tracing::node_query::TraceNodeManager,
        planning::operations::{
            filter::node_filter, functions::node_functions, negation::node_negation,
        },
        rule_execution::VariableTranslation,
        tracing::node_query::TreeAddress,
    },
    rule_model::components::{
        IterableVariables,
        term::primitive::{Primitive, variable::Variable},
    },
    table_manager::TableManager,
};

/// For a given [ChaseRule],
/// compute useful objects which are required to construct execution plans.
///
/// Returns a [VariableTranslation], a [VariableOrder],
/// and a list of [Variable]s used in the head.
pub(super) fn variable_translation(
    rule: &ChaseRule,
    head_index: usize,
    order: &VariableOrder,
) -> (VariableTranslation, VariableOrder, Vec<Variable>) {
    let mut variable_translation = VariableTranslation::new();
    for variable in rule.variables().cloned() {
        variable_translation.add_marker(variable);
    }
    let mut order = order.clone();

    let mut head_variables = Vec::<Variable>::default();
    let mut used_variables = HashSet::<&Variable>::new();
    for (index, term) in rule.head()[head_index].terms().enumerate() {
        let create_new_variable = match term {
            Primitive::Variable(variable) => {
                if variable.is_universal()
                    && used_variables.insert(variable)
                    && !(Some(variable)
                        == rule
                            .aggregate()
                            .map(|aggregate| aggregate.output_variable()))
                {
                    if !rule
                        .positive_body()
                        .iter()
                        .flat_map(|atom| atom.variables())
                        .contains(variable)
                    {
                        order.push(variable.clone());
                    }

                    head_variables.push(variable.clone());

                    false
                } else {
                    true
                }
            }
            Primitive::Ground(_) => true,
        };

        if create_new_variable {
            let new_variable = Variable::universal(&format!("_VH_{index}"));

            order.push(new_variable.clone());
            variable_translation.add_marker(new_variable.clone());

            head_variables.push(new_variable);
        }
    }

    (variable_translation, order, head_variables)
}

/// Compute the execution plan to obtain valid tables.
#[allow(clippy::too_many_arguments)]
pub(super) fn valid_tables_plan(
    manager: &TraceNodeManager,
    table_manager: &TableManager,
    address: &TreeAddress,
    rule: &ChaseRule,
    head_index: usize,
    discarded_columns: &[usize],
    order: &VariableOrder,
    head_id: PermanentTableId,
    step: usize,
    need_valid: bool,
) -> (ExecutionPlan, Option<ExecutionId>, ExecutionId) {
    let mut plan = ExecutionPlan::default();

    let (variable_translation, mut order, head_variables) =
        variable_translation(rule, head_index, order);

    let is_aggregate = Some(head_index) == rule.aggregate_head_index();

    let aggregate_set = if is_aggregate {
        let aggregate = rule.aggregate().expect("is aggregate");

        aggregate
            .group_by_variables()
            .iter()
            .chain(aggregate.distinct_variables().iter())
            .chain(std::iter::once(aggregate.input_variable()))
            .cloned()
            .collect::<HashSet<_>>()
    } else {
        HashSet::default()
    };

    let body_set = rule.variables().cloned().collect::<HashSet<_>>();
    let head_set = head_variables
        .iter()
        .enumerate()
        .filter_map(|(index, variable)| {
            if !discarded_columns.contains(&index) {
                Some(variable.clone())
            } else {
                None
            }
        })
        .collect::<HashSet<_>>();
    let disjoint = head_set.is_disjoint(&body_set);

    if disjoint {
        order = order._restrict_to(&body_set);
    }

    let order_set = order.iter().cloned().collect::<HashSet<_>>();
    let head_set = head_set
        .union(&aggregate_set)
        .cloned()
        .collect::<HashSet<_>>();
    let single_variables = order_set.difference(&head_set).collect::<Vec<_>>();
    let single_exist = !single_variables.is_empty();
    let single_markers = variable_translation.operation_table(single_variables.into_iter());

    let markers_head = variable_translation.operation_table(head_variables.iter());
    let node_head = plan.fetch_table(markers_head.clone(), head_id);

    let markers_join = variable_translation.operation_table(order.iter());

    let mut node_join = plan.join_empty(markers_join);

    if !disjoint {
        node_join.add_subnode(node_head);
    }

    if let Some(query_id) = manager.query_table(address) {
        let node_query = plan.fetch_table(markers_head.clone(), query_id);
        node_join.add_subnode(node_query);
    }

    for (body_index, body_atom) in rule.positive_body().iter().enumerate() {
        let mut body_address = address.clone();
        body_address.push(body_index);

        let markers_union = variable_translation.operation_table(body_atom.terms());
        let mut node_union = plan.union_empty(markers_union.clone());

        for id in manager.valid_tables_before(&body_address, step) {
            node_union.add_subnode(plan.fetch_table(markers_union.clone(), id));
        }

        node_join.add_subnode(node_union);
    }

    let node_body_functions = node_functions(
        &mut plan,
        &variable_translation,
        node_join,
        rule.positive_operations(),
    );

    let node_body_filter = node_filter(
        &mut plan,
        &variable_translation,
        node_body_functions,
        rule.positive_filters(),
    );

    let node_negation = node_negation(
        &mut plan,
        table_manager,
        &variable_translation,
        node_body_filter,
        step,
        rule.negative_body(),
        rule.negative_filters(),
    );

    let node_negation = if single_exist {
        plan.single(node_negation, single_markers)
    } else {
        node_negation
    };

    let id_assignment = plan.write_permanent(node_negation.clone(), "assignment", "assignment");

    let id_valid = if disjoint {
        None
    } else if need_valid {
        let node_result = plan.projectreorder(markers_head, node_negation);
        let id_valid = plan.write_permanent(node_result, "valid", "valid");
        Some(id_valid)
    } else {
        None
    };

    (plan, id_valid, id_assignment)
}

/// Return the set of variables in a [ChaseRule]
/// that are only used once, i.e. are unrestricted and unused.
pub(super) fn unique_variables(rule: &ChaseRule) -> HashSet<Variable> {
    let mut result = HashMap::<Variable, usize>::new();

    for variable in rule.variables() {
        let value = result.entry(variable.clone()).or_insert(0);
        *value += 1;
    }

    result
        .into_iter()
        .filter_map(|(variable, count)| if count == 1 { Some(variable) } else { None })
        .collect::<HashSet<_>>()
}

/// Compute the execution plan to obtain the final result tables.
pub(super) fn result_tables_plan(
    manager: &TraceNodeManager,
    address: &TreeAddress,
    variable_translation: &VariableTranslation,
    head_variables: &[Variable],
    order: &VariableOrder,
    body_variables: &[Variable],
) -> Option<ExecutionPlan> {
    let mut parent_address = address.clone();
    parent_address.pop();

    let markers_head = variable_translation.operation_table(head_variables.iter());
    let markers_join = variable_translation.operation_table(order.iter());
    let markers_body = variable_translation.operation_table(body_variables.iter());

    let id_head = manager.result_table(&parent_address)?;
    let id_join = manager.final_assignment_table(&parent_address)?;
    let id_body = manager.final_valid_table(address)?;

    let mut plan = ExecutionPlan::default();

    let node_head = plan.fetch_table(markers_head.clone(), id_head);
    let node_join = plan.fetch_table(markers_join.clone(), id_join);
    let node_body = plan.fetch_table(markers_body.clone(), id_body);

    let node_filter_results = plan.join(markers_join, vec![node_head, node_join, node_body]);

    let node_project = plan.projectreorder(markers_body, node_filter_results);
    plan.write_permanent(node_project, "filter", "filter");

    Some(plan)
}

/// Compute the union of all valid tables associated with the given node.
pub(super) fn consolidate_valid_tables(
    database: &mut DatabaseInstance,
    manager: &TraceNodeManager,
    address: &TreeAddress,
) -> Option<PermanentTableId> {
    if manager.valid_tables(address).count() == 1 {
        return Some(manager.valid_tables(address).next().unwrap());
    }

    let mut plan = ExecutionPlan::default();
    let mut node_union = plan.union_empty(OperationTable::default());
    for table in manager.valid_tables(address) {
        node_union.add_subnode(plan.fetch_table(OperationTable::default(), table));
    }
    plan.write_permanent(node_union, "union", "union");
    let (_, &result_id) = database.execute_plan(plan).ok()?.iter().next()?;

    Some(result_id)
}

/// Compute the union of all assignment tables associated with the given node.
pub(super) fn consolidate_assignment_tables(
    database: &mut DatabaseInstance,
    manager: &TraceNodeManager,
    address: &TreeAddress,
) -> Option<PermanentTableId> {
    if manager.assignment_tables(address).count() == 1 {
        return Some(manager.assignment_tables(address).next().unwrap());
    }

    let mut plan = ExecutionPlan::default();
    let mut node_union = plan.union_empty(OperationTable::default());
    for table in manager.assignment_tables(address) {
        node_union.add_subnode(plan.fetch_table(OperationTable::default(), table));
    }
    plan.write_permanent(node_union, "union", "union");
    let (_, &result_id) = database.execute_plan(plan).ok()?.iter().next()?;

    Some(result_id)
}

/// Project away discarded columns in leaf nodes.
pub(super) fn ignore_discarded_columns_base(
    database: &mut DatabaseInstance,
    id: PermanentTableId,
    arity: usize,
    discarded_columns: &[usize],
) -> Option<PermanentTableId> {
    let markers = OperationTable::new_unique(arity);
    let mut single_markers = OperationTable::default();
    for discarded in discarded_columns {
        single_markers.push(*markers.get(*discarded));
    }

    let mut plan = ExecutionPlan::default();
    let node_load = plan.fetch_table(markers, id);
    let node_single = plan.single(node_load, single_markers);

    plan.write_permanent(node_single, "single", "single");
    database
        .execute_plan(plan)
        .expect("error while executing plan")
        .iter()
        .next()
        .map(|(_, &result_id)| result_id)
}
