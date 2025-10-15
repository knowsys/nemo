//! This module contains various helper functions
//! for evaluating tree node queries.

use std::{
    collections::{HashMap, HashSet},
    ops::Range,
};

use itertools::Itertools;
use nemo_physical::{
    management::{
        database::{
            DatabaseInstance,
            id::{ExecutionId, PermanentTableId},
        },
        execution_plan::{ExecutionNodeRef, ExecutionPlan},
    },
    tabular::operations::{Filters, FunctionAssignment, OperationTable},
};

use crate::{
    execution::{
        execution_engine::tracing::node_query::TraceNodeManager,
        planning_new::{
            VariableTranslation,
            analysis::variable_order::VariableOrder,
            normalization::{atom::body::BodyAtom, operation::Operation, rule::NormalizedRule},
            operations::function_filter_negation::GeneratorFunctionFilterNegation,
        },
        tracing::node_query::TreeAddress,
    },
    rule_model::components::{
        tag::Tag,
        term::primitive::{Primitive, variable::Variable},
    },
    table_manager::TableManager,
};

/// For a given [NormalizedRule],
/// compute useful objects which are required to construct execution plans.
///
/// Returns a [VariableTranslation], a [VariableOrder],
/// and a list of [Variable]s used in the head.
pub(super) fn variable_translation(
    rule: &NormalizedRule,
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
                        .positive_all()
                        .iter()
                        .flat_map(|atom| atom.terms())
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
    rule: &NormalizedRule,
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

    let is_aggregate = Some(head_index) == rule.aggregate_index();

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
        order = order.restrict_to(&body_set);
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

    for (body_index, body_atom) in rule.positive_all().iter().enumerate() {
        let mut body_address = address.clone();
        body_address.push(body_index);

        let markers_union = variable_translation.operation_table(body_atom.terms());
        let mut node_union = plan.union_empty(markers_union.clone());

        for id in manager.valid_tables_before(&body_address, step) {
            node_union.add_subnode(plan.fetch_table(markers_union.clone(), id));
        }

        node_join.add_subnode(node_union);
    }

    let input_variables = Vec::default();

    let mut operations = rule.operations().clone();
    let mut atoms_negations = rule.negative().clone();

    let generator = GeneratorFunctionFilterNegation::new(
        input_variables,
        &mut operations,
        &mut atoms_negations,
    );

    let node_body_functions = node_functions(
        &mut plan,
        &variable_translation,
        node_join,
        generator.functions(),
    );

    let node_body_filter = node_filter(
        &mut plan,
        &variable_translation,
        node_body_functions,
        generator.filters(),
    );

    let node_negation = node_negation(
        &mut plan,
        table_manager,
        &variable_translation,
        node_body_filter,
        step,
        generator.negations(),
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

/// Return the set of variables in a [NormalizedRule]
/// that are only used once, i.e. are unrestricted and unused.
pub(super) fn unique_variables(rule: &NormalizedRule) -> HashSet<Variable> {
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
pub(super) async fn consolidate_valid_tables(
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
    let (_, &result_id) = database.execute_plan(plan).await.ok()?.iter().next()?;

    Some(result_id)
}

/// Compute the union of all assignment tables associated with the given node.
pub(super) async fn consolidate_assignment_tables(
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
    let (_, &result_id) = database.execute_plan(plan).await.ok()?.iter().next()?;

    Some(result_id)
}

/// Project away discarded columns in leaf nodes.
pub(super) async fn ignore_discarded_columns_base(
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
        .await
        .expect("error while executing plan")
        .iter()
        .next()
        .map(|(_, &result_id)| result_id)
}

/// Calculate helper structures that define the filters that need to be applied.
pub(crate) fn node_filter<'a, FilterIter>(
    plan: &mut ExecutionPlan,
    translation: &VariableTranslation,
    subnode: ExecutionNodeRef,
    filters: FilterIter,
) -> ExecutionNodeRef
where
    FilterIter: Iterator<Item = &'a Operation>,
{
    let trees = filters
        .map(|operation| operation.function_tree(translation))
        .collect::<Filters>();

    plan.filter(subnode, trees)
}

/// Calculate helper structures that define the filters that need to be applied.
pub(crate) fn node_functions<'a, OperationIter>(
    plan: &mut ExecutionPlan,
    translation: &VariableTranslation,
    subnode: ExecutionNodeRef,
    operations: OperationIter,
) -> ExecutionNodeRef
where
    OperationIter: Iterator<Item = &'a (Variable, Operation)>,
{
    let mut output_markers = subnode.markers_cloned();
    let mut assignments = FunctionAssignment::new();

    for (variable, operation) in operations {
        let marker = *translation.get(variable).expect("All variables are known");
        let function_tree = operation.function_tree(translation);

        assignments.insert(marker, function_tree);
        output_markers.push(marker);
    }

    plan.function(output_markers, subnode, assignments)
}

/// Compute the appropriate execution plan to evaluate negated atoms.
pub(crate) fn node_negation<NegationIter>(
    plan: &mut ExecutionPlan,
    table_manager: &TableManager,
    translation: &VariableTranslation,
    node_main: ExecutionNodeRef,
    current_step_number: usize,
    negation: NegationIter,
) -> ExecutionNodeRef
where
    NegationIter: Iterator<Item = (BodyAtom, Vec<Operation>)>,
{
    let subtracted = negation
        .map(|(atom, constraints)| {
            let subtract_markers = translation.operation_table(atom.terms());

            let node = subplan_union(
                plan,
                table_manager,
                &atom.predicate(),
                0..current_step_number,
                subtract_markers.clone(),
            );

            let node_filtered = node_filter(plan, translation, node, constraints.iter());

            // The tables may contain columns that are not part of `node_main`.
            // These need to be projected away.
            let markers_project_target = node_main.markers_cloned().restrict(&subtract_markers);
            plan.projectreorder(markers_project_target, node_filtered)
        })
        .collect();

    plan.subtract(node_main, subtracted)
}

/// Given a predicate and a range of execution steps,
/// adds to the given [ExecutionPlan]
/// a node representing the union of subtables within that range.
///
/// Note that only the output of the union will receive column markers.
pub(crate) fn subplan_union(
    plan: &mut ExecutionPlan,
    table_manager: &TableManager,
    predicate: &Tag,
    steps: Range<usize>,
    output_markers: OperationTable,
) -> ExecutionNodeRef {
    let subtables = table_manager
        .tables_in_range(predicate, &steps)
        .into_iter()
        .map(|id| plan.fetch_table(OperationTable::default(), id))
        .collect();

    plan.union(output_markers, subtables)
}
