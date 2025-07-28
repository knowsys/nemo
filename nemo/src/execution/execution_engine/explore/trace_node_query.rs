//! This module contains code for executing experiments regarding node queries.

use std::collections::{HashMap, HashSet};

use itertools::Itertools;
use nemo_physical::{
    datavalues::AnyDataValue,
    management::{
        database::{
            DatabaseInstance,
            id::{ExecutionId, PermanentTableId},
            sources::SimpleTable,
        },
        execution_plan::{ColumnOrder, ExecutionPlan},
    },
    tabular::operations::OperationTable,
};

use crate::{
    chase_model::{
        ChaseAtom, GroundAtom, analysis::variable_order::VariableOrder, components::rule::ChaseRule,
    },
    execution::{
        ExecutionEngine,
        planning::operations::{
            filter::node_filter, functions::node_functions, negation::node_negation,
        },
        rule_execution::VariableTranslation,
        selection_strategy::strategy::RuleSelectionStrategy,
        tracing::{
            node_query::{
                TableEntriesForTreeNodesQuery, TableEntriesForTreeNodesQueryInner,
                TableEntriesForTreeNodesResponse, TreeAddress,
            },
            shared::{TableEntryQuery, TableEntryResponse},
        },
    },
    rule_model::components::{
        IterableVariables,
        atom::Atom,
        tag::Tag,
        term::primitive::{Primitive, variable::Variable},
    },
    table_manager::TableManager,
};

#[derive(Debug, Default)]
pub(crate) struct StepToId {
    steps: Vec<usize>,
    ids: Vec<PermanentTableId>,
}

impl StepToId {
    pub fn add_table(&mut self, step: usize, id: PermanentTableId) {
        assert!(
            self.steps
                .last()
                .map(|&last_step| last_step < step)
                .unwrap_or(true)
        );

        self.steps.push(step);
        self.ids.push(id);
    }

    pub fn tables_before(&self, step: usize) -> impl Iterator<Item = PermanentTableId> + '_ {
        self.steps
            .iter()
            .zip(self.ids.iter())
            .take_while(move |(current_step, _)| **current_step < step)
            .map(|(_, id)| *id)
    }

    pub fn all_tables(&self) -> impl Iterator<Item = PermanentTableId> + '_ {
        self.ids.iter().cloned()
    }
}

#[derive(Debug, Default)]
pub(crate) struct TreeTableManager {
    valid: HashMap<TreeAddress, StepToId>,
    assignment: HashMap<TreeAddress, StepToId>,
    result: HashMap<TreeAddress, PermanentTableId>,
    valid_final: HashMap<TreeAddress, PermanentTableId>,
    assignment_final: HashMap<TreeAddress, PermanentTableId>,
    query: HashMap<TreeAddress, PermanentTableId>,
}

impl TreeTableManager {
    pub fn add_valid_table(&mut self, address: &TreeAddress, step: usize, id: PermanentTableId) {
        self.valid
            .entry(address.clone())
            .or_default()
            .add_table(step, id);
    }

    pub fn add_assignment_table(
        &mut self,
        address: &TreeAddress,
        step: usize,
        id: PermanentTableId,
    ) {
        self.assignment
            .entry(address.clone())
            .or_default()
            .add_table(step, id);
    }

    pub fn add_result_table(&mut self, address: &TreeAddress, id: PermanentTableId) {
        self.result.insert(address.clone(), id);
    }

    pub fn add_final_valid_table(&mut self, address: &TreeAddress, id: PermanentTableId) {
        self.valid_final.insert(address.clone(), id);
    }

    pub fn add_final_assignment_table(&mut self, address: &TreeAddress, id: PermanentTableId) {
        self.assignment_final.insert(address.clone(), id);
    }

    pub fn add_query_table(&mut self, address: &TreeAddress, id: PermanentTableId) {
        self.query.insert(address.clone(), id);
    }

    pub fn valid_tables_before(
        &self,
        address: &TreeAddress,
        step: usize,
    ) -> Box<dyn Iterator<Item = PermanentTableId> + '_> {
        match self.valid.get(address) {
            Some(tables) => Box::new(tables.tables_before(step)),
            None => Box::new(std::iter::empty()),
        }
    }

    pub fn valid_tables(
        &self,
        address: &TreeAddress,
    ) -> Box<dyn Iterator<Item = PermanentTableId> + '_> {
        match self.valid.get(address) {
            Some(tables) => Box::new(tables.all_tables()),
            None => Box::new(std::iter::empty()),
        }
    }

    pub fn assignment_tables(
        &self,
        address: &TreeAddress,
    ) -> Box<dyn Iterator<Item = PermanentTableId> + '_> {
        match self.assignment.get(address) {
            Some(tables) => Box::new(tables.all_tables()),
            None => Box::new(std::iter::empty()),
        }
    }

    pub fn result_table(&self, address: &TreeAddress) -> Option<PermanentTableId> {
        self.result.get(address).cloned()
    }

    pub fn final_valid_table(&self, address: &TreeAddress) -> Option<PermanentTableId> {
        self.valid_final.get(address).cloned()
    }

    pub fn final_assignment_table(&self, address: &TreeAddress) -> Option<PermanentTableId> {
        self.assignment_final.get(address).cloned()
    }
}

fn variable_translation(
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
            let new_variable = Variable::universal(&format!("_VH_{}", index));

            order.push(new_variable.clone());
            variable_translation.add_marker(new_variable.clone());

            head_variables.push(new_variable);
        }
    }

    (variable_translation, order, head_variables)
}

fn valid_tables_plan(
    manager: &TreeTableManager,
    table_manager: &TableManager,
    address: &TreeAddress,
    rule: &ChaseRule,
    head_index: usize,
    order: &VariableOrder,
    head_id: PermanentTableId,
    step: usize,
) -> (ExecutionPlan, ExecutionId, ExecutionId) {
    let mut plan = ExecutionPlan::default();

    let (variable_translation, order, head_variables) =
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

    let order_set = order.iter().cloned().collect::<HashSet<_>>();
    let head_set = head_variables.iter().cloned().collect::<HashSet<_>>();
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
    node_join.add_subnode(node_head);

    if let Some(query_id) = manager.query.get(address) {
        let node_query = plan.fetch_table(markers_head.clone(), *query_id);
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

    let node_result = plan.projectreorder(markers_head, node_negation);
    let id_valid = plan.write_permanent(node_result, "valid", "valid");

    (plan, id_valid, id_assignment)
}

fn result_tables_plan(
    manager: &TreeTableManager,
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

fn consolidate_valid_tables(
    database: &mut DatabaseInstance,
    manager: &TreeTableManager,
    address: &TreeAddress,
) -> Option<PermanentTableId> {
    let mut plan = ExecutionPlan::default();
    let mut node_union = plan.union_empty(OperationTable::default());
    for table in manager.valid_tables(address) {
        node_union.add_subnode(plan.fetch_table(OperationTable::default(), table));
    }
    plan.write_permanent(node_union, "union", "union");
    let (_, &result_id) = database.execute_plan(plan).ok()?.iter().next()?;

    Some(result_id)
}

fn consolidate_assignment_tables(
    database: &mut DatabaseInstance,
    manager: &TreeTableManager,
    address: &TreeAddress,
) -> Option<PermanentTableId> {
    let mut plan = ExecutionPlan::default();
    let mut node_union = plan.union_empty(OperationTable::default());
    for table in manager.assignment_tables(address) {
        node_union.add_subnode(plan.fetch_table(OperationTable::default(), table));
    }
    plan.write_permanent(node_union, "union", "union");
    let (_, &result_id) = database.execute_plan(plan).ok()?.iter().next()?;

    Some(result_id)
}

impl<Strategy: RuleSelectionStrategy> ExecutionEngine<Strategy> {
    fn build_query_restriction(
        &mut self,
        manager: &mut TreeTableManager,
        node: &TableEntriesForTreeNodesQueryInner,
        address: TreeAddress,
        predicate: &Tag,
    ) {
        if !node.queries.is_empty() {
            let arity = self.predicate_arity(predicate).expect("invalid predicate");
            let mut simple_table = SimpleTable::new(arity);

            for query in &node.queries {
                let terms = match query {
                    TableEntryQuery::Entry(row_index) => {
                        let terms_to_trace: Vec<AnyDataValue> = self
                            .predicate_rows(&predicate)
                            .expect("unknown predicate")
                            .into_iter()
                            .flatten()
                            .nth(*row_index)
                            .expect("invalid id");

                        terms_to_trace
                    }
                    TableEntryQuery::Query(query_string) => {
                        let atom = Atom::parse(&format!("P({})", query_string))
                            .expect("invalid query string");

                        let ground = GroundAtom::try_from(atom).expect("only support ground fact");
                        ground.datavalues().collect::<Vec<_>>()
                    }
                };

                simple_table.add_row(terms);
            }

            let id = self
                .table_manager
                .database_mut()
                .register_table("query", arity);
            self.table_manager.database_mut().add_source_table(
                id,
                ColumnOrder::default(),
                simple_table,
            );

            manager.add_query_table(&address, id);
        }

        if let Some(successor) = &node.next {
            let rule = self.program.rules()[successor.rule].clone();

            for (index, (atom, node_atom)) in rule
                .positive_body()
                .iter()
                .zip(successor.children.iter())
                .enumerate()
            {
                let mut next_address = address.clone();
                next_address.push(index);

                self.build_query_restriction(manager, node_atom, next_address, &atom.predicate());
            }
        }
    }

    pub(crate) fn build_valid_nodes(
        &mut self,
        manager: &mut TreeTableManager,
        node: &TableEntriesForTreeNodesQueryInner,
        address: TreeAddress,
        predicate: &Tag,
        before_step: usize,
    ) {
        if let Some(successor) = &node.next {
            let rule = self.program.rules()[successor.rule].clone();
            let order =
                self.analysis.rule_analysis[successor.rule].promising_variable_orders[0].clone();

            let next_step = {
                self.rule_history[..before_step]
                    .iter()
                    .rposition(|rule| *rule == successor.rule)
                    .unwrap_or(self.rule_history.len())
            };

            for (index, (atom, node_atom)) in rule
                .positive_body()
                .iter()
                .zip(successor.children.iter())
                .enumerate()
            {
                let mut next_address = address.clone();
                next_address.push(index);

                self.build_valid_nodes(
                    manager,
                    node_atom,
                    next_address,
                    &atom.predicate(),
                    next_step,
                );
            }

            for (step, id) in self.table_manager.tables_in_range_rule_steps(
                predicate,
                0..before_step,
                &self.rule_history,
                successor.rule,
            ) {
                let (plan, id_valid, id_assignment) = valid_tables_plan(
                    manager,
                    &self.table_manager,
                    &address,
                    &rule,
                    successor.head_index,
                    &order,
                    id,
                    step,
                );

                let execution_results = self
                    .table_manager
                    .database_mut()
                    .execute_plan(plan)
                    .expect("execute plan failed");

                if let Some(result_id) = execution_results.get(&id_valid) {
                    manager.add_valid_table(&address, step, *result_id);
                }
                if let Some(result_id) = execution_results.get(&id_assignment) {
                    manager.add_assignment_table(&address, step, *result_id);
                }
            }

            if let Some(id) =
                consolidate_valid_tables(self.table_manager.database_mut(), manager, &address)
            {
                let valid_final_count = self.table_manager.database().count_rows_in_memory(id);
                println!("Address: {:?}, count: {}", address, valid_final_count);

                manager.add_final_valid_table(&address, id);
            } else {
                println!("consolidation failed (valid)");
            }

            if let Some(id) =
                consolidate_assignment_tables(self.table_manager.database_mut(), manager, &address)
            {
                manager.add_final_assignment_table(&address, id);
            } else {
                println!("consolidation failed (assignment)");
            }
        } else {
            // TODO: Projection optimization

            for (step, id) in self
                .table_manager
                .tables_in_range_steps(predicate, 0..before_step)
            {
                manager.add_valid_table(&address, step, id);
            }

            if let Some(id) =
                consolidate_valid_tables(self.table_manager.database_mut(), manager, &address)
            {
                manager.add_final_valid_table(&address, id);
            } else {
                println!("consolidation failed (leaf)");
            }
        }
    }

    pub(crate) fn filter_nodes(
        &mut self,
        manager: &mut TreeTableManager,
        node: &TableEntriesForTreeNodesQueryInner,
        address: TreeAddress,
    ) -> bool {
        if address.is_empty() {
            if let Some(root_table) = manager.final_valid_table(&address) {
                manager.add_result_table(&address, root_table);
            } else {
                println!("no root");
                return false;
            }
        }

        if let Some(successor) = &node.next {
            let rule = self.program.rules()[successor.rule].clone();
            let order =
                self.analysis.rule_analysis[successor.rule].promising_variable_orders[0].clone();

            let (variable_translation, order, head_variables) =
                variable_translation(&rule, successor.head_index, &order);

            for (index, (atom, node_atom)) in rule
                .positive_body()
                .iter()
                .zip(successor.children.iter())
                .enumerate()
            {
                let mut next_address = address.clone();
                next_address.push(index);

                let atom_variables = atom.variables().cloned().collect::<Vec<_>>();

                if let Some(plan) = result_tables_plan(
                    manager,
                    &next_address,
                    &variable_translation,
                    &head_variables,
                    &order,
                    &atom_variables,
                ) {
                    let Ok(results) = self.table_manager.database_mut().execute_plan(plan) else {
                        println!("filter join failed");
                        return false;
                    };

                    let Some((_, result_id)) = results.iter().next() else {
                        println!("filter join empty");
                        return false;
                    };

                    manager.add_result_table(&next_address, *result_id);
                } else {
                    println!("no plan");
                    return false;
                }

                if !self.filter_nodes(manager, node_atom, next_address) {
                    return false;
                }
            }
        }

        true
    }

    pub(crate) fn execute_node_query(
        &mut self,
        query: TableEntriesForTreeNodesQuery,
    ) -> TreeTableManager {
        let mut manager = TreeTableManager::default();
        let address = TreeAddress::default();
        let predicate = Tag::new(query.predicate);
        let node = &query.inner;
        let before_step = self.rule_history.len();

        self.build_query_restriction(&mut manager, node, address.clone(), &predicate);

        self.build_valid_nodes(&mut manager, node, address.clone(), &predicate, before_step);

        let _ = self.filter_nodes(&mut manager, node, address.clone());

        manager
    }

    pub(crate) fn node_query_answer(
        &mut self,
        manager: &TreeTableManager,
        mut response: TableEntriesForTreeNodesResponse,
    ) -> Option<TableEntriesForTreeNodesResponse> {
        for element in &mut response.elements {
            let Some(table_id) = manager.result_table(&element.address) else {
                continue;
            };
            let rows_iter = self
                .table_manager
                .database_mut()
                .table_row_iterator(table_id)
                .ok()?
                .skip(element.pagination.start);

            let rows = if element.entries.capacity() == 0 {
                rows_iter.collect::<Vec<_>>()
            } else {
                rows_iter
                    .take(element.entries.capacity())
                    .collect::<Vec<_>>()
            };

            element.pagination.more = element.pagination.start + rows.len()
                < self.table_manager.database().count_rows_in_memory(table_id);

            for row in rows {
                let entry_id = self
                    .table_manager
                    .table_row_id(&Tag::new(element.predicate.clone()), &row)
                    .expect("row should be contained somewhere");

                let table_response = TableEntryResponse {
                    entry_id,
                    terms: row,
                };

                element.entries.push(table_response);
            }
        }

        Some(response)
    }
}
