//! This module contains code for implementing provenance node queries.

use std::collections::{HashMap, HashSet};

use itertools::Itertools;
use nemo_physical::{
    management::{
        database::id::{ExecutionId, PermanentTableId},
        execution_plan::{ExecutionNodeRef, ExecutionPlan},
    },
    tabular::operations::OperationTable,
};

use crate::{
    chase_model::{
        ChaseAtom,
        analysis::variable_order::VariableOrder,
        components::{
            atom::{primitive_atom::PrimitiveAtom, variable_atom::VariableAtom},
            rule::ChaseRule,
        },
    },
    execution::{
        ExecutionEngine,
        planning::operations::{
            append::{head_instruction_from_atom, node_head_instruction},
            filter::node_filter,
            functions::node_functions,
            negation::node_negation,
        },
        rule_execution::VariableTranslation,
        selection_strategy::strategy::RuleSelectionStrategy,
        tracing::{
            node_query::{
                TableEntriesForTreeNodesQuery, TableEntriesForTreeNodesQueryInner,
                TableEntriesForTreeNodesResponse, TreeAddress,
            },
            shared::TableEntryResponse,
        },
    },
    rule_model::components::{
        IterableVariables,
        tag::Tag,
        term::primitive::{Primitive, variable::Variable},
    },
    table_manager::TableManager,
};

#[derive(Debug, Default)]
pub(crate) struct ProvenanceTableManager {
    valid: HashMap<TreeAddress, PermanentTableId>,
    assignment: HashMap<TreeAddress, PermanentTableId>,
    result: HashMap<TreeAddress, PermanentTableId>,
}

impl ProvenanceTableManager {
    pub fn add_valid_table(&mut self, address: &TreeAddress, id: PermanentTableId) {
        self.valid.insert(address.clone(), id);
    }

    pub fn add_assignment_table(&mut self, address: &TreeAddress, id: PermanentTableId) {
        self.assignment.insert(address.clone(), id);
    }

    pub fn add_result_table(&mut self, address: &TreeAddress, id: PermanentTableId) {
        self.result.insert(address.clone(), id);
    }

    pub fn valid_table(&self, address: &TreeAddress) -> Option<PermanentTableId> {
        self.valid.get(address).cloned()
    }

    pub fn assignment_table(&self, address: &TreeAddress) -> Option<PermanentTableId> {
        self.assignment.get(address).cloned()
    }

    pub fn result_table(&self, address: &TreeAddress) -> Option<PermanentTableId> {
        self.result.get(address).cloned()
    }

    pub fn results(&self) -> impl Iterator<Item = PermanentTableId> {
        self.result.iter().map(|(_, id)| *id)
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

fn union_input(table_manager: &mut TableManager, predicate: &Tag) -> PermanentTableId {
    table_manager
        .combine_predicate(predicate)
        .expect("error in union input")
        .expect("none in union input")
}

fn node_join(
    plan: &mut ExecutionPlan,
    manager: &ProvenanceTableManager,
    address: &TreeAddress,
    variable_translation: &VariableTranslation,
    input_atoms: &[VariableAtom],
    output_markers: OperationTable,
) -> ExecutionNodeRef {
    let mut node_join = plan.join_empty(output_markers);

    for (index, atom) in input_atoms.iter().enumerate() {
        let mut atom_address = address.clone();
        atom_address.push(index);

        let id = manager.valid_table(&atom_address).expect("empty join");
        let atom_markers = variable_translation.operation_table(atom.terms());

        let node_fetch = plan.fetch_table(atom_markers, id);
        node_join.add_subnode(node_fetch);
    }

    node_join
}

fn body_plan(
    plan: &mut ExecutionPlan,
    manager: &ProvenanceTableManager,
    table_manager: &TableManager,
    address: &TreeAddress,
    rule: &ChaseRule,
    variable_order: &VariableOrder,
    variable_translation: &VariableTranslation,
) -> (ExecutionNodeRef, ExecutionId) {
    let join_output_markers = variable_translation.operation_table(variable_order.iter());
    let node_join = node_join(
        plan,
        manager,
        address,
        variable_translation,
        rule.positive_body(),
        join_output_markers,
    );

    let node_body_functions = node_functions(
        plan,
        variable_translation,
        node_join,
        rule.positive_operations(),
    );

    let node_body_filter = node_filter(
        plan,
        variable_translation,
        node_body_functions,
        rule.positive_filters(),
    );

    let node_negation = node_negation(
        plan,
        table_manager,
        variable_translation,
        node_body_filter,
        usize::MAX - 1,
        rule.negative_atoms(),
        rule.negative_filters(),
    );

    let id = plan.write_permanent(node_negation.clone(), "assignment", "assignment");
    (node_negation, id)
}

fn head_plan(
    plan: &mut ExecutionPlan,
    head_atom: &PrimitiveAtom,
    variable_translation: &VariableTranslation,
    base_node: ExecutionNodeRef,
) -> ExecutionId {
    let head_instruction = head_instruction_from_atom(head_atom);

    let node_result =
        node_head_instruction(plan, variable_translation, base_node, &head_instruction);

    plan.write_permanent(node_result, "head", "head")
}

fn valid_tables_plan(
    manager: &ProvenanceTableManager,
    table_manager: &TableManager,
    address: &TreeAddress,
    rule: &ChaseRule,
    variable_order: &VariableOrder,
    head_index: usize,
) -> (ExecutionPlan, ExecutionId, ExecutionId) {
    let mut plan = ExecutionPlan::default();

    let mut variable_translation = VariableTranslation::new();
    for variable in rule.variables().cloned() {
        variable_translation.add_marker(variable);
    }

    let (node_body, id_assignment) = body_plan(
        &mut plan,
        manager,
        table_manager,
        address,
        rule,
        variable_order,
        &variable_translation,
    );

    let head_atom = &rule.head()[head_index];
    let id_valid = head_plan(&mut plan, head_atom, &variable_translation, node_body);

    (plan, id_valid, id_assignment)
}

fn result_tables_plan(
    manager: &ProvenanceTableManager,
    address: &TreeAddress,
    variable_translation: &VariableTranslation,
    variable_order: &VariableOrder,
    head_variables: &[Variable],
    body_variables: &[Variable],
) -> Option<ExecutionPlan> {
    let mut parent_address = address.clone();
    parent_address.pop();

    let markers_head = variable_translation.operation_table(head_variables.iter());
    let markers_join = variable_translation.operation_table(variable_order.iter());
    let markers_body = variable_translation.operation_table(body_variables.iter());

    let id_head = manager.result_table(&parent_address)?;
    let id_join = manager.assignment_table(&parent_address)?;
    let id_body = manager.valid_table(address)?;

    let mut plan = ExecutionPlan::default();

    let node_head = plan.fetch_table(markers_head.clone(), id_head);
    let node_join = plan.fetch_table(markers_join.clone(), id_join);
    let node_body = plan.fetch_table(markers_body.clone(), id_body);

    let node_filter_results = plan.join(markers_join, vec![node_head, node_join, node_body]);

    let node_project = plan.projectreorder(markers_body, node_filter_results);
    plan.write_permanent(node_project, "filter", "filter");

    Some(plan)
}

impl<Strategy: RuleSelectionStrategy> ExecutionEngine<Strategy> {
    fn p_build_valid_nodes(
        &mut self,
        manager: &mut ProvenanceTableManager,
        node: &TableEntriesForTreeNodesQueryInner,
        address: TreeAddress,
        predicate: &Tag,
    ) {
        if let Some(successor) = &node.next {
            let rule = self.program.rules()[successor.rule].clone();
            let variable_order =
                self.analysis.rule_analysis[successor.rule].promising_variable_orders[0].clone();

            for (index, (atom, node_atom)) in rule
                .positive_body()
                .iter()
                .zip(successor.children.iter())
                .enumerate()
            {
                let mut next_address = address.clone();
                next_address.push(index);

                self.p_build_valid_nodes(manager, node_atom, next_address, &atom.predicate());
            }

            let (plan, id_valid, id_assignment) = valid_tables_plan(
                manager,
                &self.table_manager,
                &address,
                &rule,
                &variable_order,
                successor.head_index,
            );

            let execution_results = self
                .table_manager
                .database_mut()
                .execute_plan(plan)
                .expect("execute plan failed");

            if let Some(result_id) = execution_results.get(&id_valid) {
                manager.add_valid_table(&address, *result_id);
            }
            if let Some(result_id) = execution_results.get(&id_assignment) {
                manager.add_assignment_table(&address, *result_id);
            }
        } else {
            let id = union_input(&mut self.table_manager, predicate);
            manager.add_valid_table(&address, id);
        }
    }

    fn p_filter_nodes(
        &mut self,
        manager: &mut ProvenanceTableManager,
        node: &TableEntriesForTreeNodesQueryInner,
        address: TreeAddress,
    ) -> bool {
        if address.is_empty() {
            if let Some(root_table) = manager.valid_table(&address) {
                manager.add_result_table(&address, root_table);
            } else {
                println!("no root");
                return false;
            }
        }

        if let Some(successor) = &node.next {
            let rule = self.program.rules()[successor.rule].clone();
            let variable_order =
                self.analysis.rule_analysis[successor.rule].promising_variable_orders[0].clone();

            let (variable_translation, variable_order, head_variables) =
                variable_translation(&rule, successor.head_index, &variable_order);

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
                    &variable_order,
                    &head_variables,
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

                if !self.p_filter_nodes(manager, node_atom, next_address) {
                    return false;
                }
            }
        }

        true
    }

    pub(crate) fn execute_provenance_query(
        &mut self,
        query: &TableEntriesForTreeNodesQuery,
    ) -> ProvenanceTableManager {
        let mut manager = ProvenanceTableManager::default();
        let address = TreeAddress::default();
        let predicate = Tag::new(query.predicate.clone());
        let node = &query.inner;

        self.p_build_valid_nodes(&mut manager, node, address.clone(), &predicate);

        let _ = self.p_filter_nodes(&mut manager, node, address.clone());

        manager
    }

    pub(crate) fn node_query_answer_provenance(
        &mut self,
        manager: &ProvenanceTableManager,
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
