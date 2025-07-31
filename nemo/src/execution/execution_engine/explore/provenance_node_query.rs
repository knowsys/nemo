//! This module contains code for implementing provenance node queries.

use std::collections::{HashMap, HashSet};

use itertools::Itertools;
use nemo_physical::{
    datavalues::AnyDataValue,
    management::{
        database::id::{ExecutionId, PermanentTableId},
        execution_plan::{ExecutionNodeRef, ExecutionPlan},
    },
    tabular::{
        operations::{FunctionAssignment, OperationColumnMarker, OperationTable},
        trie::Trie,
    },
};

use crate::{
    chase_model::{
        ChaseAtom,
        analysis::variable_order::VariableOrder,
        components::{
            atom::{primitive_atom::PrimitiveAtom, variable_atom::VariableAtom},
            filter::ChaseFilter,
            operation::ChaseOperation,
            rule::ChaseRule,
            term::operation_term::{Operation, OperationTerm},
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
            tree_query::{TreeForTableResponse, TreeForTableResponseSuccessor},
        },
    },
    rule_model::components::{
        IterableVariables,
        tag::Tag,
        term::{
            operation::operation_kind::OperationKind,
            primitive::{Primitive, variable::Variable},
        },
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

fn union_predicate_rule(
    table_manager: &mut TableManager,
    predicate: &Tag,
    rule: usize,
    rules: &[usize],
) -> PermanentTableId {
    let markers = OperationTable::new_unique(table_manager.arity(predicate));

    let mut plan = ExecutionPlan::default();
    let mut node_union = plan.union_empty(markers.clone());

    for (_, id) in
        table_manager.tables_in_range_rule_steps(predicate, 0..(usize::MAX - 1), rules, rule)
    {
        let node_fetch = plan.fetch_table(markers.clone(), id);
        node_union.add_subnode(node_fetch);
    }

    plan.write_permanent(node_union, "union", "union");
    let (_, id) = table_manager
        .database_mut()
        .execute_plan(plan)
        .expect("error")
        .into_iter()
        .next()
        .expect("no tables");

    id
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

fn add_simplicity_restriction_rule(
    positive_filters: &mut Vec<ChaseFilter>,
    rule: &ChaseRule,
    head_index: usize,
) {
    let head_atom = &rule.head()[head_index];

    for body in rule.positive_body() {
        if body.predicate() != head_atom.predicate() {
            continue;
        }

        let mut disjunction_subterms = Vec::default();

        for (head_term, body_variable) in head_atom.terms().zip(body.terms()) {
            let same = match head_term {
                Primitive::Variable(variable) => variable == body_variable,
                Primitive::Ground(_) => false,
            };

            if !same {
                let operation_head = OperationTerm::Primitive(head_term.clone());
                let operation_body =
                    OperationTerm::Primitive(Primitive::Variable(body_variable.clone()));
                let unequal = OperationTerm::Operation(Operation::new(
                    OperationKind::Unequals,
                    vec![operation_head, operation_body],
                ));

                println!("add: {} != {}", head_term, body_variable);

                disjunction_subterms.push(unequal);
            }
        }

        positive_filters.push(ChaseFilter::new(OperationTerm::Operation(Operation::new(
            OperationKind::BooleanDisjunction,
            disjunction_subterms,
        ))));
    }
}

fn body_plan(
    plan: &mut ExecutionPlan,
    manager: &ProvenanceTableManager,
    table_manager: &TableManager,
    address: &TreeAddress,
    rule: &ChaseRule,
    variable_order: &VariableOrder,
    variable_translation: &VariableTranslation,
    head_index: usize,
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

    let mut filters = rule.positive_filters().clone();
    add_simplicity_restriction_rule(&mut filters, rule, head_index);

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

fn head_plan_existential(
    plan: &mut ExecutionPlan,
    table_manager: &mut TableManager,
    base_node: ExecutionNodeRef,
    rule_index: usize,
    rule: &ChaseRule,
    variable_order: &VariableOrder,
    head_index: usize,
    rule_history: &[usize],
) -> ExecutionId {
    println!("PLAN EXISTENTIAL");

    println!("rule: {}", rule);

    // let rule_head_variables = rule
    //     .head()
    //     .iter()
    //     .flat_map(|atom| atom.variables())
    //     .cloned()
    //     .collect::<HashSet<_>>();

    let predicate = rule.head()[head_index].predicate();

    let (variable_translation, full_variable_order, head_variables) =
        variable_translation(&rule, head_index, &variable_order);

    let body_variables = rule
        .positive_body()
        .iter()
        .flat_map(|atom| atom.variables())
        .cloned()
        .collect::<HashSet<_>>();
    let head_variable_set = head_variables.iter().cloned().collect::<HashSet<_>>();

    let frontier_variables = head_variable_set
        .intersection(&body_variables)
        .cloned()
        .collect::<HashSet<_>>();

    // println!("body vars: {}", body_variables.iter().join(","));
    // println!("head vars: {}", rule_head_variables.iter().join(","));
    // println!("frontier vars: {}", frontier_variables.iter().join(","));

    // let head_single_variables = head_variable_set.difference(&body_variables);
    let mut head_single_variables_mut = head_variable_set.difference(&body_variables);

    // println!("head_variable_set {}", head_variable_set.iter().join(","));
    // println!(
    //     "head_single_variables {}",
    //     head_single_variables_mut.join(",")
    // );

    let frontier_order = variable_order._restrict_to(&frontier_variables);
    let frontier_markers = variable_translation.operation_table(frontier_order.iter());

    println!("frontier_order: {}", frontier_order.iter().join(","));
    println!("frontier_markers: {:?}", frontier_markers);

    let node_frontier_projection = plan.projectreorder(frontier_markers, base_node);

    // let id_existential_predicate =
    //     union_predicate_rule(table_manager, &predicate, rule_index, rule_history);
    let id_existential_predicate = union_input(table_manager, &predicate);
    let markers_existential_predicate = variable_translation.operation_table(head_variables.iter());

    println!("markers existential: {:?}", markers_existential_predicate);

    let node_existential_predicate = plan.fetch_table(
        markers_existential_predicate.clone(),
        id_existential_predicate,
    );

    if frontier_variables.is_empty() {
        // let node_single = plan.single(node_existential_predicate, markers_existential_predicate);
        // println!("EARLY EXIT");
        // return plan.write_permanent(node_single, "existential", "existential");
        return plan.write_permanent(node_existential_predicate, "load existing", "load existing");
    }

    let join_variables = head_variable_set
        .union(&frontier_variables)
        .cloned()
        .collect::<HashSet<_>>();
    let join_order = full_variable_order._restrict_to(&join_variables);
    let markers_join = variable_translation.operation_table(join_order.iter());
    println!("markers_join: {:?}", markers_join);
    let node_join = plan.join(
        markers_join,
        vec![node_frontier_projection, node_existential_predicate],
    );

    // let single_markers = variable_translation.operation_table(head_single_variables);
    // println!("single markers: {:?}", single_markers);
    // let node_single = plan.single(node_join, single_markers);

    plan.write_permanent(node_join, "existential", "existential")
}

fn valid_tables_plan(
    manager: &ProvenanceTableManager,
    table_manager: &mut TableManager,
    address: &TreeAddress,
    rule: &ChaseRule,
    variable_order: &VariableOrder,
    head_index: usize,
    rule_index: usize,
    rule_history: &[usize],
    same_predicates: &[usize],
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
        head_index,
    );

    let head_atom = &rule.head()[head_index];
    let is_existential = head_atom
        .variables()
        .find(|variable| variable.is_existential())
        .is_some();
    let id_valid = if !is_existential {
        head_plan(&mut plan, head_atom, &variable_translation, node_body)
    } else {
        head_plan_existential(
            &mut plan,
            table_manager,
            node_body,
            rule_index,
            rule,
            variable_order,
            head_index,
            rule_history,
        )
    };

    (plan, id_valid, id_assignment)
}

fn result_tables_plan(
    manager: &ProvenanceTableManager,
    address: &TreeAddress,
    variable_translation: &VariableTranslation,
    variable_order: &VariableOrder,
    head_variables: &[Variable],
    body_variables: &[Variable],
    join_variables: &[Variable],
) -> Option<ExecutionPlan> {
    let mut parent_address = address.clone();
    parent_address.pop();

    let markers_head = variable_translation.operation_table(head_variables.iter());
    let markers_join = variable_translation.operation_table(join_variables.iter());
    let markers_body = variable_translation.operation_table(body_variables.iter());
    let markers_full = variable_translation.operation_table(variable_order.iter());

    let id_head = manager.result_table(&parent_address)?;
    let id_join = manager.assignment_table(&parent_address)?;
    let id_body = manager.valid_table(address)?;

    let mut plan = ExecutionPlan::default();

    let node_head = plan.fetch_table(markers_head, id_head);
    let node_join = plan.fetch_table(markers_join, id_join);
    let node_body = plan.fetch_table(markers_body.clone(), id_body);

    let node_filter_results = plan.join(markers_full, vec![node_head, node_join, node_body]);

    let node_project = plan.projectreorder(markers_body, node_filter_results);
    plan.write_permanent(node_project, "filter", "filter");

    Some(plan)
}

// fn compute_predicate_pairs_for_address(
//     pairs: &mut Vec<(TreeAddress, TreeAddress)>,
//     map: &HashMap<TreeAddress, Tag>,
//     address: &TreeAddress,
//     predicate: &Tag,
// ) {
//     for cut in (0..(address.len() - 1)).rev() {
//         let parent_address = address[0..cut].iter().cloned().collect::<Vec<_>>();
//         let parent_predicate = map.get(&parent_address).expect("must be filled");

//         if predicate == parent_predicate {
//             pairs.push((parent_address, address.clone()));
//         }
//     }
// }

// fn compute_predicate_on_each_node(
//     map: &mut HashMap<TreeAddress, Tag>,
//     pairs: &mut Vec<(TreeAddress, TreeAddress)>,
//     node: &TableEntriesForTreeNodesQueryInner,
//     rules: &Vec<ChaseRule>,
//     address: &TreeAddress,
//     predicate: &Tag,
// ) {
//     map.insert(address.clone(), predicate.clone());

//     if let Some(successor) = &node.next {
//         let rule = &rules[successor.rule];

//         for (index, (atom, node_atom)) in rule
//             .positive_body()
//             .iter()
//             .zip(successor.children.iter())
//             .enumerate()
//         {
//             let mut next_address = address.clone();
//             next_address.push(index);

//             compute_predicate_on_each_node(
//                 map,
//                 pairs,
//                 node_atom,
//                 rules,
//                 &next_address,
//                 &atom.predicate(),
//             );
//         }
//     }

//     compute_predicate_pairs_for_address(pairs, map, address, predicate)
// }

fn check_repeated_predicate(
    repeat_in_address: &mut Vec<TreeAddress>,
    search_predicate: &Tag,
    node: &TableEntriesForTreeNodesQueryInner,
    rules: &Vec<ChaseRule>,
    address: &TreeAddress,
    current_predicate: &Tag,
) {
    if search_predicate == current_predicate {
        repeat_in_address.push(address.clone());
    }

    if let Some(successor) = &node.next {
        let rule = &rules[successor.rule];

        for (index, (atom, node_atom)) in rule
            .positive_body()
            .iter()
            .zip(successor.children.iter())
            .enumerate()
        {
            let mut next_address = address.clone();
            next_address.push(index);

            check_repeated_predicate(
                repeat_in_address,
                search_predicate,
                node_atom,
                rules,
                &next_address,
                &atom.predicate(),
            );
        }
    }
}

fn compute_repeating_subtrees(
    repeated: &mut Vec<(Tag, TreeAddress, Vec<TreeAddress>)>,
    node: &TableEntriesForTreeNodesQueryInner,
    rules: &Vec<ChaseRule>,
    address: &TreeAddress,
    current_predicate: &Tag,
) {
    if let Some(successor) = &node.next {
        let rule = &rules[successor.rule];
        let head_predicate = rule.head()[successor.head_index].predicate();

        let mut repeat = Vec::<TreeAddress>::new();
        for (index, (atom, node_atom)) in rule
            .positive_body()
            .iter()
            .zip(successor.children.iter())
            .enumerate()
        {
            let mut next_address = address.clone();
            next_address.push(index);

            compute_repeating_subtrees(
                repeated,
                node_atom,
                rules,
                &next_address,
                &atom.predicate(),
            );

            let mut repeated_child = Vec::default();
            check_repeated_predicate(
                &mut repeated_child,
                &head_predicate,
                node_atom,
                rules,
                &next_address,
                &atom.predicate(),
            );
            repeat.extend(repeated_child);
        }

        if !repeat.is_empty() {
            repeated.push((current_predicate.clone(), address.clone(), repeat));
        }
    }
}

fn compute_check_trees(
    node: &TableEntriesForTreeNodesQuery,
    rules: &Vec<ChaseRule>,
) -> Vec<(Tag, TreeAddress, Vec<TreeAddress>)> {
    let mut repeated = Vec::default();
    let predicate = Tag::new(node.predicate.clone());
    let address = Vec::default();

    compute_repeating_subtrees(&mut repeated, &node.inner, rules, &address, &predicate);

    repeated
}

fn intersect_pairs(
    table_manager: &mut TableManager,
    manager: &ProvenanceTableManager,
    predicate: &Tag,
    parent: &TreeAddress,
    child: &TreeAddress,
) -> Option<Trie> {
    let mut plan = ExecutionPlan::default();

    println!("Addresses: {:?}, {:?}", parent, child);

    // if child.len() - parent.len() < 2 {
    //     return None;
    // }

    let arity = table_manager.arity(predicate);
    let markers = OperationTable::new_unique(arity);

    let id_parent = manager.result_table(parent).expect("result parent");
    let id_child = manager.result_table(child).expect("result child");

    println!(
        "lens: {}, {}",
        table_manager.database().count_rows_in_memory(id_parent),
        table_manager.database().count_rows_in_memory(id_child)
    );

    let node_parent = plan.fetch_table(markers.clone(), id_parent);
    let node_child = plan.fetch_table(markers.clone(), id_child);

    let node_join = plan.join(markers, vec![node_parent, node_child]);
    plan.write_permanent(node_join, "intersection", "intersection");

    table_manager
        .database_mut()
        .execute_plan_trie(plan)
        .expect("error")
        .pop()
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
            let head_predicate = rule.head()[successor.head_index].predicate();
            let variable_order =
                self.analysis.rule_analysis[successor.rule].promising_variable_orders[0].clone();
            let mut same_predicate = Vec::default();

            for (index, (atom, node_atom)) in rule
                .positive_body()
                .iter()
                .zip(successor.children.iter())
                .enumerate()
            {
                if atom.predicate() == head_predicate {
                    same_predicate.push(index);
                }

                let mut next_address = address.clone();
                next_address.push(index);

                self.p_build_valid_nodes(manager, node_atom, next_address, &atom.predicate());
            }

            let (plan, id_valid, id_assignment) = valid_tables_plan(
                manager,
                &mut self.table_manager,
                &address,
                &rule,
                &variable_order,
                successor.head_index,
                successor.rule,
                &self.rule_history,
                &same_predicate,
            );

            let execution_results = self
                .table_manager
                .database_mut()
                .execute_plan(plan)
                .expect("execute plan failed");

            if let Some(result_id) = execution_results.get(&id_valid) {
                // for same in same_predicate {
                //     let mut same_address = address.clone();
                //     same_address.push(same);

                //     let previous_id =
                // }

                if !same_predicate.is_empty() {
                    let count = self
                        .table_manager
                        .database()
                        .count_rows_in_memory(*result_id);
                    println!("COUNT: {count}");
                    std::process::exit(0);
                }

                manager.add_valid_table(&address, *result_id);
            } else {
                println!("failed ot build valid table: {:?}", address);
            }
            if let Some(result_id) = execution_results.get(&id_assignment) {
                manager.add_assignment_table(&address, *result_id);
            } else {
                println!("failed to build assignment table: {:?}", address)
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
            let join_variables = variable_order.iter().cloned().collect::<Vec<_>>();

            let (variable_translation, full_variable_order, head_variables) =
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
                    &full_variable_order,
                    &head_variables,
                    &atom_variables,
                    &join_variables,
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

    fn p_ensure_simplicity(
        &mut self,
        manager: &mut ProvenanceTableManager,
        query: &TableEntriesForTreeNodesQuery,
    ) {
        let repeated = compute_check_trees(query, self.chase_program().rules());

        for (predicate, parent, children) in repeated {
            for child in children {
                if let Some(trie_intersection) = intersect_pairs(
                    &mut self.table_manager,
                    manager,
                    &predicate,
                    &parent,
                    &child,
                ) {
                    println!("intersection: {}", trie_intersection.num_rows());
                } else {
                    println!("no intersection");
                }
            }
        }
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

        self.p_ensure_simplicity(&mut manager, query);

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
