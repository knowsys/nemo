//! This module implementing tracing for node queries.

use std::collections::HashSet;

use nemo_physical::{
    datavalues::AnyDataValue,
    management::{database::sources::SimpleTable, execution_plan::ColumnOrder},
};

use crate::{
    execution::{
        ExecutionEngine,
        execution_engine::tracing::node_query::{
            manager::TraceNodeManager,
            util::{
                consolidate_assignment_tables, consolidate_valid_tables,
                ignore_discarded_columns_base, join_query_node, result_tables_plan,
                unique_variables, valid_tables_plan, variable_translation,
            },
        },
        planning::normalization::{atom::ground::GroundAtom, program::NormalizedProgram},
        selection_strategy::strategy::RuleSelectionStrategy,
        tracing::{
            node_query::{
                TableEntriesForTreeNodesQuery, TableEntriesForTreeNodesQueryInner,
                TableEntriesForTreeNodesResponse, TableEntriesForTreeNodesResponseElement,
                TreeAddress,
            },
            shared::{PaginationResponse, Rule as TraceRule, TableEntryQuery, TableEntryResponse},
        },
    },
    rule_model::components::{atom::Atom, tag::Tag},
};

mod manager;
mod util;

impl<Strategy: RuleSelectionStrategy> ExecutionEngine<Strategy> {
    /// Phase 1 of `trace_node_execute`
    ///
    /// Collect the fact restriction in each node into tables.
    async fn trace_node_restriction(
        &mut self,
        manager: &mut TraceNodeManager,
        node: &TableEntriesForTreeNodesQueryInner,
        address: TreeAddress,
        predicate: &Tag,
        program: &NormalizedProgram,
    ) {
        if !node.queries.is_empty() {
            let arity = self.predicate_arity(predicate).expect("invalid predicate");

            // We collect facts that are part of the query
            // in a light-weight table

            let mut simple_table = SimpleTable::new(arity);

            for query in &node.queries {
                let terms = match query {
                    TableEntryQuery::Entry(row_index) => {
                        let terms_to_trace: Vec<AnyDataValue> = self
                            .predicate_rows(predicate)
                            .await
                            .expect("unknown predicate")
                            .into_iter()
                            .flatten()
                            .nth(*row_index)
                            .expect("invalid id");

                        terms_to_trace
                    }
                    TableEntryQuery::Query(query_string) => {
                        let atom = Atom::parse(&format!("P({query_string})"))
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
                .register_table("_TRACE_NODE_QUERY_IMPORT", arity);
            self.table_manager.database_mut().add_source_table(
                id,
                ColumnOrder::default(),
                simple_table,
            );

            manager.add_query_table(&address, id);
        }

        // Recursive call to this function for all successor nodes

        if let Some(successor) = &node.next {
            let rule = program.rules()[successor.rule].clone();

            for (index, (atom, node_atom)) in rule
                .positive_all()
                .iter()
                .zip(successor.children.iter())
                .enumerate()
            {
                let mut next_address = address.clone();
                next_address.push(index);

                Box::pin(self.trace_node_restriction(
                    manager,
                    node_atom,
                    next_address,
                    &atom.predicate(),
                    program,
                ))
                .await;
            }
        }
    }

    /// Phase 2 of `trace_node_execute`
    ///
    /// Bottom-up computation of "valid" facts per query node,
    /// meaning that every such fact satisfies all constraints
    /// imposed by its subtree.
    #[allow(clippy::too_many_arguments)]
    async fn trace_node_valid(
        &mut self,
        manager: &mut TraceNodeManager,
        node: &TableEntriesForTreeNodesQueryInner,
        address: TreeAddress,
        predicate: &Tag,
        discarded_columns: &[usize],
        before_step: usize,
        program: &NormalizedProgram,
    ) {
        manager.add_discard(&address, discarded_columns);

        if let Some(successor) = &node.next {
            // True if all children do not have any restrictions
            let simple_successor = successor.children.iter().all(|child| child.is_simple());

            let rule = program.rules()[successor.rule].clone();
            let order = rule.variable_order().clone();

            // Any facts derived after this point are not relevant for this node
            let next_step = {
                self.rule_history[..before_step]
                    .iter()
                    .rposition(|rule| *rule == successor.rule)
                    .unwrap_or(self.rule_history.len())
            };

            let unique_variables = unique_variables(&rule);

            for (index, (atom, node_atom)) in rule
                .positive_all()
                .iter()
                .zip(successor.children.iter())
                .enumerate()
            {
                let mut next_address = address.clone();
                next_address.push(index);

                // We track unused variables because the values of
                // the corresponding columns are simply projected away and provide no restriction
                let mut discarded_columns = Vec::default();
                for (term_index, variable) in atom.terms().enumerate() {
                    if unique_variables.contains(variable) {
                        discarded_columns.push(term_index);
                    }
                }

                // Recursive call to this function
                Box::pin(self.trace_node_valid(
                    manager,
                    node_atom,
                    next_address,
                    &atom.predicate(),
                    &discarded_columns,
                    next_step,
                    program,
                ))
                .await;
            }

            for (step, id) in self.table_manager.tables_in_range_rule_steps(
                predicate,
                0..before_step,
                &self.rule_history,
                successor.rule,
            ) {
                // We iterate over all tables of the current predicate
                // that was derived within the step limit
                // from the rule annotated on its incoming edge

                // We then calculate the subset of facts,
                // which adheres to all restrictions placed on this node by its subtree,
                // stored in the table with id `id_valid`.

                // Further, we remember the variable assignments that lead to each fact,
                // and store it in a separate table,
                // store in the table with id `id_assignment`

                let (plan, id_valid, id_assignment) = valid_tables_plan(
                    manager,
                    &self.table_manager,
                    &address,
                    &rule,
                    successor.head_index,
                    discarded_columns,
                    &order,
                    id,
                    step,
                    !simple_successor,
                );

                let execution_results = self
                    .table_manager
                    .database_mut()
                    .execute_plan(plan)
                    .await
                    .expect("execute plan failed");

                if let Some(id_valid) = id_valid {
                    if let Some(result_id) = execution_results.get(&id_valid) {
                        manager.add_valid_table(&address, step, *result_id);
                    }
                } else {
                    manager.add_valid_table(&address, step, id);
                }

                if let Some(result_id) = execution_results.get(&id_assignment) {
                    manager.add_assignment_table(&address, step, *result_id);
                }
            }

            // Valid facts for each step are combined into a single table

            if let Some(id) =
                consolidate_valid_tables(self.table_manager.database_mut(), manager, &address).await
            {
                manager.add_final_valid_table(&address, id);
            }

            if let Some(id) =
                consolidate_assignment_tables(self.table_manager.database_mut(), manager, &address)
                    .await
            {
                manager.add_final_assignment_table(&address, id);
            }
        } else {
            for (step, id) in self
                .table_manager
                .tables_in_range_steps(predicate, 0..before_step)
            {
                let query_table = manager.query_table(&address);
                let arity = self.table_manager.arity(predicate);

                if discarded_columns.is_empty() {
                    // If all columns are required, we simply add the table

                    if let Some(id) =
                        join_query_node(self.table_manager.database_mut(), arity, id, query_table)
                            .await
                    {
                        manager.add_valid_table(&address, step, id);
                    }
                } else {
                    // Otherwise, project the discarded columns away

                    if let Some(ignored_id) = ignore_discarded_columns_base(
                        self.table_manager.database_mut(),
                        id,
                        arity,
                        discarded_columns,
                        query_table,
                    )
                    .await
                    {
                        manager.add_valid_table(&address, step, ignored_id);
                    } else {
                        manager.add_valid_table(&address, step, id);
                    }
                }
            }

            if let Some(id) =
                consolidate_valid_tables(self.table_manager.database_mut(), manager, &address).await
            {
                manager.add_final_valid_table(&address, id);
            }
        }
    }

    /// Phase 3 of `trace_node_execute`
    ///
    /// Delete facts that were derived in the bottom-up phase,
    /// thereby pushing constraints from the sibling and parent nodes.
    ///
    /// Return whether the result contains elements.
    async fn trace_node_filter(
        &mut self,
        manager: &mut TraceNodeManager,
        node: &TableEntriesForTreeNodesQueryInner,
        address: TreeAddress,
        program: &NormalizedProgram,
    ) -> bool {
        if address.is_empty() {
            if let Some(root_table) = manager.final_valid_table(&address) {
                manager.add_result_table(&address, root_table);
            } else {
                return false;
            }
        }

        let discarded_columns = manager.discard(&address);

        if let Some(successor) = &node.next {
            let rule = program.rules()[successor.rule].clone();

            let order = rule.variable_order().clone();

            let (variable_translation, order, head_variables) =
                variable_translation(&rule, successor.head_index, &order);

            let body_set = rule.variables_non_head().cloned().collect::<HashSet<_>>();
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

            // In case the head is completely disjoint from the body,
            // there are no restrictions and hence no filtering is required.
            let disjoint = head_set.is_disjoint(&body_set);

            for (index, (atom, node_atom)) in rule
                .positive_all()
                .iter()
                .zip(successor.children.iter())
                .enumerate()
            {
                let mut next_address = address.clone();
                next_address.push(index);

                let atom_variables = atom.terms().cloned().collect::<Vec<_>>();

                if !disjoint {
                    if let Some(plan) = result_tables_plan(
                        manager,
                        &next_address,
                        &variable_translation,
                        &head_variables,
                        &order,
                        &atom_variables,
                    ) {
                        let Ok(results) =
                            self.table_manager.database_mut().execute_plan(plan).await
                        else {
                            return false;
                        };

                        let Some((_, result_id)) = results.iter().next() else {
                            return false;
                        };

                        manager.add_result_table(&next_address, *result_id);
                    } else {
                        return false;
                    }
                } else if let Some(result_id) = manager.final_valid_table(&next_address) {
                    manager.add_result_table(&next_address, result_id);
                } else {
                    return false;
                }

                if !Box::pin(self.trace_node_filter(manager, node_atom, next_address, program))
                    .await
                {
                    return false;
                }
            }
        }

        true
    }

    /// Compute the results for a [TableEntriesForTreeNodesQuery].
    async fn trace_node_execute(
        &mut self,
        query: &TableEntriesForTreeNodesQuery,
        program: &NormalizedProgram,
    ) -> TraceNodeManager {
        let mut manager = TraceNodeManager::default();
        let address = TreeAddress::default();
        let predicate = Tag::new(query.predicate.clone());
        let node = &query.inner;
        let before_step = self.rule_history.len();
        let discarded_columns = Vec::default();

        self.trace_node_restriction(&mut manager, node, address.clone(), &predicate, program)
            .await;

        self.trace_node_valid(
            &mut manager,
            node,
            address.clone(),
            &predicate,
            &discarded_columns,
            before_step,
            program,
        )
        .await;

        let _ = self
            .trace_node_filter(&mut manager, node, address.clone(), program)
            .await;

        manager
    }

    /// Collect the answer to the node query using [TreeTableManager]
    /// and write it into the prepared [TableEntriesForTreeNodesResponse]
    async fn trace_node_answer(
        &mut self,
        manager: &TraceNodeManager,
        mut response: TableEntriesForTreeNodesResponse,
    ) -> Option<TableEntriesForTreeNodesResponse> {
        for element in &mut response.elements {
            let Some(table_id) = manager.result_table(&element.address) else {
                continue;
            };

            // To realize pagination, we simply compute all results
            // and only return the requested subset
            //
            // TODO: Find a method to improve upon this

            let rows_iter = self
                .table_manager
                .database_mut()
                .table_row_iterator(table_id)
                .await
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
                    .await
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

    /// From a given [TableEntriesForTreeNodesQueryInner]
    /// precompute the structure of the [TableEntriesForTreeNodesResponse],
    /// leaving empty the fields for the actual response.
    async fn trace_node_prepare_recursive(
        &mut self,
        elements: &mut Vec<TableEntriesForTreeNodesResponseElement>,
        node: &TableEntriesForTreeNodesQueryInner,
        address: TreeAddress,
        predicate: &Tag,
        program: &NormalizedProgram,
    ) {
        // Collect all (syntactly) possible rules
        // that could be triggered by or could trigger
        // the predicate assigned to the current node

        let possible_rules_above = program
            .rules_with_body_predicate(predicate)
            .flat_map(|index| {
                TraceRule::all_possible_single_head_rules(index, self.program().rule(index))
            })
            .collect::<Vec<_>>();

        let possible_rules_below = program
            .rules_with_head_predicate(predicate)
            .flat_map(|index| {
                TraceRule::possible_rules_for_head_predicate(
                    index,
                    self.program().rule(index),
                    predicate,
                )
            })
            .collect::<Vec<_>>();

        // Prepare the response element,
        // preallocating the space needed for the result entries
        // based on the pagination information (if given)

        let element = TableEntriesForTreeNodesResponseElement {
            predicate: predicate.to_string(),
            entries: Vec::with_capacity(
                node.pagination
                    .map(|pagination| pagination.count)
                    .unwrap_or_default(),
            ),
            pagination: PaginationResponse {
                start: node
                    .pagination
                    .map(|pagination| pagination.start)
                    .unwrap_or_default(),
                more: false,
            },
            possible_rules_above,
            possible_rules_below,
            address: address.clone(),
        };

        elements.push(element);

        if let Some(successor) = &node.next {
            let rule = program.rules()[successor.rule].clone();

            // Call this function recursively for each successor

            for (index, (child, atom)) in successor
                .children
                .iter()
                .zip(rule.positive_all())
                .enumerate()
            {
                let next_predicate = atom.predicate();

                let mut next_address = address.clone();
                next_address.push(index);

                Box::pin(self.trace_node_prepare_recursive(
                    elements,
                    child,
                    next_address,
                    &next_predicate,
                    program,
                ))
                .await;
            }

            // The content of the negated nodes is just the complete table
            // corresponding to the negated atoms predicate:

            for (index, negative_atom) in rule.negative().iter().enumerate() {
                let mut next_address = address.clone();
                next_address.push(rule.positive_all().len() + index);

                let rows = if let Some(rows) = self
                    .predicate_rows(&negative_atom.predicate())
                    .await
                    .expect("collect negation rows failed")
                {
                    rows.collect::<Vec<_>>()
                } else {
                    Vec::default()
                };

                let mut entries = Vec::default();

                for row in rows {
                    let entry_id = self
                        .table_manager
                        .table_row_id(&negative_atom.predicate(), &row)
                        .await
                        .expect("row should be contained somewhere");

                    let table_response = TableEntryResponse {
                        entry_id,
                        terms: row,
                    };

                    entries.push(table_response);
                }

                let negation_element = TableEntriesForTreeNodesResponseElement {
                    predicate: negative_atom.predicate().to_string(),
                    entries,
                    pagination: PaginationResponse {
                        start: 0,
                        more: false,
                    },
                    possible_rules_above: Vec::default(),
                    possible_rules_below: Vec::default(),
                    address: next_address,
                };

                elements.push(negation_element);
            }
        }
    }

    /// For a given [TableEntriesForTreeNodesQuery]
    /// precompute the structure of the [TableEntriesForTreeNodesResponse],
    /// leaving empty the fields for the actual response.
    async fn trace_node_prepare(
        &mut self,
        query: &TableEntriesForTreeNodesQuery,
        program: &NormalizedProgram,
    ) -> TableEntriesForTreeNodesResponse {
        let mut elements = Vec::<TableEntriesForTreeNodesResponseElement>::default();

        self.trace_node_prepare_recursive(
            &mut elements,
            &query.inner,
            Vec::default(),
            &Tag::new(query.predicate.clone()),
            program,
        )
        .await;

        TableEntriesForTreeNodesResponse { elements }
    }

    /// Evaluate a [TableEntriesForTreeNodesQuery].
    pub async fn trace_node(
        &mut self,
        query: &TableEntriesForTreeNodesQuery,
    ) -> TableEntriesForTreeNodesResponse {
        let program = self.program.clone();

        let response = self.trace_node_prepare(query, &program).await;
        let manager = self.trace_node_execute(query, &program).await;

        self.trace_node_answer(&manager, response)
            .await
            .unwrap_or_default()
    }
}
