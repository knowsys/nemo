//! Module defining the strategy for evaluating a conjunctive query.

use std::collections::HashMap;

use crate::{
    logical::{
        execution::planning::plan_util::{join_binding, subtree_union},
        model::{Atom, ConjunctiveQuery, Identifier, Literal, Program, Rule, Term},
        program_analysis::variable_order::build_preferable_variable_orders,
        table_manager::{ColumnOrder, TableKey},
        TableManager,
    },
    physical::{
        dictionary::Dictionary,
        management::{
            execution_plan::{ExecutionResult, ExecutionTree},
            ExecutionPlan,
        },
        util::Reordering,
    },
};

/// Transforms a given [`ConjunctiveQuery`] into a [`Program`] consisting of one datalog rule
/// that would evaluate the query.
fn cq_to_program<Dict: Dictionary>(query: &ConjunctiveQuery) -> Program<Dict> {
    let head_atom = Atom::new(
        Identifier(usize::MAX),
        query
            .answer_variables
            .iter()
            .map(|v| Term::Variable(*v))
            .collect(),
    );
    let body_literals = query
        .atoms
        .iter()
        .map(|a| Literal::Positive(a.clone()))
        .collect();

    let rule = Rule::new(vec![head_atom], body_literals, vec![]);

    Program::new(
        None,
        HashMap::new(),
        Vec::new(),
        vec![rule],
        Vec::new(),
        Dict::default(),
    )
}

/// Compute the [`ExecutionTree`] for the given [`ConjunctiveQuery`].
/// Returns the plan as well as the [`TableKey`] where the result will be stored.
pub fn compute_cq_plan<Dict: Dictionary>(
    query: ConjunctiveQuery,
    table_manager: &TableManager<Dict>,
    step_number: usize,
) -> (ExecutionPlan<TableKey>, TableKey) {
    // We will request every table in their default column order
    // TODO: Is there a better way?

    let mut plan = ExecutionPlan::<TableKey>::new();

    let query_program = cq_to_program::<Dict>(&query);
    let variable_order = &build_preferable_variable_orders(&query_program, None)[0][0];
    let join_bindings = query
        .atoms
        .iter()
        .map(|a| join_binding(a, &Reordering::default(a.terms().len()), variable_order))
        .collect();

    const TMP_JOIN: usize = 0;
    let mut tree_join =
        ExecutionTree::<TableKey>::new(String::from("CQ Join"), ExecutionResult::Temp(TMP_JOIN));

    let mut node_join = tree_join.join_empty(join_bindings);

    for atom in query.atoms {
        let node_union = subtree_union(
            &mut tree_join,
            table_manager,
            atom.predicate(),
            0..(step_number + 1),
            &Reordering::default(atom.terms().len()),
        );

        node_join.add_subnode(node_union);
    }

    tree_join.set_root(node_join);
    plan.push(tree_join);

    // TODO: BCQs are not implemented yet
    debug_assert!(!query.answer_variables.is_empty());

    // Saving it under the identifier "usize::MAX" seems a bit hacky.
    let result_name =
        table_manager.get_table_name(Identifier(usize::MAX), step_number..step_number + 1);
    let result_key = TableKey::from_name(
        result_name,
        ColumnOrder::default(query.answer_variables.len()),
    );

    let mut tree_project = ExecutionTree::<TableKey>::new(
        String::from("CQ Project"),
        ExecutionResult::Save(result_key.clone()),
    );

    let head_atom = &query_program.rules()[0].head()[0];
    let head_binding = join_binding(
        head_atom,
        &Reordering::default(head_atom.terms().len()),
        variable_order,
    );
    let reorder = Reordering::new(head_binding, variable_order.len());

    let node_fetch_tmp = tree_project.fetch_temp(TMP_JOIN);
    let node_project = tree_project.project(node_fetch_tmp, reorder);

    tree_project.set_root(node_project);
    plan.push(tree_project);

    (plan, result_key)
}
