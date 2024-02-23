//! This module contains a helper function for computing a node in an execution plan,
//! which realizes a filter operation.

use nemo_physical::{
    function::tree::FunctionTree,
    management::execution_plan::{ExecutionNodeRef, ExecutionPlan},
    tabular::operations::{Filters, OperationColumnMarker},
};

use crate::{execution::rule_execution::VariableTranslation, model::Constraint};

use super::term::term_to_function_tree;

fn constraint_to_tree(
    translation: &VariableTranslation,
    constraint: &Constraint,
) -> FunctionTree<OperationColumnMarker> {
    let (left_term, right_term) = constraint.terms();
    let left = term_to_function_tree(translation, left_term);
    let right = term_to_function_tree(translation, right_term);

    match constraint {
        Constraint::Equals(_, _) => FunctionTree::equals(left, right),
        Constraint::Unequals(_, _) => FunctionTree::unequals(left, right),
        Constraint::LessThan(_, _) => FunctionTree::numeric_lessthan(left, right),
        Constraint::GreaterThan(_, _) => FunctionTree::numeric_greaterthan(left, right),
        Constraint::LessThanEq(_, _) => FunctionTree::numeric_lessthaneq(left, right),
        Constraint::GreaterThanEq(_, _) => FunctionTree::numeric_greaterthaneq(left, right),
    }
}

/// Calculate helper structures that define the filters that need to be applied.
pub(crate) fn node_filter(
    plan: &mut ExecutionPlan,
    variable_translation: &VariableTranslation,
    subnode: ExecutionNodeRef,
    constraints: &[Constraint],
) -> ExecutionNodeRef {
    let filters = constraints
        .iter()
        .map(|constraint| constraint_to_tree(variable_translation, constraint))
        .collect::<Filters>();

    plan.filter(subnode, filters)
}
