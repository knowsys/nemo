//! Functionality that is useful for planing a rule application but is not specific to either part.

use std::{
    collections::{HashMap, HashSet},
    ops::Range,
};

use nemo_physical::{
    columnar::operations::condition::statement::ConditionStatement,
    datatypes::DataValueT,
    management::{
        database::{ColumnOrder, TableId},
        execution_plan::{ExecutionNodeRef, ExecutionPlan},
    },
    tabular::operations::triescan_append::AppendInstruction,
};

use crate::{
    model::{
        chase_model::{ChaseAtom, PrimitiveAtom, VariableAtom},
        Constraint, Identifier, PrimitiveTerm, PrimitiveType, Variable,
    },
    program_analysis::{analysis::RuleAnalysis, variable_order::VariableOrder},
    table_manager::TableManager,
};

use super::arithmetic::compile_termtree;

/// This function replaces each variable in the atom with its position in the variable ordering
///
/// Example:
///
/// * a(x, y, z) with atom order [0, 1, 2] and variable order [y, z, x] results in [1, 2, 0]
///
/// This function for computing JoinBindings:
///
/// * Example: For a leapfrog join a(x, y, z) b(z, y) with order [x, y, z] you'd obtain [[0, 1, 2], [2, 1]]
pub(super) fn atom_binding(atom: &VariableAtom, variable_order: &VariableOrder) -> Vec<usize> {
    atom.terms()
        .iter()
        .map(|v| {
            *variable_order
                .get(v)
                .expect("Function assumed that every variable is contained in the variable order")
        })
        .collect()
}

/// Calculate helper structures that define the filters that need to be applied.
pub(super) fn compute_constraints(
    variable_order: &VariableOrder,
    constraints: &[Constraint],
    variable_types: &HashMap<Variable, PrimitiveType>,
) -> Vec<ConditionStatement<DataValueT>> {
    let mut result = Vec::new();

    for constraint in constraints {
        let some_variable = constraint
            .variables()
            .next()
            .expect("A constraint should contain at least one variable");
        let variable_type = variable_types
            .get(some_variable)
            .expect("Every variable should have a type.");

        let (left, right) = constraint.terms();
        let left_tree = compile_termtree(left, variable_order, variable_type);
        let right_tree = compile_termtree(right, variable_order, variable_type);

        let new_statement = match constraint {
            Constraint::Equals(_, _) => ConditionStatement::equal(left_tree, right_tree),
            Constraint::Unequals(_, _) => ConditionStatement::unequal(left_tree, right_tree),
            Constraint::LessThan(_, _) => ConditionStatement::less_than(left_tree, right_tree),
            Constraint::GreaterThan(_, _) => {
                ConditionStatement::greater_than(left_tree, right_tree)
            }
            Constraint::LessThanEq(_, _) => {
                ConditionStatement::less_than_equal(left_tree, right_tree)
            }
            Constraint::GreaterThanEq(_, _) => {
                ConditionStatement::greater_than_equal(left_tree, right_tree)
            }
        };

        result.push(new_statement);
    }

    result
}

/// Compute the subplan that represents the union of a tables within a certain step range.
pub(super) fn subplan_union(
    plan: &mut ExecutionPlan,
    manager: &TableManager,
    predicate: Identifier,
    steps: &Range<usize>,
) -> ExecutionNodeRef {
    let base_tables: Vec<TableId> = manager.tables_in_range(predicate, steps);

    let mut union_node = plan.union_empty();
    for table_id in base_tables.into_iter() {
        let base_node = plan.fetch_existing(table_id);
        union_node.add_subnode(base_node);
    }

    union_node
}

/// Compute the subplan that represents the union of a tables within a certain step range.
pub(super) fn subplan_union_reordered(
    plan: &mut ExecutionPlan,
    manager: &TableManager,
    predicate: Identifier,
    steps: &Range<usize>,
    column_order: ColumnOrder,
) -> ExecutionNodeRef {
    let base_tables: Vec<TableId> = manager.tables_in_range(predicate, steps);

    let mut union_node = plan.union_empty();
    for table_id in base_tables.into_iter() {
        let base_node = plan.fetch_existing_reordered(table_id, column_order.clone());
        union_node.add_subnode(base_node);
    }

    union_node
}

/// Derived from head atoms which may contain duplicate (non-existential) variables or constants.
/// Represents a normal form which only contains non-duplicate universal variables and
/// the respective [`AppendInstruction`]s to obtain the intended result.
#[derive(Debug)]
pub(super) struct HeadInstruction {
    /// Reduced form of the the atom which only contains duplicate variables, constants or existential variables.
    pub reduced_atom: VariableAtom,
    /// The [`AppendInstruction`]s to get the original atom.
    pub append_instructions: Vec<Vec<AppendInstruction>>,
    /// The arity of the original atom.
    pub _arity: usize,
}

/// Given an atom, bring compute the corresponding [`HeadInstruction`].
/// TODO: This needs to be revised once the Type System on the logical layer has been implemented.
pub(super) fn head_instruction_from_atom(
    atom: &PrimitiveAtom,
    analysis: &RuleAnalysis,
) -> HeadInstruction {
    let arity = atom.terms().len();
    let mut reduced_terms = Vec::<Variable>::with_capacity(arity);
    let mut append_instructions = Vec::<Vec<AppendInstruction>>::new();

    append_instructions.push(vec![]);
    let mut current_append_vector = &mut append_instructions[0];

    let mut variable_map = HashMap::<Identifier, usize>::new();

    for (logical_type, term) in atom.terms().iter().enumerate().map(|(i, t)| {
        (
            analysis.predicate_types.get(&atom.predicate()).unwrap()[i],
            t,
        )
    }) {
        match term {
            PrimitiveTerm::Variable(variable) => {
                let variable_identifier = match variable {
                    Variable::Universal(id) => id,
                    Variable::Existential(id) => id,
                };

                if let Some(repeat_index) = variable_map.get(variable_identifier) {
                    let instruction = AppendInstruction::RepeatColumn(*repeat_index);
                    current_append_vector.push(instruction);
                } else {
                    let reduced_index = reduced_terms.len();
                    reduced_terms.push(variable.clone());

                    variable_map.insert(variable_identifier.clone(), reduced_index);

                    append_instructions.push(vec![]);
                    current_append_vector = append_instructions.last_mut().unwrap();
                }
            }
            PrimitiveTerm::Constant(constant) => {
                let data_value_t = logical_type.ground_term_to_data_value_t(constant.clone()).expect("Trying to convert a ground type into an invalid logical type. Should have been prevented by the type checker.");
                let instruction = AppendInstruction::Constant(data_value_t);
                current_append_vector.push(instruction);
            }
        }
    }

    let reduced_atom = VariableAtom::new(atom.predicate(), reduced_terms);

    HeadInstruction {
        reduced_atom,
        append_instructions,
        _arity: arity,
    }
}

pub(super) fn cut_last_layers(
    variable_order: &VariableOrder,
    used_variables: &HashSet<Variable>,
) -> (usize, usize) {
    if variable_order.is_empty() || used_variables.is_empty() {
        return (0, variable_order.len() - 1);
    }

    let mut last_index = 0;
    let variable_order_list = variable_order.as_ordered_list();

    for (index, variable) in variable_order_list.iter().enumerate() {
        if used_variables.contains(variable) {
            last_index = index;
        }
    }

    (last_index + 1, variable_order.len() - last_index - 1)
}
