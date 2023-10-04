//! Functionality that is useful for planing a rule application but is not specific to either part.

use std::{
    collections::{HashMap, HashSet},
    ops::Range,
};

use nemo_physical::{
    datatypes::DataValueT,
    management::{
        database::{ColumnOrder, TableId},
        execution_plan::{ExecutionNodeRef, ExecutionPlan},
    },
    tabular::operations::{
        triescan_append::AppendInstruction,
        triescan_select::{SelectEqualClasses, ValueAssignment},
    },
};

use crate::{
    model::{
        chase_model::ChaseAtom, Filter, FilterOperation, Identifier, PrimitiveType, Term, Variable,
    },
    program_analysis::{analysis::RuleAnalysis, variable_order::VariableOrder},
    table_manager::TableManager,
};

/// This function replaces each variable in the atom with its position in the variable ordering
///
/// Example:
///
/// * a(x, y, z) with atom order [0, 1, 2] and variable order [y, z, x] results in [1, 2, 0]
///
/// This function for computing JoinBindings:
///
/// * Example: For a leapfrog join a(x, y, z) b(z, y) with order [x, y, z] you'd obtain [[0, 1, 2], [2, 1]]
pub(super) fn atom_binding(atom: &ChaseAtom, variable_order: &VariableOrder) -> Vec<usize> {
    atom.terms().iter().map(|t| if let Term::Variable(variable) = t {
        *variable_order.get(variable).unwrap()
    } else {
        panic!("It is assumed that this function is only called on atoms which only contain variables.");
    }).collect()
}

/// Calculate helper structures that define the filters that need to be applied.
pub(super) fn compute_filters(
    variable_order: &VariableOrder,
    filters: &[Filter],
    variable_types: &HashMap<Variable, PrimitiveType>,
) -> (SelectEqualClasses, HashMap<usize, ValueAssignment>) {
    let mut filter_assignments = HashMap::<usize, ValueAssignment>::new();
    let mut filter_classes = Vec::<HashSet<&Variable>>::new();
    for filter in filters {
        let left_variable = &filter.lhs;
        if !variable_order.contains(left_variable) {
            continue;
        }

        // Case 1: Variables equals another variable
        // In this case, we need to update the `filter_classes`
        if filter.operation == FilterOperation::Equals {
            if let Term::Variable(right_variable) = &filter.rhs {
                if !variable_order.contains(right_variable) {
                    continue;
                }

                let left_index = filter_classes
                    .iter()
                    .position(|s| s.contains(left_variable));
                let right_index = filter_classes
                    .iter()
                    .position(|s| s.contains(right_variable));

                match left_index {
                    Some(li) => match right_index {
                        Some(ri) => match ri.cmp(&li) {
                            std::cmp::Ordering::Less => {
                                let other_set = filter_classes[li].clone();
                                filter_classes[ri].extend(other_set);

                                filter_classes.remove(li);
                            }
                            std::cmp::Ordering::Equal => todo!(),
                            std::cmp::Ordering::Greater => {
                                let other_set = filter_classes[ri].clone();
                                filter_classes[li].extend(other_set);

                                filter_classes.remove(ri);
                            }
                        },
                        None => {
                            filter_classes[li].insert(right_variable);
                        }
                    },
                    None => match right_index {
                        Some(ri) => {
                            filter_classes[ri].insert(left_variable);
                        }
                        None => {
                            let mut new_set = HashSet::new();
                            new_set.insert(left_variable);
                            new_set.insert(right_variable);

                            filter_classes.push(new_set);
                        }
                    },
                }

                continue;
            }
        }

        // Case 2: Variable is compared to another variable or with a constant
        // In this case, we need to update the `filter_assignments`

        match &filter.rhs {
            Term::Variable(right_variable) => {
                if !variable_order.contains(right_variable) {
                    continue;
                }

                let column_idx_left = *variable_order
                    .get(left_variable)
                    .expect("Loop iteration is skipped for unknown variables.");
                let column_idx_right = *variable_order
                    .get(right_variable)
                    .expect("Loop iteration is skipped for unknown variables.");

                let (column_idx_value, column_idx_bound, operation) =
                    if column_idx_left > column_idx_right {
                        (column_idx_left, column_idx_right, filter.operation)
                    } else {
                        (column_idx_right, column_idx_left, filter.operation.flip())
                    };

                let current_assignment = filter_assignments.entry(column_idx_value).or_default();

                // add_restriction(
                //     &operation,
                //     FilterValue::Column(column_idx_bound),
                //     &mut current_assignment.lower_bounds,
                //     &mut current_assignment.upper_bounds,
                //     &mut current_assignment.avoid_values,
                // );
            }
            _ => {
                let column_idx_value = *variable_order
                    .get(&filter.lhs)
                    .expect("Loop iteration is skipped for unknown variables.");
                let right_value = variable_types
                    .get(&filter.lhs)
                    .expect("Each variable should have been assigned a type.")
                    .ground_term_to_data_value_t(filter.rhs.clone()).expect("Trying to convert a ground type into an invalid logical type. Should have been prevented by the type checker.");

                let current_assignment = filter_assignments.entry(column_idx_value).or_default();

                // add_restriction(
                //     &filter.operation,
                //     FilterValue::Constant(right_value),
                //     &mut current_assignment.lower_bounds,
                //     &mut current_assignment.upper_bounds,
                //     &mut current_assignment.avoid_values,
                // );
            }
        }
    }

    let filter_classes: SelectEqualClasses = filter_classes
        .iter()
        .map(|s| {
            let mut r = s
                .iter()
                .map(|v| *variable_order.get(v).expect("The above loop skips iterations if one of the filter variables is not contained in the variable order."))
                .collect::<Vec<usize>>();
            r.sort();
            r
        })
        .collect();

    (filter_classes, filter_assignments)
}

// fn add_restriction(
//     operation: &FilterOperation,
//     value: FilterValue<DataValueT>,
//     lower_bounds: &mut Vec<FilterBound<DataValueT>>,
//     upper_bounds: &mut Vec<FilterBound<DataValueT>>,
//     avoid_values: &mut Vec<FilterValue<DataValueT>>,
// ) {
//     match operation {
//         FilterOperation::Equals => {
//             lower_bounds.push(FilterBound::Inclusive(value.clone()));
//             upper_bounds.push(FilterBound::Inclusive(value))
//         }
//         FilterOperation::LessThan => upper_bounds.push(FilterBound::Exclusive(value)),
//         FilterOperation::GreaterThan => lower_bounds.push(FilterBound::Exclusive(value)),
//         FilterOperation::LessThanEq => upper_bounds.push(FilterBound::Inclusive(value)),
//         FilterOperation::GreaterThanEq => lower_bounds.push(FilterBound::Inclusive(value)),
//         FilterOperation::Unequals => avoid_values.push(value),
//     }
// }

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
    pub reduced_atom: ChaseAtom,
    /// The [`AppendInstruction`]s to get the original atom.
    pub append_instructions: Vec<Vec<AppendInstruction>>,
    /// The arity of the original atom.
    pub _arity: usize,
}

/// Given an atom, bring compute the corresponding [`HeadInstruction`].
/// TODO: This needs to be revised once the Type System on the logical layer has been implemented.
pub(super) fn head_instruction_from_atom(
    atom: &ChaseAtom,
    analysis: &RuleAnalysis,
) -> HeadInstruction {
    let arity = atom.terms().len();
    let mut reduced_terms = Vec::<Term>::with_capacity(arity);
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
        if let Term::Variable(variable) = term {
            let variable_identifier = match variable {
                Variable::Universal(id) => id,
                Variable::Existential(id) => id,
            };

            if let Some(repeat_index) = variable_map.get(variable_identifier) {
                let instruction = AppendInstruction::RepeatColumn(*repeat_index);
                current_append_vector.push(instruction);
            } else {
                let reduced_index = reduced_terms.len();
                reduced_terms.push(Term::Variable(variable.clone()));

                variable_map.insert(variable_identifier.clone(), reduced_index);

                append_instructions.push(vec![]);
                current_append_vector = append_instructions.last_mut().unwrap();
            }
        } else if let Term::Aggregate(_aggregate) = term {
            panic!("Aggregate terms in the head should have already been replaced by placeholder variables in the chase rule creation and are thus not supported here")
        } else {
            let data_value_t = logical_type.ground_term_to_data_value_t(term.clone()).expect("Trying to convert a ground type into an invalid logical type. Should have been prevented by the type checker.");
            let instruction = AppendInstruction::Constant(data_value_t);
            current_append_vector.push(instruction);
        }
    }

    let reduced_atom = ChaseAtom::new(atom.predicate(), reduced_terms);

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
