//! Functionality that is useful for planing a rule application but is not specific to either part.

use std::collections::{HashMap, HashSet};

use crate::{
    logical::{
        model::{Atom, Filter, Term, Variable},
        program_analysis::variable_order::VariableOrder,
        types::LogicalTypeCollection,
        types::LogicalTypeEnum,
    },
    physical::{
        tabular::operations::{triescan_select::SelectEqualClasses, ValueAssignment},
        util::Reordering,
    },
};

/// Constant which refers to the body join table
pub const BODY_JOIN: usize = 0;

/// Calculates the [`ColumnOrder`] to transform the given [`Atom`]
/// such that in complies with the given [`VariableOrder`].
/// Say we have a(x, y, z) but variable_order = [x -> 1, y -> 2, z -> 0].
/// Then the result would be [2, 0, 1].
pub fn order_atom(atom: &Atom, variable_order: &VariableOrder) -> Reordering {
    let terms = atom.terms();
    let mut reordering = (0..terms.len()).collect::<Vec<usize>>();
    reordering.sort_by(|a, b| {
        let variable_a = if let Term::Variable(var) = &terms[*a] {
            var
        } else {
            // TODO: This shows why we need a separate encoding of rules for parsing and further processing
            panic!("Normalized rule should not contain any other terms then atoms");
        };

        let variable_b = if let Term::Variable(var) = &terms[*b] {
            var
        } else {
            panic!("Normalized rule should not contain any other terms then atoms");
        };

        let position_a = *variable_order
            .get(variable_a)
            .expect("VariableOrder should contain atom variables.");
        let position_b = *variable_order
            .get(variable_b)
            .expect("VariableOrder should contain atom variables.");

        position_a.cmp(&position_b)
    });

    Reordering::new(reordering, atom.terms().len())
}

/// Calculate the join binding for the given atom.
/// Essentially, it replaces each variable with the position in the variable ordering
/// while keeping in mind that the atom might be reordered.
/// Say you have the join a(x, y, z) b(y, z). For no reordering the binding would be [[0, 1, 2], [1, 2]].
/// If the variable order where [z, y, x] then we'd have [[0, 1, 2], [0, 1]].
pub fn join_binding(
    atom: &Atom,
    column_order: &Reordering,
    variable_order: &VariableOrder,
) -> Vec<usize> {
    // TODO: Currently this code assumes that no existential variable is part of the variable ordering
    // Reconsider this when adding support for existential rules.

    column_order
        .iter()
        .map(|&i| {
            if let Term::Variable(variable) = &atom.terms()[i] {
                *variable_order.get(variable).unwrap()
            } else {
                panic!("Only universal variables are supported.");
            }
        })
        .collect()
}

/// Calculate helper structures that define the filters that need to be applied.
/// TODO: Revise this when updating the type system.
pub fn filters<LogicalTypes: LogicalTypeCollection>(
    body_variables: &HashSet<Variable>,
    variable_order: &VariableOrder,
    filters: &[&Filter],
    variable_types: &HashMap<Variable, LogicalTypes>,
) -> (SelectEqualClasses, Vec<ValueAssignment>) {
    let mut body_variables_sorted: Vec<Variable> = body_variables.clone().into_iter().collect();
    body_variables_sorted.sort_by(|a, b| variable_order.get(a).cmp(&variable_order.get(b)));

    let mut body_variables_sorted = body_variables.iter().collect::<Vec<_>>();
    body_variables_sorted.sort_by(|a, b| {
        variable_order
            .get(a)
            .unwrap()
            .cmp(variable_order.get(b).unwrap())
    });
    let mut variable_to_columnindex = HashMap::new();
    for (index, variable) in body_variables_sorted.iter().enumerate() {
        variable_to_columnindex.insert(*variable, index);
    }

    let mut filter_assignments = Vec::<ValueAssignment>::new();
    let mut filter_classes = Vec::<HashSet<&Variable>>::new();
    for filter in filters {
        match &filter.right {
            Term::Variable(right_variable) => {
                let left_variable = &filter.left;

                let left_index = filter_classes
                    .iter()
                    .position(|s| s.contains(left_variable));
                let right_index = filter_classes
                    .iter()
                    .position(|s| s.contains(right_variable));

                match left_index {
                    Some(li) => match right_index {
                        Some(ri) => {
                            if ri > li {
                                let other_set = filter_classes.remove(ri);
                                filter_classes[li].extend(other_set);
                            } else {
                                let other_set = filter_classes.remove(li);
                                filter_classes[ri].extend(other_set);
                            }
                        }
                        None => {
                            filter_classes[li].insert(right_variable);
                        }
                    },
                    None => match right_index {
                        Some(ri) => {
                            filter_classes[ri].insert(right_variable);
                        }
                        None => {
                            let mut new_set = HashSet::new();
                            new_set.insert(left_variable);
                            new_set.insert(right_variable);

                            filter_classes.push(new_set);
                        }
                    },
                }
            }
            _ => {
                // TODO: proper error handling
                let datavalue = variable_types
                    .get(&filter.left)
                    .expect("We expect a type for every variable (it might be the default type).")
                    .convert_from_ground_term(&filter.right)
                    .expect("for now we just expect the convertion to work")
                    .as_data_value_t();
                filter_assignments.push(ValueAssignment {
                    column_idx: *variable_to_columnindex.get(&filter.left).unwrap(),
                    value: datavalue,
                });
            }
        }
    }
    let filter_classes: SelectEqualClasses = filter_classes
        .iter()
        .map(|s| {
            let mut r = s
                .iter()
                .map(|v| *variable_to_columnindex.get(v).unwrap())
                .collect::<Vec<usize>>();
            r.sort();
            r
        })
        .collect();

    (filter_classes, filter_assignments)
}
