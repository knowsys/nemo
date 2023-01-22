//! Functionality that is useful for planing a rule application but is not specific to either part.

use std::{
    collections::{HashMap, HashSet},
    ops::Range,
};

use crate::{
    logical::{
        model::{Atom, Filter, Identifier, NumericLiteral, Term, Variable},
        program_analysis::variable_order::VariableOrder,
        table_manager::TableKey,
        TableManager,
    },
    physical::{
        datatypes::DataValueT,
        dictionary::Dictionary,
        management::execution_plan::{ExecutionNodeRef, ExecutionTree},
        tabular::operations::{
            triescan_append::AppendInstruction, triescan_select::SelectEqualClasses,
            ValueAssignment,
        },
        util::Reordering,
    },
};

/// Constant which refers to the body join table
pub(super) const BODY_JOIN: usize = 0;

/// Calculates the [`ColumnOrder`] to transform the given [`Atom`]
/// such that in complies with the given [`VariableOrder`].
/// Say we have a(x, y, z) but variable_order = [x -> 1, y -> 2, z -> 0].
/// Then the result would be [2, 0, 1].
pub(super) fn order_atom(atom: &Atom, variable_order: &VariableOrder) -> Reordering {
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
pub(super) fn join_binding(
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
pub(super) fn compute_filters(
    body_variables: &HashSet<Variable>,
    variable_order: &VariableOrder,
    filters: &[&Filter],
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
                if let Some(datavalue) = filter.right.to_datavalue_t() {
                    filter_assignments.push(ValueAssignment {
                        column_idx: *variable_to_columnindex.get(&filter.left).unwrap(),
                        value: datavalue,
                    });
                } else {
                    // TODO: Not sure what to do in this case
                }
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

/// Compute the subtree that represents the union of a tables within a certain step range.
pub(super) fn subtree_union<Dict: Dictionary>(
    tree: &mut ExecutionTree<TableKey>,
    manager: &TableManager<Dict>,
    predicate: Identifier,
    steps: Range<usize>,
    order: &Reordering,
) -> ExecutionNodeRef<TableKey> {
    debug_assert!(order.is_permutation());

    let base_tables: Vec<TableKey> = manager
        .get_table_covering(predicate, steps)
        .into_iter()
        .map(|r| TableKey::new(predicate, r, order.clone().into()))
        .collect();

    let mut union_node = tree.union_empty();
    for key in base_tables {
        let base_node = tree.fetch_table(key);
        union_node.add_subnode(base_node);
    }

    union_node
}

// TODO: Was ist mit existential variablen????
// Vermutung: Existential müssen einfach genau so wie Universal behandelt werden
// Append(null) muss auf dem

/// Derived from head atoms which may contain duplicate (non-existential) variables or constants.
/// Represents a normal form which only contains non-duplicate universal variables and
/// the respective [`AppendInstruction`]s to obtain the intended result.
#[derive(Debug)]
pub(super) struct HeadInstruction {
    /// Reduced form of the the atom which only contains duplicate variables, constants or existential variables.
    pub reduced_atom: Atom,
    /// The [`AppendInstruction`]s to get the original atom.
    pub append_instructions: Vec<Vec<AppendInstruction>>,
    /// The arity of the original atom.
    pub arity: usize,
}

/// Given an atom, bring compute the corresponding [`HeadInstruction`].
/// TODO: This needs to be revised once the Type System on the logical layer has been implemented.
pub(super) fn head_instruction_from_atom(atom: &Atom) -> HeadInstruction {
    let arity = atom.terms().len();
    let mut reduced_terms = Vec::<Term>::with_capacity(arity);
    let mut append_instructions = Vec::<Vec<AppendInstruction>>::new();

    append_instructions.push(vec![]);
    let mut current_append_vector = &mut append_instructions[0];

    let mut variable_map = HashMap::<Identifier, usize>::new();

    for (term_index, term) in atom.terms().iter().enumerate() {
        match term {
            Term::NumericLiteral(nl) => match nl {
                NumericLiteral::Integer(i) => {
                    let instruction = AppendInstruction::Constant(
                        DataValueT::U64((*i).try_into().unwrap()),
                        false,
                    );
                    current_append_vector.push(instruction);
                }
                _ => unimplemented!(),
            },
            Term::Constant(identifier) => {
                let instruction = AppendInstruction::Constant(
                    DataValueT::U64(identifier.to_constant_u64()),
                    false,
                );
                current_append_vector.push(instruction);
            }
            Term::Variable(variable) => {
                if let Variable::Universal(universal_variable) = variable {
                    if let Some(repeat_index) = variable_map.get(universal_variable) {
                        let instruction = AppendInstruction::RepeatColumn(*repeat_index);
                        current_append_vector.push(instruction);
                    } else {
                        reduced_terms
                            .push(Term::Variable(Variable::Universal(*universal_variable)));

                        variable_map.insert(*universal_variable, term_index);

                        append_instructions.push(vec![]);
                        current_append_vector = append_instructions.last_mut().unwrap();
                    }
                } else {
                    panic!("This class provides a strategy only for datalog rules.");
                }
            }
            Term::RdfLiteral(_) => unimplemented!(),
        }
    }

    let reduced_atom = Atom::new(atom.predicate(), reduced_terms);

    HeadInstruction {
        reduced_atom,
        append_instructions,
        arity,
    }
}
