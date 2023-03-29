//! Functionality that is useful for planing a rule application but is not specific to either part.

use std::{
    collections::{HashMap, HashSet},
    ops::Range,
};

use crate::{
    logical::{
        model::{Atom, Filter, Identifier, NumericLiteral, Term, Variable},
        program_analysis::variable_order::VariableOrder,
        TableManager,
    },
    physical::{
        datatypes::{StorageTypeName, StorageValueT},
        dictionary::Dictionary,
        management::{
            database::{Dict, TableId},
            execution_plan::{ExecutionNodeRef, ExecutionPlan},
        },
        tabular::operations::{
            triescan_append::AppendInstruction, triescan_select::SelectEqualClasses,
            ValueAssignment,
        },
    },
};

/// This function replaces each variable in the atom with its position in the variable ordering
/// Example:
///     * a(x, y, z) with atom order [0, 1, 2] and variable order [y, z, x] results in [1, 2, 0]
/// This function for computing JoinBindings:
///         - Example: For a leapfrog join a(x, y, z) b(z, y) with order [x, y, z] you'd obtain [[0, 1, 2], [2, 1]]
pub(super) fn atom_binding(atom: &Atom, variable_order: &VariableOrder) -> Vec<usize> {
    atom.terms().iter().map(|t| if let Term::Variable(variable) = t {
        *variable_order.get(variable).unwrap()
    } else {
        panic!("It is assumed that this function is only called on atoms which only contain variables.");
    }).collect()
}

/// Calculate helper structures that define the filters that need to be applied.
/// TODO: Revise this when updating the type system.
pub(super) fn compute_filters(
    body_variables: &HashSet<Variable>,
    variable_order: &VariableOrder,
    filters: &[Filter],
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
                // TODO: this needs to change fot the type system; it panics in various cases right now
                filter_assignments.push(ValueAssignment {
                    column_idx: *variable_to_columnindex.get(&filter.left).unwrap(),
                    value_mapper: {
                        let filter_right_cloned = filter.right.clone();
                        Box::new(move |dict: &mut Dict| match filter_right_cloned.clone() {
                            Term::Constant(Identifier(s)) => {
                                StorageValueT::U64(dict.add(s).try_into().unwrap())
                            }
                            Term::Variable(_) => unreachable!(),
                            Term::NumericLiteral(n) => match n {
                                NumericLiteral::Integer(i) => {
                                    StorageValueT::U64(i.try_into().unwrap())
                                }
                                NumericLiteral::Decimal(_, _) => todo!(),
                                NumericLiteral::Double(d) => StorageValueT::Double(d),
                            },
                            Term::RdfLiteral(_) => todo!(),
                        })
                    },
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
    pub _arity: usize,
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
            Term::NumericLiteral(nl) => match *nl {
                NumericLiteral::Integer(i) => {
                    let instruction = AppendInstruction::Constant(
                        Box::new(move |_dict| StorageValueT::U64(i.try_into().unwrap())),
                        StorageTypeName::U64,
                        false,
                    );
                    current_append_vector.push(instruction);
                }
                _ => todo!(),
            },
            Term::Constant(identifier) => {
                let id_name = identifier.name();

                let instruction = AppendInstruction::Constant(
                    Box::new(move |dict| {
                        StorageValueT::U64(dict.add(id_name.clone()).try_into().unwrap())
                    }),
                    StorageTypeName::U64,
                    true,
                );
                current_append_vector.push(instruction);
            }
            Term::Variable(variable) => {
                let variable_identifier = match variable {
                    Variable::Universal(id) => id,
                    Variable::Existential(id) => id,
                };

                if let Some(repeat_index) = variable_map.get(variable_identifier) {
                    let instruction = AppendInstruction::RepeatColumn(*repeat_index);
                    current_append_vector.push(instruction);
                } else {
                    reduced_terms.push(Term::Variable(variable.clone()));

                    variable_map.insert(variable_identifier.clone(), term_index);

                    append_instructions.push(vec![]);
                    current_append_vector = append_instructions.last_mut().unwrap();
                }
            }
            Term::RdfLiteral(_) => todo!(),
        }
    }

    let reduced_atom = Atom::new(atom.predicate(), reduced_terms);

    HeadInstruction {
        reduced_atom,
        append_instructions,
        _arity: arity,
    }
}
