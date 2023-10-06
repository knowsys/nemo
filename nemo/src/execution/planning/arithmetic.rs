//! Helper functions concerning the implementation of arithmetic operations.

use std::collections::HashMap;

use nemo_physical::{
    columnar::operations::arithmetic::expression::ArithmeticTree,
    datatypes::DataValueT,
    management::{execution_plan::ExecutionNodeRef, ExecutionPlan},
    tabular::operations::triescan_append::AppendInstruction,
    util::TaggedTree,
};

use crate::{
    model::{PrimitiveType, Term, Variable},
    program_analysis::variable_order::VariableOrder,
};

fn termtree_to_arithmetictree(
    tree: &TaggedTree<TermOperation>,
    order: &VariableOrder,
    logical_type: &PrimitiveType,
) -> ArithmeticTree<DataValueT> {
    match &tree.tag {
        TermOperation::Term(term) => {
            if let Term::Variable(variable) = term {
                ArithmeticTree::Reference(
                    *order
                        .get(variable)
                        .expect("Variable order must contain an entry for every variable."),
                )
            } else {
                ArithmeticTree::Constant(
                    logical_type
                        .ground_term_to_data_value_t(term.clone())
                        .expect("Type checker should have caught any errors at this point."),
                )
            }
        }
        TermOperation::Addition => ArithmeticTree::Addition(
            tree.subtrees
                .iter()
                .map(|t| termtree_to_arithmetictree(t, order, logical_type))
                .collect(),
        ),
        TermOperation::Multiplication => ArithmeticTree::Multiplication(
            tree.subtrees
                .iter()
                .map(|t| termtree_to_arithmetictree(t, order, logical_type))
                .collect(),
        ),
        TermOperation::Subtraction => ArithmeticTree::Subtraction(
            Box::new(termtree_to_arithmetictree(
                &tree.subtrees[0],
                order,
                logical_type,
            )),
            Box::new(termtree_to_arithmetictree(
                &tree.subtrees[1],
                order,
                logical_type,
            )),
        ),
        TermOperation::Division => ArithmeticTree::Division(
            Box::new(termtree_to_arithmetictree(
                &tree.subtrees[0],
                order,
                logical_type,
            )),
            Box::new(termtree_to_arithmetictree(
                &tree.subtrees[1],
                order,
                logical_type,
            )),
        ),
        TermOperation::Function(_) => todo!("function terms are not implemented yet."),
    }
}

pub(super) fn generate_node_arithmetic(
    current_plan: &mut ExecutionPlan,
    variable_order: &VariableOrder,
    node: ExecutionNodeRef,
    first_unused_index: usize,
    constructors: &HashMap<Variable, TermTree>,
    types: &HashMap<Variable, PrimitiveType>,
) -> (ExecutionNodeRef, VariableOrder) {
    let mut instructions = vec![vec![]; variable_order.len() + 1];
    let constructor_instructions = &mut instructions[first_unused_index];

    let mut new_variable_order = variable_order.clone();

    for (constructor_index, (variable, tree)) in constructors.iter().enumerate() {
        new_variable_order.push_position(variable.clone(), first_unused_index + constructor_index);
        constructor_instructions.push(AppendInstruction::Arithmetic(termtree_to_arithmetictree(
            &tree.0,
            variable_order,
            types
                .get(variable)
                .expect("Every variable must be assigned to a type"),
        )));
    }

    (
        current_plan.append_columns(node, instructions),
        new_variable_order,
    )
}
