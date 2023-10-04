//! Helper functions concerning the implementation of arithmetic operations.

use std::collections::HashMap;

use nemo_physical::{
    columnar::operations::columnscan_arithmetic::{ArithmeticOperation, BinaryOperation},
    datatypes::DataValueT,
    management::{execution_plan::ExecutionNodeRef, ExecutionPlan},
    tabular::operations::triescan_append::AppendInstruction,
    util::TaggedTree,
};

use crate::{
    model::{PrimitiveType, Term, TermOperation, TermTree, Variable},
    program_analysis::variable_order::VariableOrder,
};

fn append_term_instructions(
    term: &TaggedTree<TermOperation>,
    variable_order: &VariableOrder,
    logical_type: &PrimitiveType,
    instructions: &mut Vec<ArithmeticOperation<DataValueT>>,
) {
    match &term.tag {
        TermOperation::Term(term) => {
            if let Term::Variable(variable) = term {
                instructions.push(ArithmeticOperation::PushRef(
                    *variable_order
                        .get(variable)
                        .expect("Variable order must contain an entry for every variable."),
                ))
            } else {
                instructions.push(ArithmeticOperation::PushConst(
                    logical_type
                        .ground_term_to_data_value_t(term.clone())
                        .expect("Type checker should have caught any errors at this point."),
                ))
            }
        }
        TermOperation::Addition => {
            let mut subtrees = term.subtrees.iter();
            let lhs = subtrees
                .next()
                .expect("Arithmetic expression has at least two subtrees");

            append_term_instructions(lhs, variable_order, logical_type, instructions);

            for subtree in subtrees {
                append_term_instructions(subtree, variable_order, logical_type, instructions);
                instructions.push(ArithmeticOperation::BinaryOperation(
                    BinaryOperation::Addition,
                ));
            }
        }
        TermOperation::Subtraction => {
            let mut subtrees = term.subtrees.iter();
            let lhs = subtrees
                .next()
                .expect("Arithmetic expression has at least two subtrees");

            append_term_instructions(lhs, variable_order, logical_type, instructions);

            for subtree in subtrees {
                append_term_instructions(subtree, variable_order, logical_type, instructions);
                instructions.push(ArithmeticOperation::BinaryOperation(
                    BinaryOperation::Subtraction,
                ));
            }
        }
        TermOperation::Multiplication => {
            let mut subtrees = term.subtrees.iter();
            let lhs = subtrees
                .next()
                .expect("Arithmetic expression has at least two subtrees");

            append_term_instructions(lhs, variable_order, logical_type, instructions);

            for subtree in subtrees {
                append_term_instructions(subtree, variable_order, logical_type, instructions);
                instructions.push(ArithmeticOperation::BinaryOperation(
                    BinaryOperation::Multiplication,
                ));
            }
        }
        TermOperation::Division => {
            let mut subtrees = term.subtrees.iter();
            let lhs = subtrees
                .next()
                .expect("Arithmetic expression has at least two subtrees");

            append_term_instructions(lhs, variable_order, logical_type, instructions);

            for subtree in subtrees {
                append_term_instructions(subtree, variable_order, logical_type, instructions);
                instructions.push(ArithmeticOperation::BinaryOperation(
                    BinaryOperation::Division,
                ));
            }
        }
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

        let mut operation_instructions = Vec::new();
        append_term_instructions(
            &tree.0,
            variable_order,
            types
                .get(variable)
                .expect("Every variables has an assigned type at this point"),
            &mut operation_instructions,
        );

        constructor_instructions.push(AppendInstruction::Operation(operation_instructions));
    }

    (
        current_plan.append_columns(node, instructions),
        new_variable_order,
    )
}
