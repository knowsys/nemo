//! Helper functions concerning the implementation of arithmetic operations.

use std::collections::HashMap;

use nemo_physical::{
    columnar::operations::columnscan_arithmetic::{ArithmeticOperand, ArithmeticOperation},
    management::{execution_plan::ExecutionNodeRef, ExecutionPlan},
    tabular::operations::triescan_append::{AppendInstruction, OperationTreeT},
    util::TaggedTree,
};

use crate::{
    model::{LeafTerm, PrimitiveType, PrimitiveValue, Term, TermOperation, Variable},
    program_analysis::variable_order::VariableOrder,
};

fn termtree_to_operationtree(
    tree: &TaggedTree<TermOperation, LeafTerm>,
    order: &VariableOrder,
    logical_type: &PrimitiveType,
) -> OperationTreeT {
    macro_rules! arithmetic_operation {
        ($op:expr) => {
            match $op {
                TermOperation::Addition => ArithmeticOperation::Addition,
                TermOperation::Subtraction => ArithmeticOperation::Subtraction,
                TermOperation::Division => ArithmeticOperation::Division,
                TermOperation::Multiplication => ArithmeticOperation::Multiplication,
                TermOperation::Function(_) => unreachable!("checked separately"),
            }
        };
    }
    match tree {
        TaggedTree::Leaf(term) => match term {
            LeafTerm::Variable(variable) => OperationTreeT::leaf(ArithmeticOperand::ColumnScan(
                *order
                    .get(variable)
                    .expect("Variable order must contain an entry for every variable."),
            )),
            LeafTerm::Constant(term) => OperationTreeT::leaf(ArithmeticOperand::Constant(
                logical_type
                    .ground_term_to_data_value_t(term.clone())
                    .expect("Type checker should have caught any errors at this point."),
            )),
            LeafTerm::Aggregate(_) => todo!(),
        },
        TaggedTree::Node { tag, subtrees } => match tag {
            TermOperation::Function(_) => todo!("function terms are not implemented yet."),
            op => OperationTreeT::tree(
                arithmetic_operation!(op),
                subtrees
                    .iter()
                    .map(|t| termtree_to_operationtree(t, order, logical_type))
                    .collect(),
            ),
        },
    }
}

pub(super) fn generate_node_arithmetic(
    current_plan: &mut ExecutionPlan,
    variable_order: &VariableOrder,
    node: ExecutionNodeRef,
    first_unused_index: usize,
    constructors: &HashMap<Variable, Term>,
    types: &HashMap<Variable, PrimitiveType>,
) -> (ExecutionNodeRef, VariableOrder) {
    let mut instructions = vec![vec![]; variable_order.len() + 1];
    let constructor_instructions = &mut instructions[first_unused_index];

    let mut new_variable_order = variable_order.clone();

    for (constructor_index, (variable, tree)) in constructors.iter().enumerate() {
        new_variable_order.push_position(variable.clone(), first_unused_index + constructor_index);
        constructor_instructions.push(AppendInstruction::Operation(termtree_to_operationtree(
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
