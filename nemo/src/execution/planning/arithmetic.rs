//! Helper functions concerning the implementation of arithmetic operations.

use std::collections::HashMap;

use nemo_physical::{
    columnar::operations::arithmetic::expression::ArithmeticTree,
    datatypes::DataValueT,
    management::{execution_plan::ExecutionNodeRef, ExecutionPlan},
    tabular::operations::triescan_append::AppendInstruction,
};

use crate::{
    model::{chase_model::Constructor, PrimitiveTerm, PrimitiveType, Term, Variable},
    program_analysis::variable_order::VariableOrder,
};

pub(super) fn termtree_to_arithmetictree(
    term: &Term,
    order: &VariableOrder,
    logical_type: &PrimitiveType,
) -> ArithmeticTree<DataValueT> {
    match term {
        Term::Primitive(primitive) => match primitive {
            PrimitiveTerm::Variable(variable) => ArithmeticTree::Reference(
                *order
                    .get(variable)
                    .expect("Variable order must contain an entry for every variable."),
            ),
            PrimitiveTerm::Constant(constant) => ArithmeticTree::Constant(
                logical_type
                    .ground_term_to_data_value_t(constant.clone())
                    .expect("Type checker should have caught any errors at this point."),
            ),
        },
        Term::Addition(left, right) => {
            let left_tree = termtree_to_arithmetictree(left, order, logical_type);
            let right_tree = termtree_to_arithmetictree(right, order, logical_type);

            ArithmeticTree::Addition(vec![left_tree, right_tree])
        }
        Term::Subtraction(left, right) => {
            let left_tree = termtree_to_arithmetictree(left, order, logical_type);
            let right_tree = termtree_to_arithmetictree(right, order, logical_type);

            ArithmeticTree::Subtraction(Box::new(left_tree), Box::new(right_tree))
        }
        Term::Multiplication(left, right) => {
            let left_tree = termtree_to_arithmetictree(left, order, logical_type);
            let right_tree = termtree_to_arithmetictree(right, order, logical_type);

            ArithmeticTree::Multiplication(vec![left_tree, right_tree])
        }
        Term::Division(left, right) => {
            let left_tree = termtree_to_arithmetictree(left, order, logical_type);
            let right_tree = termtree_to_arithmetictree(right, order, logical_type);

            ArithmeticTree::Division(Box::new(left_tree), Box::new(right_tree))
        }
        Term::Exponent(left, right) => {
            let left_tree = termtree_to_arithmetictree(left, order, logical_type);
            let right_tree = termtree_to_arithmetictree(right, order, logical_type);

            ArithmeticTree::Exponent(Box::new(left_tree), Box::new(right_tree))
        }
        Term::SquareRoot(sub) => {
            let sub_tree = termtree_to_arithmetictree(sub, order, logical_type);

            ArithmeticTree::SquareRoot(Box::new(sub_tree))
        }
        Term::UnaryMinus(sub) => {
            let sub_tree = termtree_to_arithmetictree(sub, order, logical_type);

            ArithmeticTree::Negation(Box::new(sub_tree))
        }
        Term::Abs(sub) => {
            let sub_tree = termtree_to_arithmetictree(sub, order, logical_type);

            ArithmeticTree::Abs(Box::new(sub_tree))
        }
        Term::Aggregation(_) => unreachable!("Aggregation is not an arithmetic operation"),
        Term::Function(_) => panic!("Functions should not be evaluated"),
    }
}

pub(super) fn generate_node_arithmetic(
    current_plan: &mut ExecutionPlan,
    variable_order: &VariableOrder,
    node: ExecutionNodeRef,
    first_unused_index: usize,
    constructors: &[Constructor],
    types: &HashMap<Variable, PrimitiveType>,
) -> (ExecutionNodeRef, VariableOrder) {
    let mut instructions = vec![vec![]; variable_order.len() + 1];
    let constructor_instructions = &mut instructions[first_unused_index];

    let mut new_variable_order = variable_order.clone();

    for (constructor_index, constructor) in constructors.iter().enumerate() {
        new_variable_order.push_position(
            constructor.variable().clone(),
            first_unused_index + constructor_index,
        );
        constructor_instructions.push(AppendInstruction::Arithmetic(termtree_to_arithmetictree(
            constructor.term(),
            variable_order,
            types
                .get(constructor.variable())
                .expect("Every variable must be assigned to a type"),
        )));
    }

    (
        current_plan.append_columns(node, instructions),
        new_variable_order,
    )
}
