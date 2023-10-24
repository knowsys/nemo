//! Helper functions concerning the implementation of arithmetic operations.

use std::collections::HashMap;

use nemo_physical::{
    columnar::operations::arithmetic::expression::{StackOperation, StackProgram, StackValue},
    datatypes::DataValueT,
    management::{execution_plan::ExecutionNodeRef, ExecutionPlan},
    tabular::operations::triescan_append::AppendInstruction,
};

use crate::{
    model::{chase_model::Constructor, PrimitiveTerm, PrimitiveType, Term, Variable},
    program_analysis::variable_order::VariableOrder,
};

/// Builds a [`StackProgram`] with [`DataValueT`]
/// from a given [`Term`].
pub fn compile_termtree(
    term: &Term,
    order: &VariableOrder,
    logical_type: &PrimitiveType,
) -> StackProgram<DataValueT> {
    fn build_operations(
        term: &Term,
        operations: &mut Vec<StackOperation<DataValueT>>,
        order: &VariableOrder,
        logical_type: &PrimitiveType,
    ) {
        match term {
            Term::Aggregation(_) => unreachable!("Aggregation is not an arithmetic operation"),
            Term::Function(_, _) => panic!("Functions should not be evaluated"),

            Term::Primitive(primitive) => operations.push(StackOperation::Push(match primitive {
                PrimitiveTerm::Variable(variable) => StackValue::Reference(
                    *order
                        .get(variable)
                        .expect("Variable order must contain an entry for every variable."),
                ),
                PrimitiveTerm::Constant(constant) => StackValue::Constant(
                    logical_type
                        .ground_term_to_data_value_t(constant.clone())
                        .expect("Type checker should have caught any errors at this point."),
                ),
            })),

            Term::Binary {
                operation,
                lhs,
                rhs,
            } => {
                build_operations(lhs, operations, order, logical_type);
                build_operations(rhs, operations, order, logical_type);
                operations.push(StackOperation::BinaryOperation((*operation).into()))
            }

            Term::Unary(operation, inner) => {
                build_operations(inner, operations, order, logical_type);
                operations.push(StackOperation::UnaryOperation((*operation).into()))
            }
        }
    }

    let mut term_operations = Vec::new();
    build_operations(term, &mut term_operations, order, logical_type);
    StackProgram::new(term_operations).expect("Compilation produces only valid stack programs.")
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
        constructor_instructions.push(AppendInstruction::Arithmetic(compile_termtree(
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
