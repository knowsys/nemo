//! This module contains a helper function to translate [Term] into [FunctionTree]

use nemo_physical::{function::tree::FunctionTree, tabular::operations::OperationColumnMarker};

use crate::{
    execution::rule_execution::VariableTranslation,
    model::{BinaryOperation, PrimitiveTerm, Term, UnaryOperation},
};

/// Helper function to translate a [Term] into a [FunctionTree]
pub(super) fn term_to_function_tree(
    translation: &VariableTranslation,
    term: &Term,
) -> FunctionTree<OperationColumnMarker> {
    match term {
        Term::Primitive(primitive) => match primitive {
            PrimitiveTerm::Constant(constant) => FunctionTree::constant(constant.as_datavalue()),
            PrimitiveTerm::Variable(variable) => FunctionTree::reference(
                *translation
                    .get(variable)
                    .expect("Every variable must be known"),
            ),
        },
        Term::Binary {
            operation,
            lhs,
            rhs,
        } => {
            let left = term_to_function_tree(translation, lhs);
            let right = term_to_function_tree(translation, rhs);

            match operation {
                BinaryOperation::Addition => FunctionTree::numeric_addition(left, right),
                BinaryOperation::Subtraction => FunctionTree::numeric_subtraction(left, right),
                BinaryOperation::Multiplication => {
                    FunctionTree::numeric_multiplication(left, right)
                }
                BinaryOperation::Division => FunctionTree::numeric_division(left, right),
                BinaryOperation::Exponent => FunctionTree::numeric_power(left, right),
            }
        }
        Term::Unary(operation, subterm) => {
            let sub = term_to_function_tree(translation, subterm);

            match operation {
                UnaryOperation::SquareRoot => FunctionTree::numeric_squareroot(sub),
                UnaryOperation::UnaryMinus => FunctionTree::numeric_negation(sub),
                UnaryOperation::Abs => FunctionTree::numeric_absolute(sub),
            }
        }
        Term::Aggregation(_) => unimplemented!("Aggregates are not implement yet"),
        Term::Function(_, _) => unimplemented!("Function symbols are not supported yet"),
    }
}
