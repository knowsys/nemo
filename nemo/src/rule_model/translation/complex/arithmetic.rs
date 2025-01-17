//! This module contains a function to create a arithmetic term
//! from the corresponding ast node.

use crate::{
    newtype_wrapper,
    parser::ast::{self},
    rule_model::{
        components::term::{
            operation::{operation_kind::OperationKind, Operation},
            Term,
        },
        error::TranslationError,
        translation::{ASTProgramTranslation, TranslationComponent},
    },
};

pub(crate) struct ArithmeticOperation(Operation);
newtype_wrapper!(ArithmeticOperation: Operation);

impl TranslationComponent for ArithmeticOperation {
    type Ast<'a> = ast::expression::complex::arithmetic::Arithmetic<'a>;

    fn build_component<'a, 'b>(
        translation: &mut ASTProgramTranslation<'a, 'b>,
        arithmetic: &'b ast::expression::complex::arithmetic::Arithmetic<'a>,
    ) -> Result<Self, TranslationError> {
        let kind = match arithmetic.kind() {
            ast::expression::complex::arithmetic::ArithmeticOperation::Addition => {
                OperationKind::NumericSum
            }
            ast::expression::complex::arithmetic::ArithmeticOperation::Subtraction => {
                OperationKind::NumericSubtraction
            }
            ast::expression::complex::arithmetic::ArithmeticOperation::Multiplication => {
                OperationKind::NumericProduct
            }
            ast::expression::complex::arithmetic::ArithmeticOperation::Division => {
                OperationKind::NumericDivision
            }
        };

        let subterms = vec![
            Term::build_component(translation, arithmetic.left())?,
            Term::build_component(translation, arithmetic.right())?,
        ];

        Ok(ArithmeticOperation(translation.register_component(
            Operation::new(kind, subterms),
            arithmetic,
        )))
    }
}
