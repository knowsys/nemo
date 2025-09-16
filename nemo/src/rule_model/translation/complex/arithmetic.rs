//! This module contains a function to create a arithmetic term
//! from the corresponding ast node.

use crate::{
    newtype_wrapper,
    parser::ast::{self},
    rule_model::{
        components::term::{
            Term,
            operation::{Operation, operation_kind::OperationKind},
        },
        origin::Origin,
        translation::{ASTProgramTranslation, TranslationComponent},
    },
};

pub(crate) struct ArithmeticOperation(Operation);
newtype_wrapper!(ArithmeticOperation: Operation);

impl TranslationComponent for ArithmeticOperation {
    type Ast<'a> = ast::expression::complex::arithmetic::Arithmetic<'a>;

    fn build_component<'a>(
        translation: &mut ASTProgramTranslation,
        arithmetic: &ast::expression::complex::arithmetic::Arithmetic<'a>,
    ) -> Option<Self> {
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

        Some(ArithmeticOperation(Origin::ast(
            Operation::new(kind, subterms),
            arithmetic,
        )))
    }
}
