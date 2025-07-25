//! This module contains a function to create an operation
//! from an infix ast node.

use crate::{
    newtype_wrapper,
    parser::ast::{self},
    rule_model::{
        components::term::{
            operation::{operation_kind::OperationKind, Operation},
            Term,
        },
        origin::Origin,
        translation::{ASTProgramTranslation, TranslationComponent},
    },
};

pub(crate) struct InfixOperation(Operation);
newtype_wrapper!(InfixOperation: Operation);

impl TranslationComponent for InfixOperation {
    type Ast<'a> = ast::expression::complex::infix::InfixExpression<'a>;

    fn build_component<'a>(
        translation: &mut ASTProgramTranslation,
        infix: &Self::Ast<'a>,
    ) -> Option<Self> {
        let kind = match infix.kind() {
            ast::expression::complex::infix::InfixExpressionKind::Equality => OperationKind::Equal,
            ast::expression::complex::infix::InfixExpressionKind::Inequality => {
                OperationKind::Unequals
            }
            ast::expression::complex::infix::InfixExpressionKind::GreaterEqual => {
                OperationKind::NumericGreaterthaneq
            }
            ast::expression::complex::infix::InfixExpressionKind::Greater => {
                OperationKind::NumericGreaterthan
            }
            ast::expression::complex::infix::InfixExpressionKind::LessEqual => {
                OperationKind::NumericLessthaneq
            }
            ast::expression::complex::infix::InfixExpressionKind::Less => {
                OperationKind::NumericLessthan
            }
        };

        let (left, right) = infix.pair();

        let subterms = vec![
            Term::build_component(translation, left)?,
            Term::build_component(translation, right)?,
        ];

        Some(InfixOperation(Origin::ast(
            Operation::new(kind, subterms),
            infix,
        )))
    }
}
