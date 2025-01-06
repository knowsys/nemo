//! This module contains a function to create an operation
//! from an infix ast node.

use crate::{
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

pub(crate) struct InfixOperation(Operation);

impl InfixOperation {
    pub(crate) fn into_inner(self) -> Operation {
        self.0
    }
}

impl TranslationComponent for InfixOperation {
    type Ast<'a> = ast::expression::complex::infix::InfixExpression<'a>;

    fn build_component<'a, 'b>(
        translation: &mut ASTProgramTranslation<'a, 'b>,
        infix: &'b Self::Ast<'a>,
    ) -> Result<Self, TranslationError> {
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

        Ok(InfixOperation(
            translation.register_component(Operation::new(kind, subterms), infix),
        ))
    }
}
