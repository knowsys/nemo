//! This module contains a function to create an operation
//! from an infix ast node.

use crate::{
    parser::ast::{self},
    rule_model::{
        components::term::operation::{operation_kind::OperationKind, Operation},
        error::TranslationError,
        translation::ASTProgramTranslation,
    },
};

impl<'a> ASTProgramTranslation<'a> {
    /// Create an [Operation] from an infix AST node.
    pub(crate) fn build_infix(
        &mut self,
        infix: &'a ast::expression::complex::infix::InfixExpression,
    ) -> Result<Operation, TranslationError> {
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

        let subterms = vec![self.build_inner_term(left)?, self.build_inner_term(right)?];

        Ok(self.register_component(Operation::new(kind, subterms), infix))
    }
}
