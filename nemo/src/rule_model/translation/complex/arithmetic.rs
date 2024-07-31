//! This module contains a function to create a arithmetic term
//! from the corresponding ast node.

use crate::parser::ast;

use crate::rule_model::components::term::operation::operation_kind::OperationKind;
use crate::rule_model::components::term::operation::Operation;
use crate::rule_model::{error::TranslationError, translation::ASTProgramTranslation};

impl<'a> ASTProgramTranslation<'a> {
    /// Create a arithmetic term from the corresponding AST node.
    pub(crate) fn build_arithmetic(
        &mut self,
        arithmetic: &'a ast::expression::complex::arithmetic::Arithmetic,
    ) -> Result<Operation, TranslationError> {
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
            self.build_inner_term(arithmetic.left())?,
            self.build_inner_term(arithmetic.right())?,
        ];

        Ok(Operation::new(kind, subterms))
    }
}
