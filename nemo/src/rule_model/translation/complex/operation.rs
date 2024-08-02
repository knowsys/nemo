//! This module contains a function to create an operation term
//! from the corresponding ast node.

use crate::{
    parser::ast::{self},
    rule_model::{
        components::term::operation::Operation, error::TranslationError,
        translation::ASTProgramTranslation,
    },
};

impl<'a> ASTProgramTranslation<'a> {
    /// Create an operation term from the corresponding AST node.
    pub(crate) fn build_operation(
        &mut self,
        operation: &'a ast::expression::complex::operation::Operation,
    ) -> Result<Operation, TranslationError> {
        let kind = operation.kind();
        let mut subterms = Vec::new();
        for expression in operation.expressions() {
            subterms.push(self.build_inner_term(expression)?);
        }

        Ok(self.register_component(Operation::new(kind, subterms), operation))
    }
}
