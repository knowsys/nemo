//! This module contains a function to create a tuple term
//! from the corresponding ast node.

use crate::{
    parser::ast,
    rule_model::{
        components::term::tuple::Tuple, error::TranslationError, translation::ASTProgramTranslation,
    },
};

impl<'a> ASTProgramTranslation<'a> {
    /// Create a tuple term from the corresponding AST node.
    pub(crate) fn build_tuple(
        &mut self,
        tuple: &'a ast::expression::complex::tuple::Tuple,
    ) -> Result<Tuple, TranslationError> {
        let mut subterms = Vec::new();
        for expression in tuple.expressions() {
            subterms.push(self.build_inner_term(expression)?);
        }

        Ok(self.register_component(Tuple::new(subterms), tuple))
    }
}
