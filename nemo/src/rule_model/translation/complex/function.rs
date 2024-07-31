//! This module contains a function to create a function term
//! from the corresponding ast node.

use crate::parser::ast;

use crate::rule_model::components::term::function::FunctionTerm;
use crate::rule_model::{error::TranslationError, translation::ASTProgramTranslation};

impl<'a> ASTProgramTranslation<'a> {
    /// Create a function term from the corresponding AST node.
    pub(crate) fn build_function(
        &mut self,
        function: &'a ast::expression::complex::atom::Atom,
    ) -> Result<FunctionTerm, TranslationError> {
        let name = self.resolve_tag(function.tag())?;
        let mut subterms = Vec::new();
        for expression in function.expressions() {
            subterms.push(self.build_inner_term(expression)?);
        }

        Ok(FunctionTerm::new(&name, subterms))
    }
}
