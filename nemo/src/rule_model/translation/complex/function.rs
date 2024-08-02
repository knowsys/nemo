//! This module contains a function to create a function term
//! from the corresponding ast node.

use crate::rule_model::components::tag::Tag;
use crate::rule_model::components::term::function::FunctionTerm;
use crate::{
    parser::ast::{self},
    rule_model::{error::TranslationError, translation::ASTProgramTranslation},
};

impl<'a> ASTProgramTranslation<'a> {
    /// Create a function term from the corresponding AST node.
    pub(crate) fn build_function(
        &mut self,
        function: &'a ast::expression::complex::atom::Atom,
    ) -> Result<FunctionTerm, TranslationError> {
        let tag = Tag::from(self.resolve_tag(function.tag())?)
            .set_origin(self.register_node(function.tag()));

        let mut subterms = Vec::new();
        for expression in function.expressions() {
            subterms.push(self.build_inner_term(expression)?);
        }

        Ok(self.register_component(FunctionTerm::new(tag, subterms), function))
    }
}
