//! This module contains a function for handling output statements.

use crate::{
    parser::ast,
    rule_model::{
        components::{output::Output, tag::Tag},
        error::TranslationError,
        translation::ASTProgramTranslation,
    },
};

impl<'a> ASTProgramTranslation<'a> {
    /// Handle a output ast node.
    pub fn handle_output(
        &mut self,
        output: &'a ast::directive::output::Output,
    ) -> Result<(), TranslationError> {
        for predicate in output.predicates() {
            let tag =
                Tag::new(self.resolve_tag(predicate)?).set_origin(self.register_node(predicate));
            self.program_builder.add_output(Output::new(tag));
        }

        Ok(())
    }
}
