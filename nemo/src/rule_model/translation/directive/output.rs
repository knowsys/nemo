//! This module contains a function for handling output statements.

use crate::{
    parser::ast::{self},
    rule_model::{
        components::{output::Output, Tag},
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
        let predicate = Tag::new(self.resolve_tag(output.predicate())?);
        self.program_builder.add_output(Output::new(predicate));

        Ok(())
    }
}
