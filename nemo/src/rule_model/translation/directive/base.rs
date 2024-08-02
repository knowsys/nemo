//! This module contains a function for handling base statements.

use crate::{
    parser::ast::{self, ProgramAST},
    rule_model::{
        error::{
            info::Info, translation_error::TranslationErrorKind, ComplexErrorLabelKind,
            TranslationError,
        },
        translation::ASTProgramTranslation,
    },
};

impl<'a> ASTProgramTranslation<'a> {
    /// Handle a base ast node.
    pub fn handle_base(
        &mut self,
        base: &'a ast::directive::base::Base,
    ) -> Result<(), TranslationError> {
        if let Some((_, first_base)) = &self.base {
            return Err(
                TranslationError::new(base.span(), TranslationErrorKind::BaseRedefinition)
                    .add_label(
                        ComplexErrorLabelKind::Information,
                        first_base.span().range(),
                        Info::FirstDefinition,
                    ),
            );
        }

        self.base = Some((base.iri().content(), base));

        Ok(())
    }
}
