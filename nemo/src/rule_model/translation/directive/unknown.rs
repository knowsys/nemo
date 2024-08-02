//! This module contains a function for handling unknown directive statements.

use crate::{
    parser::ast::{self},
    rule_model::{
        error::{translation_error::TranslationErrorKind, TranslationError},
        translation::ASTProgramTranslation,
    },
};

impl<'a> ASTProgramTranslation<'a> {
    /// Handle a unknown directive ast node.
    pub fn handle_unknown_directive(
        &mut self,
        unknown: &'a ast::directive::unknown::UnknownDirective,
    ) -> Result<(), TranslationError> {
        Err(TranslationError::new(
            unknown.name_token().span(),
            TranslationErrorKind::DirectiveUnknown(unknown.name()),
        ))
    }
}
