//! This module contains a function for handling declare statements.

use crate::{
    parser::ast::{self, ProgramAST},
    rule_model::{
        error::{translation_error::TranslationErrorKind, TranslationError},
        translation::ASTProgramTranslation,
    },
};

impl<'a> ASTProgramTranslation<'a> {
    /// Handle a declare ast node.
    pub fn handle_declare(
        &mut self,
        declare: &'a ast::directive::declare::Declare,
    ) -> Result<(), TranslationError> {
        Err(TranslationError::new(
            declare.span(),
            TranslationErrorKind::UnsupportedDeclare,
        ))
    }
}
