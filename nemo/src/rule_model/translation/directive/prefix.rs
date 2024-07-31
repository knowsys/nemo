//! This module contains a function for handling prefix statements.

use std::collections::hash_map::Entry;

use crate::{
    parser::ast::{self},
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
    pub fn handle_prefix(
        &mut self,
        prefix: &'a ast::directive::prefix::Prefix,
    ) -> Result<(), TranslationError> {
        match self.prefix_mapping.entry(prefix.prefix()) {
            Entry::Occupied(entry) => {
                let (_, prefix_first) = entry.get();
                return Err(TranslationError::new(
                    prefix.prefix_token().span(),
                    TranslationErrorKind::PrefixRedefinition,
                )
                .add_label(
                    ComplexErrorLabelKind::Information,
                    prefix_first.prefix_token().span().range(),
                    Info::FirstDefinition,
                ));
            }
            Entry::Vacant(entry) => {
                entry.insert((prefix.value().content(), prefix));
            }
        }

        Ok(())
    }
}
