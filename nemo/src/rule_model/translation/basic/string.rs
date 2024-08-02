//! This module contains a function to create a string term
//! from the corresponding ast node.

use nemo_physical::datavalues::AnyDataValue;

use crate::parser::ast;

use crate::rule_model::{error::TranslationError, translation::ASTProgramTranslation};

impl<'a> ASTProgramTranslation<'a> {
    /// Create a string term from the corresponding AST node.
    pub(crate) fn build_string(
        &mut self,
        string: &'a ast::expression::basic::string::StringLiteral,
    ) -> Result<AnyDataValue, TranslationError> {
        let value = if let Some(language_tag) = string.language_tag() {
            AnyDataValue::new_language_tagged_string(string.content(), language_tag)
        } else {
            AnyDataValue::new_plain_string(string.content())
        };

        Ok(value)
    }
}
