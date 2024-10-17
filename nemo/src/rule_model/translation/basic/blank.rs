//! This module contains a function to create a blank term
//! from the corresponding ast node.

use nemo_physical::datavalues::AnyDataValue;

use crate::parser::ast;

use crate::rule_model::{error::TranslationError, translation::ASTProgramTranslation};

impl<'a> ASTProgramTranslation<'a> {
    /// Create a boolean term from the corresponding AST node.
    pub(crate) fn build_blank(
        &mut self,
        blank: &'a ast::expression::basic::blank::Blank,
    ) -> Result<AnyDataValue, TranslationError> {
        let blank_string = format!("{}/{}", self.input_label, blank.name());

        Ok(AnyDataValue::new_iri(blank_string))
    }
}
