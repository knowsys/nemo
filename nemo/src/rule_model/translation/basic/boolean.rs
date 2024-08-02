//! This module contains a function to create a boolean term
//! from the corresponding ast node.

use nemo_physical::datavalues::AnyDataValue;

use crate::parser::ast;

use crate::rule_model::{error::TranslationError, translation::ASTProgramTranslation};

impl<'a> ASTProgramTranslation<'a> {
    /// Create a boolean term from the corresponding AST node.
    pub(crate) fn build_boolean(
        &mut self,
        boolean: &'a ast::expression::basic::boolean::Boolean,
    ) -> Result<AnyDataValue, TranslationError> {
        let truth = match boolean.value() {
            ast::expression::basic::boolean::BooleanValue::False => false,
            ast::expression::basic::boolean::BooleanValue::True => true,
        };

        Ok(AnyDataValue::new_boolean(truth))
    }
}
