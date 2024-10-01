//! This module contains a function to create an iri constant
//! from the corresponding ast node.

use nemo_physical::datavalues::AnyDataValue;

use crate::parser::ast;

use crate::rule_model::{error::TranslationError, translation::ASTProgramTranslation};

impl<'a> ASTProgramTranslation<'a> {
    /// Create a constant term from the corresponding AST node.
    pub(crate) fn build_constant(
        &mut self,
        constant: &'a ast::expression::basic::constant::Constant,
    ) -> Result<AnyDataValue, TranslationError> {
        let name = self.resolve_tag(constant.tag())?;

        Ok(AnyDataValue::new_iri(name))
    }
}
