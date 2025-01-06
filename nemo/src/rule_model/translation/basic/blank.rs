//! This module contains a function to create a blank term
//! from the corresponding ast node.

use nemo_physical::datavalues::AnyDataValue;

use crate::parser::ast;

use crate::rule_model::translation::TranslationComponent;
use crate::rule_model::{error::TranslationError, translation::ASTProgramTranslation};

#[derive(Debug, Clone)]
pub(crate) struct BlankLiteral(AnyDataValue);

impl BlankLiteral {
    pub(crate) fn into_inner(self) -> AnyDataValue {
        self.0
    }
}

impl TranslationComponent for BlankLiteral {
    type Ast<'a> = ast::expression::basic::blank::Blank<'a>;

    fn build_component<'a, 'b>(
        translation: &mut ASTProgramTranslation<'a, 'b>,
        blank: &'b Self::Ast<'a>,
    ) -> Result<Self, TranslationError> {
        let blank_string = format!("{}/{}", translation.input_label, blank.name());

        Ok(BlankLiteral(AnyDataValue::new_iri(blank_string)))
    }
}
