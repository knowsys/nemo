//! This module contains a function to create a string term
//! from the corresponding ast node.

use nemo_physical::datavalues::AnyDataValue;

use crate::parser::ast;

use crate::rule_model::translation::TranslationComponent;
use crate::rule_model::{error::TranslationError, translation::ASTProgramTranslation};

pub(crate) struct StringLiteral(AnyDataValue);

impl StringLiteral {
    pub(crate) fn into_inner(self) -> AnyDataValue {
        self.0
    }
}

impl TranslationComponent for StringLiteral {
    type Ast<'a> = ast::expression::basic::string::StringLiteral<'a>;

    fn build_component<'a, 'b>(
        _translation: &mut ASTProgramTranslation<'a, 'b>,
        string: &'b Self::Ast<'a>,
    ) -> Result<Self, TranslationError> {
        let value = if let Some(language_tag) = string.language_tag() {
            AnyDataValue::new_language_tagged_string(string.content(), language_tag)
        } else {
            AnyDataValue::new_plain_string(string.content())
        };

        Ok(StringLiteral(value))
    }
}
