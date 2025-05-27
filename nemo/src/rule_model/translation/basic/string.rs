//! This module contains a function to create a string term
//! from the corresponding ast node.

use nemo_physical::datavalues::AnyDataValue;

use crate::{
    newtype_wrapper,
    parser::ast,
    rule_model::translation::{ASTProgramTranslation, TranslationComponent},
};

pub(crate) struct StringLiteral(AnyDataValue);
newtype_wrapper!(StringLiteral: AnyDataValue);

impl TranslationComponent for StringLiteral {
    type Ast<'a> = ast::expression::basic::string::StringLiteral<'a>;

    fn build_component<'a>(
        _translation: &mut ASTProgramTranslation,
        string: &Self::Ast<'a>,
    ) -> Option<Self> {
        let value = if let Some(language_tag) = string.language_tag() {
            AnyDataValue::new_language_tagged_string(string.content(), language_tag)
        } else {
            AnyDataValue::new_plain_string(string.content())
        };

        Some(StringLiteral(value))
    }
}
