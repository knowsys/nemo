//! This module contains a function to create a boolean term
//! from the corresponding ast node.

use nemo_physical::datavalues::AnyDataValue;

use crate::newtype_wrapper;
use crate::parser::ast;

use crate::rule_model::translation::TranslationComponent;
use crate::rule_model::{error::TranslationError, translation::ASTProgramTranslation};

#[derive(Debug, Clone)]
pub(crate) struct BooleanLiteral(AnyDataValue);
newtype_wrapper!(BooleanLiteral: AnyDataValue);

impl TranslationComponent for BooleanLiteral {
    type Ast<'a> = ast::expression::basic::boolean::Boolean<'a>;

    fn build_component<'a, 'b>(
        _translation: &mut ASTProgramTranslation<'a, 'b>,
        boolean: &'b Self::Ast<'a>,
    ) -> Result<Self, TranslationError> {
        let truth = match boolean.value() {
            ast::expression::basic::boolean::BooleanValue::False => false,
            ast::expression::basic::boolean::BooleanValue::True => true,
        };

        Ok(BooleanLiteral(AnyDataValue::new_boolean(truth)))
    }
}
