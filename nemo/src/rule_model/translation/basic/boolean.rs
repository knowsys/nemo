//! This module contains a function to create a boolean term
//! from the corresponding ast node.

use nemo_physical::datavalues::AnyDataValue;

use crate::newtype_wrapper;
use crate::parser::ast;

use crate::rule_model::translation::ASTProgramTranslation;
use crate::rule_model::translation::TranslationComponent;

#[derive(Debug, Clone)]
pub(crate) struct BooleanLiteral(AnyDataValue);
newtype_wrapper!(BooleanLiteral: AnyDataValue);

impl TranslationComponent for BooleanLiteral {
    type Ast<'a> = ast::expression::basic::boolean::Boolean<'a>;

    fn build_component<'a>(
        _translation: &mut ASTProgramTranslation,
        boolean: &Self::Ast<'a>,
    ) -> Option<Self> {
        let truth = match boolean.value() {
            ast::expression::basic::boolean::BooleanValue::False => false,
            ast::expression::basic::boolean::BooleanValue::True => true,
        };

        Some(BooleanLiteral(AnyDataValue::new_boolean(truth)))
    }
}
