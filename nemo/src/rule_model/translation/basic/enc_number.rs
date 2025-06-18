//! This module contains a function to create a enc_number term
//! from the corresponding ast node.

use nemo_physical::datavalues::AnyDataValue;

use crate::newtype_wrapper;
use crate::parser::ast;

use crate::rule_model::translation::ASTProgramTranslation;
use crate::rule_model::translation::TranslationComponent;

pub(crate) struct EncodedNumberLiteral(AnyDataValue);
newtype_wrapper!(EncodedNumberLiteral: AnyDataValue);

impl TranslationComponent for EncodedNumberLiteral {
    type Ast<'a> = ast::expression::basic::enc_number::EncodedNumber<'a>;

    fn build_component<'a>(
        _translation: &mut ASTProgramTranslation,
        enc_number: &Self::Ast<'a>,
    ) -> Option<Self> {
        Some(EncodedNumberLiteral(match enc_number.value() {
            ast::expression::basic::enc_number::EncodedNumberValue::Integer(integer) => {
                AnyDataValue::new_integer_from_i64(integer)
            }
            ast::expression::basic::enc_number::EncodedNumberValue::Large(large) => {
                AnyDataValue::new_other(large, String::from("xsd:integer"))
            }
        }))
    }
}
