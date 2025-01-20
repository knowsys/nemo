//! This module contains a function to create a enc_number term
//! from the corresponding ast node.

use nemo_physical::datavalues::AnyDataValue;

use crate::newtype_wrapper;
use crate::parser::ast;

use crate::rule_model::translation::TranslationComponent;
use crate::rule_model::{error::TranslationError, translation::ASTProgramTranslation};

pub(crate) struct EncodedNumberLiteral(AnyDataValue);
newtype_wrapper!(EncodedNumberLiteral: AnyDataValue);

impl TranslationComponent for EncodedNumberLiteral {
    type Ast<'a> = ast::expression::basic::enc_number::EncodedNumber<'a>;

    fn build_component<'a, 'b>(
        _translation: &mut ASTProgramTranslation<'a, 'b>,
        enc_number: &'b Self::Ast<'a>,
    ) -> Result<Self, TranslationError> {
        Ok(EncodedNumberLiteral(match enc_number.value() {
            ast::expression::basic::enc_number::NumberValue::Integer(integer) => {
                AnyDataValue::new_integer_from_i64(integer)
            }
            ast::expression::basic::enc_number::NumberValue::Large(large) => {
                AnyDataValue::new_other(large, String::from("xsd:integer"))
            }
        }))
    }
}
