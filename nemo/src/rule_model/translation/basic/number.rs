//! This module contains a function to create a number term
//! from the corresponding ast node.

use nemo_physical::datavalues::AnyDataValue;

use crate::parser::ast;

use crate::parser::ast::expression::basic::number::NumberTypeMarker;
use crate::rule_model::{error::TranslationError, translation::ASTProgramTranslation};

impl<'a> ASTProgramTranslation<'a> {
    /// Create a number term from the corresponding AST node.
    pub(crate) fn build_number(
        &mut self,
        number: &'a ast::expression::basic::number::Number,
    ) -> Result<AnyDataValue, TranslationError> {
        Ok(match number.value() {
            ast::expression::basic::number::NumberValue::Integer(integer) => {
                AnyDataValue::new_integer_from_i64(integer)
            }
            ast::expression::basic::number::NumberValue::Float(float) => {
                AnyDataValue::new_float_from_f32(float).expect("NaN and infinity are not parsed")
            }
            ast::expression::basic::number::NumberValue::Double(double) => {
                AnyDataValue::new_double_from_f64(double).expect("NaN and infinity are not parsed")
            }
            ast::expression::basic::number::NumberValue::Large(large) => {
                let datatype_iri = if number.is_exponential() {
                    match number.type_marker() {
                        Some(NumberTypeMarker::Float) => "xsd:float",
                        Some(NumberTypeMarker::Double) | None => "xsd:double",
                    }
                } else {
                    match number.type_marker() {
                        Some(NumberTypeMarker::Float) => "xsd:float",
                        Some(NumberTypeMarker::Double) => "xsd:double",
                        None => {
                            if number.is_fractional() {
                                "xsd:decimal"
                            } else {
                                "xsd:integer"
                            }
                        }
                    }
                };

                AnyDataValue::new_other(large, String::from(datatype_iri))
            }
        })
    }
}