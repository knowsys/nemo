//! This module contains a function to create an iri constant
//! from the corresponding ast node.

use nemo_physical::datavalues::AnyDataValue;

use crate::newtype_wrapper;
use crate::parser::ast;

use crate::rule_model::translation::TranslationComponent;
use crate::rule_model::{error::TranslationError, translation::ASTProgramTranslation};

#[derive(Debug, Clone)]
pub(crate) struct ConstantLiteral(AnyDataValue);
newtype_wrapper!(ConstantLiteral: AnyDataValue);

impl TranslationComponent for ConstantLiteral {
    type Ast<'a> = ast::expression::basic::constant::Constant<'a>;

    fn build_component<'a, 'b>(
        translation: &mut ASTProgramTranslation<'a, 'b>,
        constant: &'b Self::Ast<'a>,
    ) -> Result<Self, TranslationError> {
        let name = translation.resolve_tag(constant.tag())?;

        Ok(ConstantLiteral(AnyDataValue::new_iri(name)))
    }
}
