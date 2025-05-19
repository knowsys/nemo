//! This module contains a function to create an iri constant
//! from the corresponding ast node.

use nemo_physical::datavalues::AnyDataValue;

use crate::newtype_wrapper;
use crate::parser::ast;

use crate::rule_model::translation::ASTProgramTranslation;
use crate::rule_model::translation::TranslationComponent;

#[derive(Debug, Clone)]
pub(crate) struct ConstantLiteral(AnyDataValue);
newtype_wrapper!(ConstantLiteral: AnyDataValue);

impl TranslationComponent for ConstantLiteral {
    type Ast<'a> = ast::expression::basic::constant::Constant<'a>;

    fn build_component<'a>(
        translation: &mut ASTProgramTranslation,
        constant: &Self::Ast<'a>,
    ) -> Option<Self> {
        let name = translation.resolve_tag(constant.tag())?;

        Some(ConstantLiteral(AnyDataValue::new_iri(name)))
    }
}
