//! This module contains a function to create a blank term
//! from the corresponding ast node.

use nemo_physical::datavalues::AnyDataValue;

use crate::newtype_wrapper;
use crate::parser::ast;

use crate::rule_model::translation::ASTProgramTranslation;
use crate::rule_model::translation::TranslationComponent;

#[derive(Debug, Clone)]
pub(crate) struct BlankLiteral(AnyDataValue);
newtype_wrapper!(BlankLiteral: AnyDataValue);

impl TranslationComponent for BlankLiteral {
    type Ast<'a> = ast::expression::basic::blank::Blank<'a>;

    fn build_component<'a>(
        _translation: &mut ASTProgramTranslation,
        blank: &Self::Ast<'a>,
    ) -> Option<Self> {
        Some(BlankLiteral(AnyDataValue::new_iri(blank.name())))
    }
}
