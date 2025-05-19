//! This module contains a function that converts an rdf literal AST node
//! into its corresponding logical representation.

use nemo_physical::datavalues::AnyDataValue;

use crate::{
    newtype_wrapper,
    parser::ast,
    rule_model::{
        error::translation_error::TranslationError,
        translation::{ASTProgramTranslation, TranslationComponent},
    },
};

pub(crate) struct RdfLiteral(AnyDataValue);
newtype_wrapper!(RdfLiteral: AnyDataValue);

impl TranslationComponent for RdfLiteral {
    type Ast<'a> = ast::expression::basic::rdf_literal::RdfLiteral<'a>;

    fn build_component<'a, 'b>(
        translation: &mut ASTProgramTranslation,
        rdf: &'b Self::Ast<'a>,
    ) -> Option<Self> {
        let datatype_iri = translation.resolve_tag(rdf.tag())?;

        match AnyDataValue::new_from_typed_literal(rdf.content(), datatype_iri) {
            Ok(data_value) => Some(RdfLiteral(data_value)),
            Err(error) => {
                translation
                    .report
                    .add(rdf, TranslationError::DataValueCreationError(error));
                None
            }
        }
    }
}
