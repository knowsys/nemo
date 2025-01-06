//! This module contains a function that converts an rdf literal AST node
//! into its corresponding logical representation.

use nemo_physical::datavalues::AnyDataValue;

use crate::parser::ast::{self, ProgramAST};

use crate::rule_model::error::translation_error::TranslationErrorKind;
use crate::rule_model::translation::TranslationComponent;
use crate::rule_model::{error::TranslationError, translation::ASTProgramTranslation};

pub(crate) struct RdfLiteral(AnyDataValue);

impl RdfLiteral {
    pub(crate) fn into_inner(self) -> AnyDataValue {
        self.0
    }
}

impl TranslationComponent for RdfLiteral {
    type Ast<'a> = ast::expression::basic::rdf_literal::RdfLiteral<'a>;

    fn build_component<'a, 'b>(
        translation: &mut ASTProgramTranslation<'a, 'b>,
        rdf: &'b Self::Ast<'a>,
    ) -> Result<Self, TranslationError> {
        let datatype_iri = translation.resolve_tag(rdf.tag())?;

        match AnyDataValue::new_from_typed_literal(rdf.content(), datatype_iri) {
            Ok(data_value) => Ok(RdfLiteral(data_value)),
            Err(error) => Err(TranslationError::new(
                rdf.span(),
                TranslationErrorKind::DataValueCreationError(error),
            )),
        }
    }
}
