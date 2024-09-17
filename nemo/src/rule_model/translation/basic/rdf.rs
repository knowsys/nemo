//! This module contains a function that converts an rdf literal AST node
//! into its corresponding logical representation.

use nemo_physical::datavalues::AnyDataValue;

use crate::parser::ast::{self, ProgramAST};

use crate::rule_model::error::translation_error::TranslationErrorKind;
use crate::rule_model::{error::TranslationError, translation::ASTProgramTranslation};

impl<'a> ASTProgramTranslation<'a> {
    /// Create a term the corresponding rdf AST node.
    pub(crate) fn build_rdf(
        &mut self,
        rdf: &'a ast::expression::basic::rdf_literal::RdfLiteral,
    ) -> Result<AnyDataValue, TranslationError> {
        let datatype_iri = self.resolve_tag(rdf.tag())?;

        match AnyDataValue::new_from_typed_literal(rdf.content(), datatype_iri) {
            Ok(data_value) => Ok(data_value),
            Err(error) => Err(TranslationError::new(
                rdf.span(),
                TranslationErrorKind::DataValueCreationError(error),
            )),
        }
    }
}
