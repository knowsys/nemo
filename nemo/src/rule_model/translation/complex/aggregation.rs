//! This module contains a function to create an aggregation term
//! from the corresponding ast node.

use crate::parser::ast::{self, ProgramAST};

use crate::rule_model::components::term::aggregate::Aggregate;
use crate::rule_model::error::translation_error::TranslationErrorKind;
use crate::rule_model::{error::TranslationError, translation::ASTProgramTranslation};

impl<'a> ASTProgramTranslation<'a> {
    /// Create an aggregation term from the corresponding AST node.
    pub(crate) fn build_aggregation(
        &mut self,
        aggregation: &'a ast::expression::complex::aggregation::Aggregation,
    ) -> Result<Aggregate, TranslationError> {
        let kind = if let Some(kind) = aggregation.kind() {
            kind
        } else {
            return Err(TranslationError::new(
                aggregation.tag().span(),
                TranslationErrorKind::AggregationUnknown(aggregation.tag().content()),
            ));
        };

        let aggregate = self.build_inner_term(aggregation.aggregate())?;
        let mut distinct = Vec::new();
        for expression in aggregation.distinct() {
            if let ast::expression::Expression::Variable(variable) = expression {
                distinct.push(self.build_variable(variable)?);
            } else {
                return Err(TranslationError::new(
                    expression.span(),
                    TranslationErrorKind::AggregationDistinctNonVariable(
                        expression.context_type().name().to_string(),
                    ),
                ));
            }
        }

        Ok(Aggregate::new(kind, aggregate, distinct))
    }
}
