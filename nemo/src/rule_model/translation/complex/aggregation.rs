//! This module contains a function to create an aggregation term
//! from the corresponding ast node.

use crate::{
    parser::ast::{self, ProgramAST},
    rule_model::{
        components::term::{aggregate::Aggregate, primitive::variable::Variable, Term},
        error::{translation_error::TranslationErrorKind, TranslationError},
        translation::{ASTProgramTranslation, TranslationComponent},
    },
};

impl TranslationComponent for Aggregate {
    type Ast<'a> = ast::expression::complex::aggregation::Aggregation<'a>;

    fn build_component<'a, 'b>(
        translation: &mut ASTProgramTranslation<'a, 'b>,
        aggregation: &'b Self::Ast<'a>,
    ) -> Result<Self, TranslationError> {
        let kind = if let Some(kind) = aggregation.kind() {
            kind
        } else {
            return Err(TranslationError::new(
                aggregation.tag().span(),
                TranslationErrorKind::AggregationUnknown(aggregation.tag().content()),
            ));
        };

        let aggregate = Term::build_component(translation, aggregation.aggregate())?;
        let mut distinct = Vec::new();
        for expression in aggregation.distinct() {
            if let ast::expression::Expression::Variable(variable) = expression {
                distinct.push(Variable::build_component(translation, variable)?);
            } else {
                return Err(TranslationError::new(
                    expression.span(),
                    TranslationErrorKind::AggregationDistinctNonVariable(
                        expression.context_type().name().to_string(),
                    ),
                ));
            }
        }

        Ok(translation.register_component(Aggregate::new(kind, aggregate, distinct), aggregation))
    }
}
