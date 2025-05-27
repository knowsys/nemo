//! This module contains a function to create an aggregation term
//! from the corresponding ast node.

use crate::{
    parser::ast::{self},
    rule_model::{
        components::term::{aggregate::Aggregate, primitive::variable::Variable, Term},
        error::translation_error::TranslationError,
        origin::Origin,
        translation::{ASTProgramTranslation, TranslationComponent},
    },
};

impl TranslationComponent for Aggregate {
    type Ast<'a> = ast::expression::complex::aggregation::Aggregation<'a>;

    fn build_component<'a>(
        translation: &mut ASTProgramTranslation,
        aggregation: &Self::Ast<'a>,
    ) -> Option<Self> {
        let kind = if let Some(kind) = aggregation.kind() {
            kind
        } else {
            translation.report.add(
                aggregation.tag(),
                TranslationError::AggregationUnknown {
                    aggregation: aggregation.tag().content(),
                },
            );
            return None;
        };

        let aggregate = Term::build_component(translation, aggregation.aggregate())?;
        let mut distinct = Vec::new();
        for expression in aggregation.distinct() {
            if let ast::expression::Expression::Variable(variable) = expression {
                distinct.push(Variable::build_component(translation, variable)?);
            } else {
                translation.report.add(
                    expression,
                    TranslationError::AggregationDistinctNonVariable {
                        kind: expression.context_type().name().to_string(),
                    },
                );
                return None;
            }
        }

        Some(Origin::ast(
            Aggregate::new(kind, aggregate, distinct),
            aggregation,
        ))
    }
}
