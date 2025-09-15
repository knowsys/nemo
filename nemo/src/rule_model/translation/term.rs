use crate::{
    parser::ast::{self},
    rule_model::{
        components::term::{
            Term, aggregate::Aggregate, function::FunctionTerm, map::Map,
            primitive::variable::Variable, tuple::Tuple,
        },
        error::translation_error::TranslationError,
        origin::Origin,
    },
};

use super::{
    ASTProgramTranslation, TranslationComponent,
    basic::{
        blank::BlankLiteral, boolean::BooleanLiteral, constant::ConstantLiteral,
        enc_number::EncodedNumberLiteral, number::NumberLiteral, rdf::RdfLiteral,
        string::StringLiteral,
    },
    complex::{
        arithmetic::ArithmeticOperation, fstring::FormatStringLiteral,
        operation::FunctionLikeOperation,
    },
};

impl TranslationComponent for Term {
    type Ast<'a> = ast::expression::Expression<'a>;

    fn build_component<'a>(
        translation: &mut ASTProgramTranslation,
        ast: &Self::Ast<'a>,
    ) -> Option<Self> {
        let inner = match ast {
            ast::expression::Expression::Arithmetic(arithmetic) => Term::from(
                ArithmeticOperation::build_component(translation, arithmetic)?.into_inner(),
            ),
            ast::expression::Expression::Atom(function) => {
                Term::from(FunctionTerm::build_component(translation, function)?)
            }
            ast::expression::Expression::Blank(blank) => {
                Term::from(BlankLiteral::build_component(translation, blank)?.into_inner())
            }
            ast::expression::Expression::Boolean(boolean) => {
                Term::from(BooleanLiteral::build_component(translation, boolean)?.into_inner())
            }
            ast::expression::Expression::Constant(constant) => {
                Term::from(ConstantLiteral::build_component(translation, constant)?.into_inner())
            }
            ast::expression::Expression::Number(number) => {
                Term::from(NumberLiteral::build_component(translation, number)?.into_inner())
            }
            ast::expression::Expression::EncodedNumber(enc_number) => Term::from(
                EncodedNumberLiteral::build_component(translation, enc_number)?.into_inner(),
            ),
            ast::expression::Expression::RdfLiteral(rdf_literal) => {
                Term::from(RdfLiteral::build_component(translation, rdf_literal)?.into_inner())
            }
            ast::expression::Expression::String(string) => {
                Term::from(StringLiteral::build_component(translation, string)?.into_inner())
            }
            ast::expression::Expression::Tuple(tuple) => {
                Term::from(Tuple::build_component(translation, tuple)?)
            }
            ast::expression::Expression::Variable(variable) => {
                Term::from(Variable::build_component(translation, variable)?)
            }
            ast::expression::Expression::Aggregation(aggregation) => {
                Term::from(Aggregate::build_component(translation, aggregation)?)
            }
            ast::expression::Expression::Map(map) => {
                Term::from(Map::build_component(translation, map)?)
            }
            ast::expression::Expression::Operation(operation) => Term::from(
                FunctionLikeOperation::build_component(translation, operation)?.into_inner(),
            ),
            ast::expression::Expression::Negation(negation) => {
                translation
                    .report
                    .add(negation, TranslationError::InnerExpressionNegation);
                return None;
            }
            ast::expression::Expression::Parenthesized(parenthesized) => {
                Term::build_component(translation, parenthesized.expression())?
            }
            ast::expression::Expression::FormatString(format_string) => Term::from(
                FormatStringLiteral::build_component(translation, format_string)?.into_inner(),
            ),
        };

        Some(Origin::ast(inner, ast))
    }
}
