use crate::{
    parser::ast::{self, ProgramAST},
    rule_model::{
        components::{
            term::{
                aggregate::Aggregate, function::FunctionTerm, map::Map,
                primitive::variable::Variable, tuple::Tuple, Term,
            },
            ProgramComponent,
        },
        error::{translation_error::TranslationErrorKind, TranslationError},
    },
};

use super::{
    basic::{
        blank::BlankLiteral, boolean::BooleanLiteral, constant::ConstantLiteral,
        enc_number::EncodedNumberLiteral, number::NumberLiteral, rdf::RdfLiteral,
        string::StringLiteral,
    },
    complex::{
        arithmetic::ArithmeticOperation, fstring::FormatStringLiteral,
        operation::FunctionLikeOperation,
    },
    ASTProgramTranslation, TranslationComponent,
};

impl TranslationComponent for Term {
    type Ast<'a> = ast::expression::Expression<'a>;

    fn build_component<'a, 'b>(
        translation: &mut ASTProgramTranslation<'a, 'b>,
        ast: &'b Self::Ast<'a>,
    ) -> Result<Self, TranslationError> {
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
                return Err(TranslationError::new(
                    negation.span(),
                    TranslationErrorKind::InnerExpressionNegation,
                ))
            }
            ast::expression::Expression::Parenthesized(parenthesized) => {
                Term::build_component(translation, parenthesized.expression())?
            }
            ast::expression::Expression::FormatString(format_string) => Term::from(
                FormatStringLiteral::build_component(translation, format_string)?.into_inner(),
            ),
        };

        Ok(inner.set_origin(translation.register_node(ast)))
    }
}
