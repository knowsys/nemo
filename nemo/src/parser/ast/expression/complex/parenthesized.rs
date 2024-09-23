//! This module defines [ParenthesizedExpression].

use nom::sequence::{delimited, pair};

use crate::parser::{
    ast::{comment::wsoc::WSoC, expression::Expression, token::Token, ProgramAST},
    context::ParserContext,
    input::ParserInput,
    span::Span,
    ParserResult,
};

/// An [Expression] enclosed in parenthesis
#[derive(Debug)]
pub struct ParenthesizedExpression<'a> {
    /// [Span] associated with this node
    span: Span<'a>,
    /// The [Expression]
    expression: Box<Expression<'a>>,
}

impl<'a> ParenthesizedExpression<'a> {
    /// Create a new [ParenthesizedExpression].
    pub(crate) fn new(span: Span<'a>, expression: Expression<'a>) -> Self {
        Self {
            span,
            expression: Box::new(expression),
        }
    }

    /// Return the underlying expression.
    pub fn expression(&self) -> &Expression {
        &self.expression
    }
}

const CONTEXT: ParserContext = ParserContext::ParenthesizedExpression;

impl<'a> ProgramAST<'a> for ParenthesizedExpression<'a> {
    fn children(&'a self) -> Vec<&'a dyn ProgramAST<'a>> {
        vec![&*self.expression]
    }

    fn span(&self) -> Span<'a> {
        self.span
    }

    fn parse(input: ParserInput<'a>) -> ParserResult<'a, Self>
    where
        Self: Sized + 'a,
    {
        let input_span = input.span;
        delimited(
            pair(Token::open_parenthesis, WSoC::parse),
            Expression::parse,
            pair(WSoC::parse, Token::closed_parenthesis),
        )(input)
        .map(|(rest, expression)| {
            let rest_span = rest.span;
            (
                rest,
                ParenthesizedExpression {
                    span: input_span.until_rest(&rest_span),
                    expression: Box::new(expression),
                },
            )
        })
    }

    fn context(&self) -> ParserContext {
        CONTEXT
    }
}

#[cfg(test)]
mod test {
    use nom::combinator::all_consuming;

    use crate::parser::ParserState;

    use super::*;

    #[test]
    fn paren_expr() {
        let test = ["(int)"];

        for input in test {
            let parser_input = ParserInput::new(input, ParserState::default());
            let result = all_consuming(ParenthesizedExpression::parse)(parser_input);
            dbg!(&result);

            assert!(result.is_ok());
        }
    }
}
