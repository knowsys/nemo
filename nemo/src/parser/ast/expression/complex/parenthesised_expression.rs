use nom::sequence::{delimited, pair};

use crate::parser::{
    ast::{comment::wsoc::WSoC, expression::Expression, token::Token, ProgramAST},
    context::ParserContext,
    input::ParserInput,
    span::Span,
    ParserResult,
};

#[derive(Debug)]
pub struct ParenthesisedExpression<'a> {
    span: Span<'a>,
    expression: Expression<'a>,
}

impl<'a> ParenthesisedExpression<'a> {
    pub fn expression(&self) -> &Expression {
        &self.expression
    }

    pub fn parse_expression(input: ParserInput<'a>) -> ParserResult<'a, Expression<'a>> {
        Self::parse(input).map(|(rest, paren_expr)| (rest, paren_expr.expression))
    }
}

const CONTEXT: ParserContext = ParserContext::ParenthesisedExpression;

impl<'a> ProgramAST<'a> for ParenthesisedExpression<'a> {
    fn children(&'a self) -> Vec<&'a dyn ProgramAST> {
        vec![&self.expression]
    }

    fn span(&self) -> Span<'a> {
        self.span
    }

    /// Parse an expression enclosed in parenthesis.
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
                ParenthesisedExpression {
                    span: input_span.until_rest(&rest_span),
                    expression,
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
        let test = ["(1 * 2)"];

        for input in test {
            let parser_input = ParserInput::new(input, ParserState::default());
            let result = all_consuming(Expression::parse)(parser_input);
            dbg!(&result);

            assert!(result.is_ok());

            // let result = result.unwrap();
            // assert_eq!(result.1.context_type(), expect);
        }
    }
}
