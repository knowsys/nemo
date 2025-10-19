//! This module defines [ParenthesizedExpression].

use nom::sequence::{delimited, pair};

use crate::{
    parser::{
        ast::{
            comment::wsoc::WSoC,
            expression::Expression,
            token::{Token, TokenKind},
            ProgramAST,
        },
        context::ParserContext,
        input::ParserInput,
        span::Span,
        ParserResult,
    },
    syntax::pretty_printing::fits_on_line,
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
    pub fn expression(&self) -> &Expression<'a> {
        &self.expression
    }
}

const CONTEXT: ParserContext = ParserContext::ParenthesizedExpression;

impl<'a> ProgramAST<'a> for ParenthesizedExpression<'a> {
    fn children(&self) -> Vec<&dyn ProgramAST<'a>> {
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

    fn pretty_print(&self, indent_level: usize) -> Option<String> {
        let mut result = format!("{}", TokenKind::OpenParenthesis);
        let content_indent = indent_level + result.len();
        let content = self.expression.pretty_print(content_indent)?;

        if fits_on_line(
            &content,
            content_indent,
            None,
            Some(TokenKind::ClosedParenthesis.name()),
        ) {
            result.push_str(&content);
        } else {
            result.push_str(&format!(
                "\n{}{content}\n{}",
                " ".repeat(content_indent),
                " ".repeat(indent_level)
            ));
        }

        result.push_str(&format!("{}", TokenKind::ClosedParenthesis));

        Some(result)
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
