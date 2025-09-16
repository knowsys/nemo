//! This module defines [Negation].

use nom::sequence::{preceded, terminated};

use crate::parser::{
    ParserResult,
    ast::{ProgramAST, comment::wsoc::WSoC, expression::Expression, token::Token},
    context::{ParserContext, context},
    input::ParserInput,
    span::Span,
};

/// A possibly tagged sequence of [Expression]s.
#[derive(Debug)]
pub struct Negation<'a> {
    /// [Span] associated with this node
    span: Span<'a>,

    /// The negated expression
    expression: Box<Expression<'a>>,
}

impl<'a> Negation<'a> {
    /// Return the negated [Expression].
    pub fn expression(&self) -> &Expression<'a> {
        &self.expression
    }
}

const CONTEXT: ParserContext = ParserContext::Negation;

impl<'a> ProgramAST<'a> for Negation<'a> {
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

        context(
            CONTEXT,
            preceded(terminated(Token::tilde, WSoC::parse), Expression::parse),
        )(input)
        .map(|(rest, expression)| {
            let rest_span = rest.span;

            (
                rest,
                Self {
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

    use crate::parser::{
        ParserState,
        ast::{ProgramAST, expression::complex::negation::Negation},
        input::ParserInput,
    };

    #[test]
    fn parse_negation() {
        let test = vec!["~a(?x)", "~abc(?x, ?y)"];

        for input in test {
            let parser_input = ParserInput::new(input, ParserState::default());
            let result = all_consuming(Negation::parse)(parser_input);

            assert!(result.is_ok());
        }
    }
}
