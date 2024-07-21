//! This module defines [Negation].

use nom::sequence::preceded;

use crate::parser::{
    ast::{expression::Expression, token::Token, ProgramAST},
    context::{context, ParserContext},
    input::ParserInput,
    span::ProgramSpan,
    ParserResult,
};

/// A possibly tagged sequence of [Expression]s.
#[derive(Debug)]
pub struct Negation<'a> {
    /// [ProgramSpan] associated with this node
    span: ProgramSpan<'a>,

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
    fn children(&self) -> Vec<&dyn ProgramAST> {
        vec![&*self.expression]
    }

    fn span(&self) -> ProgramSpan<'a> {
        self.span
    }

    fn parse(input: ParserInput<'a>) -> ParserResult<'a, Self>
    where
        Self: Sized + 'a,
    {
        let input_span = input.span;

        context(CONTEXT, preceded(Token::tilde, Expression::parse))(input).map(
            |(rest, expression)| {
                let rest_span = rest.span;

                (
                    rest,
                    Self {
                        span: input_span.until_rest(&rest_span),
                        expression: Box::new(expression),
                    },
                )
            },
        )
    }

    fn context(&self) -> ParserContext {
        CONTEXT
    }
}

#[cfg(test)]
mod test {
    use nom::combinator::all_consuming;

    use crate::parser::{
        ast::{expression::complex::negation::Negation, ProgramAST},
        input::ParserInput,
        ParserState,
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
