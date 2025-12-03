//! This module defines [Attribute].

use nom::sequence::{delimited, pair, terminated, tuple};

use crate::parser::{
    ParserResult,
    context::{ParserContext, context},
    input::ParserInput,
    span::Span,
};

use super::{ProgramAST, comment::wsoc::WSoC, expression::complex::atom::Atom, token::Token};

/// Attribute of a rule
#[derive(Debug)]
pub struct Attribute<'a> {
    /// [Span] associated with this node
    span: Span<'a>,

    /// [Atom] containing the content of the directive
    content: Atom<'a>,
}

impl<'a> Attribute<'a> {
    /// Return the [Atom] that contains the content of the attribute.
    pub fn content(&self) -> &Atom<'a> {
        &self.content
    }
}

const CONTEXT: ParserContext = ParserContext::Attribute;

impl<'a> ProgramAST<'a> for Attribute<'a> {
    fn children(&self) -> Vec<&dyn ProgramAST<'a>> {
        vec![self.content()]
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
            terminated(
                delimited(
                    tuple((Token::open_attribute, WSoC::parse)),
                    Atom::parse,
                    pair(WSoC::parse, Token::close_attribute),
                ),
                WSoC::parse,
            ),
        )(input)
        .map(|(rest, content)| {
            let rest_span = rest.span;

            (
                rest,
                Self {
                    span: input_span.until_rest(&rest_span),
                    content,
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
        ast::{ProgramAST, attribute::Attribute},
        input::ParserInput,
    };

    #[test]
    fn parse_attribute() {
        let test = vec![
            ("#[test(1, 2, 3)]\n", ("test".to_string(), 3)),
            ("#[ abc(1) ]\n", ("abc".to_string(), 1)),
        ];

        for (input, expected) in test {
            let parser_input = ParserInput::new(input, ParserState::default());
            let result = all_consuming(Attribute::parse)(parser_input);

            assert!(result.is_ok());

            let result = result.unwrap();
            assert_eq!(
                expected,
                (
                    result.1.content.tag().to_string(),
                    result.1.content.expressions().count()
                )
            );
        }
    }
}
