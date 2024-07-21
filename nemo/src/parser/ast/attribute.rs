//! This module defines [Attribute].

use nom::{
    character::complete::line_ending,
    sequence::{delimited, pair, terminated, tuple},
};

use crate::parser::{
    context::{context, ParserContext},
    input::ParserInput,
    span::ProgramSpan,
    ParserResult,
};

use super::{comment::wsoc::WSoC, expression::complex::atom::Atom, token::Token, ProgramAST};

/// Attribute of a rule
#[derive(Debug)]
pub struct Attribute<'a> {
    /// [ProgramSpan] associated with this node
    span: ProgramSpan<'a>,

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
    fn children(&self) -> Vec<&dyn ProgramAST> {
        vec![self.content()]
    }

    fn span(&self) -> ProgramSpan<'a> {
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
                    tuple((Token::hash, Token::open_bracket, WSoC::parse)),
                    Atom::parse,
                    pair(WSoC::parse, Token::closed_bracket),
                ),
                line_ending,
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
        ast::{attribute::Attribute, ProgramAST},
        input::ParserInput,
        ParserState,
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
