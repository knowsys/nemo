//! This module defines [Iri]
#![allow(missing_docs)]

use nom::sequence::delimited;

use crate::parser::{
    ast::{token::Token, ProgramAST},
    context::{context, ParserContext},
    input::ParserInput,
    span::Span,
    ParserResult,
};

/// AST node representing a Iri
#[derive(Debug)]
pub struct Iri<'a> {
    /// [Span] associated with this node
    span: Span<'a>,

    /// Part of the Iri that is the content
    content: Token<'a>,
}

impl Iri<'_> {
    /// Return the content of the iri.
    pub fn content(&self) -> String {
        self.content.to_string()
    }
}

const CONTEXT: ParserContext = ParserContext::Iri;

impl<'a> ProgramAST<'a> for Iri<'a> {
    fn children(&self) -> Vec<&dyn ProgramAST> {
        Vec::default()
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
            delimited(Token::open_iri, Token::iri, Token::close_iri),
        )(input)
        .map(|(rest, content)| {
            let rest_span = rest.span;

            (
                rest,
                Iri {
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
        ast::{expression::basic::iri::Iri, ProgramAST},
        input::ParserInput,
        ParserState,
    };

    #[test]
    fn parse_iri() {
        let test = vec![(
            "<https://www.test.com/test#test>",
            "https://www.test.com/test#test".to_string(),
        )];

        for (input, expected) in test {
            let parser_input = ParserInput::new(input, ParserState::default());
            let result = all_consuming(Iri::parse)(parser_input);

            assert!(result.is_ok());

            let result = result.unwrap();
            assert_eq!(expected, result.1.content());
        }
    }
}
