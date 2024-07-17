//! This module defines [Iri]
#![allow(missing_docs)]

use nom::sequence::tuple;

use crate::parser::{
    ast::{token::Token, ProgramAST},
    context::{context, ParserContext},
    input::ParserInput,
    span::ProgramSpan,
    ParserResult,
};

/// AST node representing a Iri
#[derive(Debug)]
pub struct Iri<'a> {
    /// [ProgramSpan] associated with this node
    span: ProgramSpan<'a>,

    /// Part of the Iri that is the content
    content: Token<'a>,
}

impl<'a> Iri<'a> {
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

    fn span(&self) -> ProgramSpan {
        self.span
    }

    fn parse(input: ParserInput<'a>) -> ParserResult<'a, Self>
    where
        Self: Sized + 'a,
    {
        let input_span = input.span;

        context(
            CONTEXT,
            tuple((Token::open_chevrons, Token::iri, Token::closed_chevrons)),
        )(input)
        .map(|(rest, (_, content, _))| {
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
