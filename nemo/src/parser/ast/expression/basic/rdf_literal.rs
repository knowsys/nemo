//! This module defines [RdfLiteral]
#![allow(missing_docs)]

use nom::sequence::tuple;

use crate::parser::{
    ast::{token::Token, ProgramAST},
    context::{context, ParserContext},
    input::ParserInput,
    span::ProgramSpan,
    ParserResult,
};

use super::iri::Iri;

/// AST node representing an rdf literal
#[derive(Debug)]
pub struct RdfLiteral<'a> {
    /// [ProgramSpan] associated with this node
    span: ProgramSpan<'a>,

    /// Content part rdf literal
    content: Token<'a>,
    /// Tag of the rdf literal
    tag: Iri<'a>,
}

impl<'a> RdfLiteral<'a> {
    /// Return the content of the rdf literal.
    pub fn content(&self) -> String {
        self.content.to_string()
    }

    // Return the tag of the rdf literal.
    pub fn tag(&self) -> String {
        self.tag.content()
    }

    /// Parse the content part of the rdf literal.
    pub fn parse_content(input: ParserInput<'a>) -> ParserResult<'a, Token<'a>> {
        tuple((Token::quote, Token::string, Token::quote))(input)
            .map(|(rest, (_, content, _))| (rest, content))
    }
}

impl<'a> ProgramAST<'a> for RdfLiteral<'a> {
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
            ParserContext::RdfLiteral,
            tuple((Self::parse_content, Token::double_caret, Iri::parse)),
        )(input)
        .map(|(rest, (content, _, tag))| {
            let rest_span = rest.span;

            (
                rest,
                RdfLiteral {
                    span: input_span.until_rest(&rest_span),
                    content,
                    tag,
                },
            )
        })
    }
}

#[cfg(test)]
mod test {
    use nom::combinator::all_consuming;

    use crate::parser::{
        ast::{expression::basic::rdf_literal::RdfLiteral, ProgramAST},
        input::ParserInput,
        ParserState,
    };

    #[test]
    fn parse_rdf_literal() {
        let test = vec![(
            "\"true\"^^<http://www.w3.org/2001/XMLSchema#boolean>",
            (
                "true".to_string(),
                "http://www.w3.org/2001/XMLSchema#boolean".to_string(),
            ),
        )];

        for (input, expected) in test {
            let parser_input = ParserInput::new(input, ParserState::default());
            let result = all_consuming(RdfLiteral::parse)(parser_input);

            assert!(result.is_ok());

            let result = result.unwrap();
            assert_eq!(expected, (result.1.content(), result.1.tag()));
        }
    }
}
