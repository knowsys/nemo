//! This module defines [RdfLiteral]
#![allow(missing_docs)]

use nom::sequence::{separated_pair, tuple};

use crate::parser::{
    ast::{
        tag::structure::StructureTag,
        token::{Token, TokenKind},
        ProgramAST,
    },
    context::{context, ParserContext},
    input::ParserInput,
    span::Span,
    ParserResult,
};

/// AST node representing an rdf literal
#[derive(Debug)]
pub struct RdfLiteral<'a> {
    /// [Span] associated with this node
    span: Span<'a>,

    /// Content part rdf literal
    content: Token<'a>,
    /// Tag of the rdf literal
    tag: StructureTag<'a>,
}

impl<'a> RdfLiteral<'a> {
    /// Return the content of the rdf literal.
    pub fn content(&self) -> String {
        self.content.to_string()
    }

    // Return the tag of the rdf literal.
    pub fn tag(&self) -> &StructureTag<'a> {
        &self.tag
    }

    /// Parse the content part of the rdf literal.
    pub fn parse_content(input: ParserInput<'a>) -> ParserResult<'a, Token<'a>> {
        tuple((Token::quote, Token::string, Token::quote))(input)
            .map(|(rest, (_, content, _))| (rest, content))
    }
}

const CONTEXT: ParserContext = ParserContext::RdfLiteral;

impl<'a> ProgramAST<'a> for RdfLiteral<'a> {
    fn children(&self) -> Vec<&dyn ProgramAST<'a>> {
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
            separated_pair(
                Self::parse_content,
                Token::double_caret,
                StructureTag::parse,
            ),
        )(input)
        .map(|(rest, (content, tag))| {
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

    fn context(&self) -> ParserContext {
        CONTEXT
    }

    fn pretty_print(&self, indent_level: usize) -> Option<String> {
        Some(format!(
            "{}{}{}{}{}",
            TokenKind::Quote,
            self.content,
            TokenKind::Quote,
            TokenKind::RdfDatatypeIndicator,
            self.tag.pretty_print(indent_level)?
        ))
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
            assert_eq!(expected, (result.1.content(), result.1.tag().to_string()));
        }
    }
}
