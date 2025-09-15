//! This module defines [StructureTag].

use std::fmt::Display;

use nom::{branch::alt, combinator::map, sequence::separated_pair};

use crate::parser::{
    ParserResult,
    ast::{ProgramAST, expression::basic::iri::Iri, token::Token},
    context::{ParserContext, context},
    input::ParserInput,
    span::Span,
};

/// Types of [StructureTag]s
#[derive(Debug)]
pub enum StructureTagKind<'a> {
    /// Plain name
    Plain(Token<'a>),
    /// Prefixed name
    Prefixed {
        /// The prefix
        prefix: Token<'a>,
        /// The name
        tag: Token<'a>,
    },
    /// Iri
    Iri(Iri<'a>),
}

/// Tags that is used to give a name to complex expressions
#[derive(Debug)]
pub struct StructureTag<'a> {
    /// [Span] associated with this node
    span: Span<'a>,

    /// Type of [StructureTag]
    kind: StructureTagKind<'a>,
}

impl<'a> StructureTag<'a> {
    /// Return the type of structure tag.
    pub fn kind(&self) -> &StructureTagKind<'a> {
        &self.kind
    }
}

impl Display for StructureTag<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.kind {
            StructureTagKind::Plain(token) => token.fmt(f),
            StructureTagKind::Prefixed { prefix, tag } => {
                f.write_fmt(format_args!("{prefix}:{tag}"))
            }
            StructureTagKind::Iri(iri) => iri.content().fmt(f),
        }
    }
}

const CONTEXT: ParserContext = ParserContext::StructureTag;

impl<'a> ProgramAST<'a> for StructureTag<'a> {
    fn children(&self) -> Vec<&dyn ProgramAST<'a>> {
        match self.kind() {
            StructureTagKind::Plain(_token) => vec![],
            StructureTagKind::Prefixed { prefix: _, tag: _ } => vec![],
            StructureTagKind::Iri(iri) => iri.children(),
        }
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
            alt((
                map(
                    separated_pair(
                        alt((Token::name, Token::empty)),
                        Token::namespace_separator,
                        Token::name,
                    ),
                    |(prefix, tag)| StructureTagKind::Prefixed { prefix, tag },
                ),
                map(Token::name, StructureTagKind::Plain),
                map(Iri::parse, StructureTagKind::Iri),
            )),
        )(input)
        .map(|(rest, kind)| {
            let rest_span = rest.span;
            (
                rest,
                Self {
                    span: input_span.until_rest(&rest_span),
                    kind,
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
        ast::{ProgramAST, tag::structure::StructureTag},
        input::ParserInput,
    };

    #[test]
    fn parse_tag() {
        let test = vec![
            ("abc", "abc".to_string()),
            ("abc:def", "abc:def".to_string()),
            (":def", ":def".to_string()),
            ("<http://example.com>", "http://example.com".to_string()),
        ];

        for (input, expected) in test {
            let parser_input = ParserInput::new(input, ParserState::default());
            let result = all_consuming(StructureTag::parse)(parser_input);

            assert!(result.is_ok());

            let result = result.unwrap();
            assert_eq!(expected, result.1.to_string());
        }
    }
}
