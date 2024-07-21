//! This module defines [StructureTag].

use nom::{branch::alt, combinator::map, sequence::separated_pair};

use crate::parser::{
    ast::{expression::basic::iri::Iri, token::Token, ProgramAST},
    context::{context, ParserContext},
    input::ParserInput,
    span::ProgramSpan,
    ParserResult,
};

/// Types of [StructureTag]s
#[derive(Debug)]
pub enum StructureTagKind<'a> {
    /// Plain name
    Plain(Token<'a>),
    /// Prefixed name
    Prefixed { prefix: Token<'a>, tag: Token<'a> },
    /// Iri
    Iri(Iri<'a>),
}

/// Tags that is used to give a name to complex expressions
#[derive(Debug)]
pub struct StructureTag<'a> {
    /// [ProgramSpan] associated with this node
    span: ProgramSpan<'a>,

    /// Type of [StructureTag]
    kind: StructureTagKind<'a>,
}

impl<'a> StructureTag<'a> {
    /// Return the type of structure tag.
    pub fn kind(&self) -> &StructureTagKind<'a> {
        &self.kind
    }

    /// Return a string representation of the [Tag].
    ///
    /// Note that this does not resolve prefixes.
    pub fn to_string(&self) -> String {
        match &self.kind {
            StructureTagKind::Plain(token) => token.to_string(),
            StructureTagKind::Prefixed { prefix, tag } => {
                format!("{}::{}", prefix.to_string(), tag.to_string())
            }
            StructureTagKind::Iri(iri) => iri.content(),
        }
    }
}

const CONTEXT: ParserContext = ParserContext::StructureTag;

impl<'a> ProgramAST<'a> for StructureTag<'a> {
    fn children(&self) -> Vec<&dyn ProgramAST> {
        match self.kind() {
            StructureTagKind::Plain(_token) => vec![],
            StructureTagKind::Prefixed { prefix: _, tag: _ } => vec![],
            StructureTagKind::Iri(iri) => iri.children(),
        }
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
            alt((
                map(
                    separated_pair(Token::name, Token::double_colon, Token::name),
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
        ast::{tag::structure::StructureTag, ProgramAST},
        input::ParserInput,
        ParserState,
    };

    #[test]
    fn parse_tag() {
        let test = vec![
            ("abc", "abc".to_string()),
            ("abc::def", "abc::def".to_string()),
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
