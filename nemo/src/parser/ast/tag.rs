//! This module defines [Tag].

use nom::{branch::alt, combinator::map, sequence::tuple};

use crate::parser::{input::ParserInput, ParserResult};

use super::{expression::basic::iri::Iri, token::Token, ProgramAST};

/// Tag used to give a name to complex expressions
#[derive(Debug)]
pub enum Tag<'a> {
    /// Plain name
    Plain(Token<'a>),
    /// Prefixed name
    Prefixed { prefix: Token<'a>, tag: Token<'a> },
    /// Iri
    Iri(Iri<'a>),
}

impl<'a> Tag<'a> {
    /// Parse a [Tag].
    pub fn parse(input: ParserInput<'a>) -> ParserResult<'a, Self> {
        alt((
            map(
                tuple((Token::name, Token::colon, Token::name)),
                |(prefix, _, tag)| Self::Prefixed { prefix, tag },
            ),
            map(Token::name, Self::Plain),
            map(Iri::parse, Self::Iri),
        ))(input)
    }

    /// Return a string representation of the [Tag].
    ///
    /// Note that this does not resolve prefixes.
    pub fn to_string(&self) -> String {
        match self {
            Tag::Plain(token) => token.to_string(),
            Tag::Prefixed { prefix, tag } => format!("{}:{}", prefix.to_string(), tag.to_string()),
            Tag::Iri(iri) => iri.content(),
        }
    }
}

#[cfg(test)]
mod test {
    use nom::combinator::all_consuming;

    use crate::parser::{ast::tag::Tag, input::ParserInput, ParserState};

    #[test]
    fn parse_tag() {
        let test = vec![
            ("abc", "abc".to_string()),
            ("abc:def", "abc:def".to_string()),
            ("<http://example.com>", "http://example.com".to_string()),
        ];

        for (input, expected) in test {
            let parser_input = ParserInput::new(input, ParserState::default());
            let result = all_consuming(Tag::parse)(parser_input);

            assert!(result.is_ok());

            let result = result.unwrap();
            assert_eq!(expected, result.1.to_string());
        }
    }
}
