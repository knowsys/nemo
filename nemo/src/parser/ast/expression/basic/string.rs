//! This module defines [String]
#![allow(missing_docs)]

use nom::{
    combinator::opt,
    sequence::{delimited, pair},
};

use crate::parser::{
    ast::{token::Token, ProgramAST},
    context::{context, ParserContext},
    input::ParserInput,
    span::Span,
    ParserResult,
};

/// AST node representing a string
#[derive(Debug)]
pub struct StringLiteral<'a> {
    /// [ProgramSpan] associated with this node
    span: Span<'a>,

    /// Part of the string that is the content
    content: Token<'a>,
    /// Optional language tag associated with the string
    language_tag: Option<Token<'a>>,
}

impl<'a> StringLiteral<'a> {
    /// Return the content of the string.
    pub fn content(&self) -> String {
        self.content.to_string()
    }

    /// Return the language tag of the string, if present.
    pub fn language_tag(&self) -> Option<String> {
        self.language_tag.as_ref().map(|token| token.to_string())
    }

    /// Parse the main part of the string.
    pub fn parse_string(input: ParserInput<'a>) -> ParserResult<'a, Token<'a>> {
        delimited(Token::quote, Token::string, Token::quote)(input)
    }

    /// Parse the language tag of the string.
    pub fn parse_language_tag(input: ParserInput<'a>) -> ParserResult<'a, Token<'a>> {
        pair(Token::at, Token::name)(input).map(|(rest, (_, tag))| (rest, tag))
    }
}

const CONTEXT: ParserContext = ParserContext::String;

impl<'a> ProgramAST<'a> for StringLiteral<'a> {
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
            pair(Self::parse_string, opt(Self::parse_language_tag)),
        )(input)
        .map(|(rest, (content, language_tag))| {
            let rest_span = rest.span;

            (
                rest,
                StringLiteral {
                    span: input_span.until_rest(&rest_span),
                    content,
                    language_tag,
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
        ast::{expression::basic::string::StringLiteral, ProgramAST},
        input::ParserInput,
        ParserState,
    };

    #[test]
    fn parse_string() {
        let test = vec![
            ("\"test\"", ("test".to_string(), None)),
            (
                "\"テスト\"@ja",
                ("テスト".to_string(), Some("ja".to_string())),
            ),
        ];

        for (input, expected) in test {
            let parser_input = ParserInput::new(input, ParserState::default());
            let result = all_consuming(StringLiteral::parse)(parser_input);

            assert!(result.is_ok());

            let result = result.unwrap();
            assert_eq!(expected, (result.1.content(), result.1.language_tag()))
        }
    }
}
