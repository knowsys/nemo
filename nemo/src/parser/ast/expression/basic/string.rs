//! This module defines [String]
#![allow(missing_docs)]

use nemo_physical::datavalues::syntax::string::{QUOTE, TRIPLE_QUOTE};
use nom::{
    branch::alt,
    combinator::opt,
    sequence::{delimited, pair},
};

use crate::{
    parser::{
        ast::{token::Token, ProgramAST},
        context::{context, ParserContext},
        input::ParserInput,
        span::Span,
        ParserResult,
    },
    syntax::pretty_printing::{fits_on_line, wrap_lines},
};

/// AST node representing a string
#[derive(Debug)]
pub struct StringLiteral<'a> {
    /// [Span] associated with this node
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
        alt((
            delimited(
                Token::triple_quote,
                alt((Token::multiline_string, Token::empty)),
                Token::triple_quote,
            ),
            delimited(
                Token::quote,
                alt((Token::string, Token::empty)),
                Token::quote,
            ),
        ))(input)
    }

    /// Parse the language tag of the string.
    pub fn parse_language_tag(input: ParserInput<'a>) -> ParserResult<'a, Token<'a>> {
        pair(Token::lang_tag_indicator, Token::name)(input).map(|(rest, (_, tag))| (rest, tag))
    }
}

const CONTEXT: ParserContext = ParserContext::String;

impl<'a> ProgramAST<'a> for StringLiteral<'a> {
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

    fn pretty_print(&self, indent_level: usize) -> Option<String> {
        let content = self.content();
        Some(
            if fits_on_line(&content, indent_level, Some(QUOTE), Some(QUOTE)) {
                format!("{}{}{}", QUOTE, content, QUOTE)
            } else {
                format!(
                    "{}\n{}\n{}{}",
                    TRIPLE_QUOTE,
                    wrap_lines(
                        &content,
                        indent_level + TRIPLE_QUOTE.len(),
                        Some(&" ".repeat(TRIPLE_QUOTE.len()))
                    ),
                    " ".repeat(indent_level),
                    TRIPLE_QUOTE
                )
            },
        )
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
            ("\"\"", ("".to_string(), None)),
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
