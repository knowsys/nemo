//! This module defines [N3Comment].

use nom::{
    branch::alt,
    character::complete::{line_ending, not_line_ending},
    combinator::eof,
    multi::many0,
    sequence::delimited,
};

use crate::parser::{
    ParserResult,
    ast::{ProgramAST, comment::line::LineComment, token::Token},
    context::{ParserContext, context},
    input::ParserInput,
    span::Span,
};

/// Notation3 comment. Like a [LineComment], but using `#`.
#[derive(Debug)]
pub struct N3Comment<'a>(LineComment<'a>);

impl N3Comment<'_> {
    /// Return the content of the comment
    pub fn content(&self) -> String {
        self.0.content()
    }
}

const CONTEXT: ParserContext = ParserContext::Comment;

impl<'a> ProgramAST<'a> for N3Comment<'a> {
    fn children(&self) -> Vec<&dyn ProgramAST<'a>> {
        self.0.children()
    }

    fn span(&self) -> Span<'a> {
        self.0.span()
    }

    fn parse(input: ParserInput<'a>) -> ParserResult<'a, Self>
    where
        Self: Sized + 'a,
    {
        let input_span = input.span;

        context(
            CONTEXT,
            delimited(Token::n3_comment, not_line_ending, alt((line_ending, eof))),
        )(input)
        .map(|(rest, content)| {
            let rest_span = rest.span;

            (
                rest,
                Self(LineComment::new(
                    input_span.until_rest(&rest_span),
                    content.span,
                )),
            )
        })
    }

    fn context(&self) -> ParserContext {
        CONTEXT
    }
}

/// Represents a series of whitespaces or comments
#[derive(Debug)]
pub struct WSoC<'a> {
    /// [Span] associated with this comment
    _span: Span<'a>,
    /// comments
    comments: Vec<N3Comment<'a>>,
}

impl<'a> WSoC<'a> {
    /// Return comments contained within this object.
    pub fn comments(&self) -> &Vec<N3Comment<'a>> {
        &self.comments
    }

    fn parse_whitespace(input: ParserInput<'a>) -> ParserResult<'a, Option<N3Comment<'a>>> {
        Token::whitespace(input).map(|(rest, _)| (rest, None))
    }

    fn parse_comment(input: ParserInput<'a>) -> ParserResult<'a, Option<N3Comment<'a>>> {
        N3Comment::parse(input).map(|(rest, comment)| (rest, Some(comment)))
    }

    /// Parse whitespace or comments.
    pub fn parse(input: ParserInput<'a>) -> ParserResult<'a, Self> {
        let input_span = input.span;

        many0(alt((Self::parse_whitespace, Self::parse_comment)))(input).map(|(rest, comments)| {
            let rest_span = rest.span;

            (
                rest,
                Self {
                    _span: input_span.until_rest(&rest_span),
                    comments: comments.into_iter().flatten().collect(),
                },
            )
        })
    }
}

#[cfg(test)]
mod test {
    use nom::combinator::all_consuming;
    use std::assert_matches;

    use crate::parser::{
        ParserState,
        ast::{
            ProgramAST,
            n3::comment::{N3Comment, WSoC},
        },
        input::ParserInput,
    };

    #[test]
    fn parse_line_comment() {
        let test = vec![
            ("# my comment", " my comment".to_string()),
            ("#my comment", "my comment".to_string()),
            ("#  \tmy comment\n", "  \tmy comment".to_string()),
            ("#### my comment", " my comment".to_string()),
        ];

        for (input, expected) in test {
            let parser_input = ParserInput::new(input, ParserState::default());
            let result = all_consuming(N3Comment::parse)(parser_input);

            assert_matches!(result, Ok(_));

            let result = result.unwrap();
            assert_eq!(expected, result.1.content());
        }
    }

    #[test]
    fn parse_wsoc() {
        let test = vec![
            ("", 0),
            ("  \n  ", 0),
            ("   # my comment \n  # Another comment \n    ", 2),
        ];

        for (input, expected) in test {
            let parser_input = ParserInput::new(input, ParserState::default());
            let result = all_consuming(WSoC::parse)(parser_input);

            assert_matches!(result, Ok(_));

            let result = result.unwrap();
            assert_eq!(expected, result.1.comments().len());
        }
    }
}
