//! This module defines [WSoC].

use nom::{
    branch::alt,
    combinator::{map, opt},
    multi::many0,
    sequence::preceded,
};

use crate::parser::{
    ast::{token::Token, ProgramAST},
    input::ParserInput,
    span::ProgramSpan,
    ParserResult,
};

use super::{closed::ClosedComment, line::LineComment};

/// Type of comment that can appear in any "whit-space position"
#[derive(Debug)]
pub enum WhiteSpaceComment<'a> {
    /// Line comment
    Line(LineComment<'a>),
    /// Closed comment
    Closed(ClosedComment<'a>),
}

/// Represents a series of whitespaces or comments
#[derive(Debug)]
pub struct WSoC<'a> {
    /// [ProgramSpan] associated with this comment
    _span: ProgramSpan<'a>,
    /// comments
    comments: Vec<WhiteSpaceComment<'a>>,
}

impl<'a> WSoC<'a> {
    /// Return comments contained within this object.
    pub fn comments(&self) -> &Vec<WhiteSpaceComment<'a>> {
        &self.comments
    }

    /// Parse one or more white-spaces optionally followed by a comment.
    pub fn parse_whitespace_comment(
        input: ParserInput<'a>,
    ) -> ParserResult<'a, Option<WhiteSpaceComment>> {
        preceded(
            Token::whitespace,
            opt(alt((
                map(LineComment::parse, WhiteSpaceComment::Line),
                map(ClosedComment::parse, WhiteSpaceComment::Closed),
            ))),
        )(input)
    }

    /// Parse whitespace or comments.
    pub fn parse(input: ParserInput<'a>) -> ParserResult<'a, Self> {
        let input_span = input.span;

        many0(Self::parse_whitespace_comment)(input).map(|(rest, comments)| {
            let rest_span = rest.span;

            (
                rest,
                Self {
                    _span: input_span.until_rest(&rest_span),
                    comments: comments.into_iter().filter_map(|comment| comment).collect(),
                },
            )
        })
    }
}

#[cfg(test)]
mod test {
    use nom::combinator::all_consuming;

    use crate::parser::{ast::comment::wsoc::WSoC, input::ParserInput, ParserState};

    #[test]
    fn parse_wsoc() {
        let test = vec![
            ("", 0),
            ("  \n  ", 0),
            ("   // my comment \n  // Another comment \n    ", 2),
            ("   /* a comment */", 1),
        ];

        for (input, expected) in test {
            let parser_input = ParserInput::new(input, ParserState::default());
            let result = all_consuming(WSoC::parse)(parser_input);

            assert!(result.is_ok());

            let result = result.unwrap();
            assert_eq!(expected, result.1.comments().len());
        }
    }
}
