//! This module defines [WSoC].

use nom::{branch::alt, combinator::map, multi::many0};

use crate::parser::{
    ast::{
        comment::{closed::ClosedComment, line::LineComment},
        token::Token,
        ProgramAST,
    },
    input::ParserInput,
    span::Span,
    ParserResult,
};

/// Type of comment that can appear in any "whit-space position"
#[derive(Debug)]
pub enum CommentType<'a> {
    /// Line comment
    Line(LineComment<'a>),
    /// Closed comment
    Closed(ClosedComment<'a>),
}

/// Represents a series of whitespaces or comments
#[derive(Debug)]
pub struct WSoC<'a> {
    /// [ProgramSpan] associated with this comment
    _span: Span<'a>,
    /// comments
    comments: Vec<CommentType<'a>>,
}

impl<'a> WSoC<'a> {
    /// Return comments contained within this object.
    pub fn comments(&self) -> &Vec<CommentType<'a>> {
        &self.comments
    }

    fn parse_whitespace(input: ParserInput<'a>) -> ParserResult<'a, Option<CommentType>> {
        Token::whitespace(input).map(|(rest, _)| (rest, None))
    }

    fn parse_comment(input: ParserInput<'a>) -> ParserResult<'a, Option<CommentType>> {
        alt((
            map(LineComment::parse, CommentType::Line),
            map(ClosedComment::parse, CommentType::Closed),
        ))(input)
        .map(|(rest, comment)| (rest, Some(comment)))
    }

    /// Parse whitespace or comments.
    pub fn parse(input: ParserInput<'a>) -> ParserResult<'a, Self> {
        let input_span = input.span;

        many0(alt((WSoC::parse_whitespace, WSoC::parse_comment)))(input).map(|(rest, comments)| {
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
