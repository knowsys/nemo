//! This module defines [WSoC].

use nom::{InputTake, branch::alt, combinator::map};

use crate::parser::{
    ParserResult,
    ast::{
        ProgramAST,
        comment::{closed::ClosedComment, line::LineComment},
    },
    input::ParserInput,
    span::Span,
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
    /// [Span] associated with this comment
    _span: Span<'a>,
    /// comments
    comments: Vec<CommentType<'a>>,
}

impl<'a> WSoC<'a> {
    /// Return comments contained within this object.
    pub fn comments(&self) -> &Vec<CommentType<'a>> {
        &self.comments
    }

    fn parse_comment(input: ParserInput<'a>) -> ParserResult<'a, CommentType<'a>> {
        alt((
            map(LineComment::parse, CommentType::Line),
            map(ClosedComment::parse, CommentType::Closed),
        ))(input)
    }

    /// Parse whitespace or comments.
    pub fn parse(input: ParserInput<'a>) -> ParserResult<'a, Self> {
        let input_span = input.span;

        let mut rest = input;
        let mut comments = Vec::new();

        loop {
            let whitespace = rest
                .span
                .fragment()
                .bytes()
                .take_while(|byte| matches!(byte, b' ' | b'\t' | b'\r' | b'\n'))
                .count();

            if whitespace > 0 {
                rest = rest.take_split(whitespace).0;
                continue;
            }

            let Ok((next, comment)) = Self::parse_comment(rest.clone()) else {
                break;
            };

            comments.push(comment);
            rest = next;
        }

        let span = input_span.until_rest(&rest.span);

        Ok((
            rest,
            Self {
                _span: span,
                comments,
            },
        ))
    }
}

#[cfg(test)]
mod test {
    use nom::combinator::all_consuming;

    use crate::parser::{ParserState, ast::comment::wsoc::WSoC, input::ParserInput};

    #[test]
    fn parse_wsoc() {
        let test = vec![
            ("", 0),
            ("  \n  ", 0),
            ("   % my comment \n  % Another comment \n    ", 2),
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
