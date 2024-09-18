//! This module defines [LineComment].

use nom::{
    branch::alt,
    character::complete::{line_ending, not_line_ending},
    combinator::eof,
    sequence::tuple,
};

use crate::parser::{
    ast::{token::Token, ProgramAST},
    context::{context, ParserContext},
    input::ParserInput,
    span::Span,
    ParserResult,
};

/// Line comment
#[derive(Debug)]
pub struct LineComment<'a> {
    /// [Span] associated with this comment
    span: Span<'a>,

    /// Part of the comment that contains the content
    content: Span<'a>,
}

const CONTEXT: ParserContext = ParserContext::Comment;

impl<'a> LineComment<'a> {
    /// Return the content of the comment
    pub fn content(&self) -> String {
        self.content.fragment().to_string()
    }
}

impl<'a> ProgramAST<'a> for LineComment<'a> {
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
            tuple((Token::comment, not_line_ending, alt((line_ending, eof)))),
        )(input)
        .map(|(rest, (_, content, _))| {
            let rest_span = rest.span;

            (
                rest,
                Self {
                    span: input_span.until_rest(&rest_span),
                    content: content.span,
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
        ast::{comment::line::LineComment, ProgramAST},
        input::ParserInput,
        ParserState,
    };

    #[test]
    fn parse_line_comment() {
        let test = vec![
            ("% my comment", " my comment".to_string()),
            ("%my comment", "my comment".to_string()),
            ("%  \tmy comment\n", "  \tmy comment".to_string()),
            ("%%%% my comment", " my comment".to_string()),
        ];

        for (input, expected) in test {
            let parser_input = ParserInput::new(input, ParserState::default());
            let result = all_consuming(LineComment::parse)(parser_input);
            dbg!(&result);

            assert!(result.is_ok());

            let result = result.unwrap();
            assert_eq!(expected, result.1.content());
        }
    }
}
