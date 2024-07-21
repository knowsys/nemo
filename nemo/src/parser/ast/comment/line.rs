//! This module defines [LineComment].

use nom::{
    character::complete::not_line_ending,
    combinator::opt,
    sequence::{pair, preceded},
};

use crate::parser::{
    ast::{token::Token, ProgramAST},
    context::{context, ParserContext},
    input::ParserInput,
    span::ProgramSpan,
    ParserResult,
};

/// Line comment
#[derive(Debug)]
pub struct LineComment<'a> {
    /// [ProgramSpan] associated with this comment
    span: ProgramSpan<'a>,

    /// Part of the comment that contains the content
    content: ProgramSpan<'a>,
}

const CONTEXT: ParserContext = ParserContext::Comment;

impl<'a> LineComment<'a> {
    /// Return the content of the comment
    pub fn content(&self) -> String {
        self.content.0.to_string()
    }
}

impl<'a> ProgramAST<'a> for LineComment<'a> {
    fn children(&self) -> Vec<&dyn ProgramAST> {
        Vec::default()
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
            preceded(
                pair(Token::comment, opt(Token::whitespace)),
                not_line_ending,
            ),
        )(input)
        .map(|(rest, content)| {
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
            ("// my comment", "my comment".to_string()),
            ("//my comment", "my comment".to_string()),
        ];

        for (input, expected) in test {
            let parser_input = ParserInput::new(input, ParserState::default());
            let result = all_consuming(LineComment::parse)(parser_input);

            assert!(result.is_ok());

            let result = result.unwrap();
            assert_eq!(expected, result.1.content());
        }
    }
}
