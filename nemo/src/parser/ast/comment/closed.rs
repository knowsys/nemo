//! This module defines [ClosedComment].

use nom::{bytes::complete::take_until, sequence::delimited};

use crate::parser::{
    ast::{
        token::{Token, TokenKind},
        ProgramAST,
    },
    context::{context, ParserContext},
    input::ParserInput,
    span::Span,
    ParserResult,
};

/// Closed comment
#[derive(Debug)]
pub struct ClosedComment<'a> {
    /// [Span] associated with this comment
    span: Span<'a>,

    /// Part of the comment that contains the content
    content: Span<'a>,
}

const CONTEXT: ParserContext = ParserContext::Comment;

impl ClosedComment<'_> {
    // NOTE: Should this return a &str, so that the consumer can decide whether to turn it into an
    //   owned value or not?
    /// Return the content of the comment
    pub fn content(&self) -> String {
        self.content.fragment().to_string()
    }
}

impl<'a> ProgramAST<'a> for ClosedComment<'a> {
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
            delimited(
                // NOTE: With this, nested comments are not allowed (won't get parsed
                //   correctly).
                Token::open_comment,
                take_until(TokenKind::CloseComment.name()),
                Token::close_comment,
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
        ast::{comment::closed::ClosedComment, ProgramAST},
        input::ParserInput,
        ParserState,
    };

    #[test]
    fn parse_closed_comment() {
        let test = vec![
            ("/*my comment*/", "my comment".to_string()),
            ("/* my comment */", " my comment ".to_string()),
        ];

        for (input, expected) in test {
            let parser_input = ParserInput::new(input, ParserState::default());
            let result = all_consuming(ClosedComment::parse)(parser_input);

            assert!(result.is_ok());

            let result = result.unwrap();
            assert_eq!(expected, result.1.content());
        }
    }
}
