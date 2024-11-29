//! This module defines [DocComment].

use nom::{
    branch::alt,
    character::complete::{line_ending, not_line_ending},
    combinator::eof,
    multi::many1,
    sequence::tuple,
};

use crate::parser::{
    ast::{token::Token, ProgramAST},
    context::{context, ParserContext},
    input::ParserInput,
    span::Span,
    ParserResult,
};

/// Doc comment that is attached to e.g. rules
#[derive(Debug)]
pub struct DocComment<'a> {
    /// [Span] associated with this comment
    span: Span<'a>,

    /// Each line of the comment
    content: Vec<Span<'a>>,
}

impl DocComment<'_> {
    /// Return the content of the comment
    pub fn content(&self) -> Vec<String> {
        self.content
            .iter()
            .map(|comment| comment.fragment().to_string())
            .collect()
    }
}
const CONTEXT: ParserContext = ParserContext::DocComment;

impl<'a> ProgramAST<'a> for DocComment<'a> {
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
            many1(tuple((
                Token::space0,
                Token::doc_comment,
                not_line_ending,
                alt((line_ending, eof)),
            ))),
        )(input)
        .map(|(rest, result)| {
            let rest_span = rest.span;
            let content = result
                .into_iter()
                .map(|(_, _, comment, _)| comment.span)
                .collect();

            (
                rest,
                Self {
                    span: input_span.until_rest(&rest_span),
                    content,
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
        ast::{comment::doc::DocComment, ProgramAST},
        input::ParserInput,
        ParserState,
    };

    #[test]
    fn parse_doc_comment() {
        let test = vec![
            ("%%% my comment", 1),
            ("%%%my comment\r\n%%% my other comment", 2),
            ("%%%my comment\r\n    %%% my other comment", 2),
        ];

        for (input, expected) in test {
            let parser_input = ParserInput::new(input, ParserState::default());
            let result = all_consuming(DocComment::parse)(parser_input);
            dbg!(&result);

            assert!(result.is_ok());

            let result = result.unwrap();
            assert_eq!(expected, result.1.content().len());
        }
    }
}
