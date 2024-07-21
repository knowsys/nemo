//! This module defines the [UnknownDirective] directive.

use nom::{
    bytes::complete::is_not,
    combinator::recognize,
    sequence::{pair, preceded, separated_pair},
};

use crate::parser::{
    ast::{comment::wsoc::WSoC, token::Token, ProgramAST},
    context::{context, ParserContext},
    input::ParserInput,
    span::ProgramSpan,
    ParserResult,
};

/// Unknown directive specified by a user
#[derive(Debug)]
pub struct UnknownDirective<'a> {
    /// [ProgramSpan] associated with this node
    span: ProgramSpan<'a>,

    /// Name of the directive
    name: Token<'a>,
    /// Content
    content: ProgramSpan<'a>,
}

impl<'a> UnknownDirective<'a> {
    /// Return the name of the directive.
    pub fn name(&self) -> String {
        self.name.to_string()
    }

    /// Return the content of the directive.
    pub fn content(&self) -> String {
        self.content.0.to_string()
    }
}

const CONTEXT: ParserContext = ParserContext::UnknownDirective;

impl<'a> ProgramAST<'a> for UnknownDirective<'a> {
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
            separated_pair(
                preceded(Token::at, Token::name),
                pair(WSoC::parse_whitespace_comment, WSoC::parse),
                recognize(is_not(".")),
            ),
        )(input)
        .map(|(rest, (name, content))| {
            let rest_span = rest.span;

            (
                rest,
                Self {
                    span: input_span.until_rest(&rest_span),
                    name,
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
        ast::{directive::unknown::UnknownDirective, ProgramAST},
        input::ParserInput,
        ParserState,
    };

    #[test]
    fn parse_unknown_directive() {
        let test = vec![(
            "@test something",
            ("test".to_string(), "something".to_string()),
        )];

        for (input, expected) in test {
            let parser_input = ParserInput::new(input, ParserState::default());
            let result = all_consuming(UnknownDirective::parse)(parser_input);

            assert!(result.is_ok());

            let result = result.unwrap();
            assert_eq!(expected, (result.1.name(), result.1.content()));
        }
    }
}
