//! This module defines the [UnknownDirective] directive.

use nom::{
    bytes::complete::is_not,
    combinator::recognize,
    sequence::{preceded, separated_pair},
};
use nom_supreme::error::{BaseErrorKind, Expectation};
use strum::IntoEnumIterator;

use crate::parser::{
    ast::{comment::wsoc::WSoC, token::Token, ProgramAST},
    context::{context, ParserContext},
    error::ParserErrorTree,
    input::ParserInput,
    span::Span,
    ParserResult,
};

use super::DirectiveKind;

/// Unknown directive specified by a user
#[derive(Debug)]
pub struct UnknownDirective<'a> {
    /// [ProgramSpan] associated with this node
    span: Span<'a>,

    /// Name of the directive
    name: Token<'a>,
    /// Content
    content: Span<'a>,
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

    /// Parse the name of the directive.
    pub fn parse_unknown(input: ParserInput<'a>) -> ParserResult<'a, Token<'a>> {
        let keyword_parser = |input: ParserInput<'a>| {
            if let Ok((rest, matched)) = Token::name(input.clone()) {
                let mut is_known = false;

                for directive in DirectiveKind::iter().map(|kind| kind.token()).flatten() {
                    if matched.to_string() == directive.name() {
                        is_known = true;
                        break;
                    }
                }

                if !is_known {
                    return Ok((rest, matched));
                }
            }

            Err(nom::Err::Error(ParserErrorTree::Base {
                location: input,
                kind: BaseErrorKind::Expected(Expectation::Tag("known directive")),
            }))
        };

        keyword_parser(input)
    }
}

const CONTEXT: ParserContext = ParserContext::UnknownDirective;

impl<'a> ProgramAST<'a> for UnknownDirective<'a> {
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
            separated_pair(
                preceded(Token::directive_indicator, Self::parse_unknown),
                WSoC::parse,
                // FIXME: Rework error recovery, because this recognises an `.` in an IRI,
                //   e.g. in `@baseerror <https://example.org/>
                //                                       ^
                //   That means that content == "<https://example", and rest == ".org/>" which
                //   will also produce an error.
                // NOTE: Maybe we could try to parse the "body" of the other directives and if
                //   one succeeds give a hint what directive could be the correct.
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

        let known_directives = vec![
            "@base something",
            "@declare something",
            "@import something",
            "@export something",
            "@prefix something",
            "@output something",
        ];

        for input in known_directives {
            let parser_input = ParserInput::new(input, ParserState::default());
            let result = all_consuming(UnknownDirective::parse)(parser_input);

            assert!(result.is_err());
        }
    }

    #[test]
    fn error_recovery() {
        let test = [(
            "@test <https://example.org> .",
            ("test", "<https://example.org> "),
        )];

        for (input, expected) in test {
            let parser_input = ParserInput::new(input, ParserState::default());
            let result = UnknownDirective::parse(parser_input);

            assert!(result.is_ok());

            let result = result.unwrap();
            assert_eq!(
                expected,
                (result.1.name().as_ref(), result.1.content().as_ref())
            );
        }
    }
}
