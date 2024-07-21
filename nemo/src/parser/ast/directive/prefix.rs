//! This module defines the [Prefix] directive.

use nom::sequence::{preceded, separated_pair, tuple};

use crate::parser::{
    ast::{comment::wsoc::WSoC, expression::basic::iri::Iri, token::Token, ProgramAST},
    context::{context, ParserContext},
    input::ParserInput,
    span::ProgramSpan,
    ParserResult,
};

/// Prefix directive
#[derive(Debug)]
pub struct Prefix<'a> {
    /// [ProgramSpan] associated with this node
    span: ProgramSpan<'a>,

    /// The prefix
    prefix: Token<'a>,
    /// Its value
    value: Iri<'a>,
}

impl<'a> Prefix<'a> {
    /// Return the defined prefix.
    pub fn prefix(&self) -> String {
        self.prefix.to_string()
    }

    /// Return the value of the prefix.
    pub fn value(&self) -> &Iri<'a> {
        &self.value
    }
}

const CONTEXT: ParserContext = ParserContext::Prefix;

impl<'a> ProgramAST<'a> for Prefix<'a> {
    fn children(&self) -> Vec<&dyn ProgramAST> {
        self.value.children()
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
                tuple((
                    Token::at,
                    Token::directive_prefix,
                    WSoC::parse_whitespace_comment,
                    WSoC::parse,
                )),
                separated_pair(
                    Token::name,
                    tuple((WSoC::parse, Token::colon, WSoC::parse)),
                    Iri::parse,
                ),
            ),
        )(input)
        .map(|(rest, (prefix, value))| {
            let rest_span = rest.span;

            (
                rest,
                Self {
                    span: input_span.until_rest(&rest_span),
                    prefix,
                    value,
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
        ast::{directive::prefix::Prefix, ProgramAST},
        input::ParserInput,
        ParserState,
    };

    #[test]
    fn parse_prefix() {
        let test = vec![(
            "@prefix owl: <http://www.w3.org/2002/07/owl#>",
            (
                "owl".to_string(),
                "http://www.w3.org/2002/07/owl#".to_string(),
            ),
        )];

        for (input, expected) in test {
            let parser_input = ParserInput::new(input, ParserState::default());
            let result = all_consuming(Prefix::parse)(parser_input);

            assert!(result.is_ok());

            let result = result.unwrap();
            assert_eq!(expected, (result.1.prefix(), result.1.value().content()));
        }
    }
}
