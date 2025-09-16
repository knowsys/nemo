//! This module defines the [Prefix] directive.

use nom::{
    branch::alt,
    sequence::{preceded, separated_pair, tuple},
};

use crate::parser::{
    ParserResult,
    ast::{ProgramAST, comment::wsoc::WSoC, expression::basic::iri::Iri, token::Token},
    context::{ParserContext, context},
    input::ParserInput,
    span::Span,
};

/// Prefix directive
#[derive(Debug)]
pub struct Prefix<'a> {
    /// [Span] associated with this node
    span: Span<'a>,

    /// The prefix
    prefix: Token<'a>,
    /// Its value
    iri: Iri<'a>,
}

impl<'a> Prefix<'a> {
    /// Return the defined prefix.
    pub fn prefix(&self) -> String {
        self.prefix.to_string()
    }

    /// Return the [Token] containing the prefix
    pub fn prefix_token(&self) -> &Token<'a> {
        &self.prefix
    }

    /// Return the value of the prefix.
    pub fn iri(&self) -> &Iri<'a> {
        &self.iri
    }

    /// Parse the main part of this directive.
    pub fn parse_body(input: ParserInput<'a>) -> ParserResult<'a, (Token<'a>, Iri<'a>)> {
        context(
            ParserContext::PrefixBody,
            separated_pair(
                alt((Token::name, Token::empty)),
                tuple((WSoC::parse, Token::prefix_assignment, WSoC::parse)),
                Iri::parse,
            ),
        )(input)
    }
}

const CONTEXT: ParserContext = ParserContext::Prefix;

impl<'a> ProgramAST<'a> for Prefix<'a> {
    fn children(&self) -> Vec<&dyn ProgramAST<'a>> {
        vec![&self.iri]
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
            preceded(
                tuple((
                    Token::directive_indicator,
                    Token::directive_prefix,
                    WSoC::parse,
                )),
                Self::parse_body,
            ),
        )(input)
        .map(|(rest, (prefix, value))| {
            let rest_span = rest.span;

            (
                rest,
                Self {
                    span: input_span.until_rest(&rest_span),
                    prefix,
                    iri: value,
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
        ParserState,
        ast::{ProgramAST, directive::prefix::Prefix},
        input::ParserInput,
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
            assert_eq!(expected, (result.1.prefix(), result.1.iri().content()));
        }
    }
}
