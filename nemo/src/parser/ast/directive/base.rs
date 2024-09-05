//! This module defines the [Base] directive.

use nom::sequence::{preceded, tuple};

use crate::parser::{
    ast::{comment::wsoc::WSoC, expression::basic::iri::Iri, token::Token, ProgramAST},
    context::{context, ParserContext},
    input::ParserInput,
    span::Span,
    ParserResult,
};

/// Base directive, indicating a global prefix
#[derive(Debug)]
pub struct Base<'a> {
    /// [Span] associated with this node
    span: Span<'a>,

    /// The global prefix
    iri: Iri<'a>,
}

impl<'a> Base<'a> {
    /// Return the base iri.
    pub fn iri(&self) -> &Iri<'a> {
        &self.iri
    }

    pub fn parse_body(input: ParserInput<'a>) -> ParserResult<'a, Iri<'a>> {
        Iri::parse(input)
    }
}

const CONTEXT: ParserContext = ParserContext::Base;

impl<'a> ProgramAST<'a> for Base<'a> {
    fn children(&self) -> Vec<&dyn ProgramAST> {
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
                    Token::directive_base,
                    WSoC::parse,
                )),
                Self::parse_body,
            ),
        )(input)
        .map(|(rest, iri)| {
            let rest_span = rest.span;

            (
                rest,
                Self {
                    span: input_span.until_rest(&rest_span),
                    iri,
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
        ast::{directive::base::Base, ProgramAST},
        input::ParserInput,
        ParserState,
    };

    #[test]
    fn parse_base() {
        let test = vec![("@base <test>", "test".to_string())];

        for (input, expected) in test {
            let parser_input = ParserInput::new(input, ParserState::default());
            let result = all_consuming(Base::parse)(parser_input);

            assert!(result.is_ok());

            let result = result.unwrap();
            assert_eq!(expected, result.1.iri().content());
        }
    }
}
