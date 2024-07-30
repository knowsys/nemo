//! This module defines the [Output] directive.

use nom::sequence::{preceded, tuple};

use crate::parser::{
    ast::{
        comment::wsoc::WSoC, sequence::Sequence, tag::structure::StructureTag, token::Token,
        ProgramAST,
    },
    context::{context, ParserContext},
    input::ParserInput,
    span::Span,
    ParserResult,
};

/// Output directive
#[derive(Debug)]
pub struct Output<'a> {
    /// [ProgramSpan] associated with this node
    span: Span<'a>,

    /// A sequence of predicates
    predicate: Sequence<'a, StructureTag<'a>>,
}

impl<'a> Output<'a> {
    /// Return the output predicate.
    pub fn predicate(&self) -> Vec<&StructureTag<'a>> {
        self.predicate.iter().collect()
    }

    pub fn parse_body(input: ParserInput<'a>) -> ParserResult<'a, Sequence<'a, StructureTag>> {
        Sequence::<StructureTag>::parse(input)
    }
}

const CONTEXT: ParserContext = ParserContext::Output;

impl<'a> ProgramAST<'a> for Output<'a> {
    fn children(&self) -> Vec<&dyn ProgramAST> {
        vec![&self.predicate]
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
                    Token::directive_output,
                    WSoC::parse,
                )),
                Self::parse_body,
            ),
        )(input)
        .map(|(rest, predicate)| {
            let rest_span = rest.span;

            (
                rest,
                Self {
                    span: input_span.until_rest(&rest_span),
                    predicate,
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
        ast::{directive::output::Output, ProgramAST},
        input::ParserInput,
        ParserState,
    };

    #[test]
    fn parse_attribute() {
        let test = vec![("@output test", "test".to_string())];

        for (input, expected) in test {
            let parser_input = ParserInput::new(input, ParserState::default());
            let result = all_consuming(Output::parse)(parser_input);

            assert!(result.is_ok());

            let result = result.unwrap();
            assert_eq!(expected, result.1.predicate()[0].to_string());
        }
    }
}