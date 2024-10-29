//! This module defines [ParameterName].

use nom::{branch::alt, combinator::map};

use crate::parser::{
    ast::{token::Token, ProgramAST},
    context::{context, ParserContext},
    input::ParserInput,
    span::Span,
    ParserResult,
};

/// Type of parameter
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Parameter {
    /// Unnamed parameter
    Unnamed,
    /// Named parameter
    Named(String),
}

/// Tags that are used to give names to certain objects
#[derive(Debug, Clone)]
pub struct ParameterName<'a> {
    /// [Span] associated with this node
    span: Span<'a>,

    /// Parameter
    parameter: Parameter,
}

impl ParameterName<'_> {
    /// Return the [Parameter] that was parsed.
    pub fn parameter(&self) -> &Parameter {
        &self.parameter
    }
}

const CONTEXT: ParserContext = ParserContext::DataType;

impl<'a> ProgramAST<'a> for ParameterName<'a> {
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
            alt((
                map(Token::underscore, |_| Parameter::Unnamed),
                map(Token::name, |token| Parameter::Named(token.to_string())),
            )),
        )(input)
        .map(|(rest, parameter)| {
            let rest_span = rest.span;

            (
                rest,
                Self {
                    span: input_span.until_rest(&rest_span),
                    parameter,
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
        ast::{tag::parameter::ParameterName, ProgramAST},
        input::ParserInput,
        ParserState,
    };

    use super::Parameter;

    #[test]
    fn parse_datatype() {
        let test = vec![
            ("test", Parameter::Named("test".to_string())),
            ("_", Parameter::Unnamed),
        ];

        for (input, expected) in test {
            let parser_input = ParserInput::new(input, ParserState::default());
            let result = all_consuming(ParameterName::parse)(parser_input);

            assert!(result.is_ok());

            let result = result.unwrap();
            assert_eq!(&expected, result.1.parameter());
        }
    }
}
