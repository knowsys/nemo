//! This module defines [DeclareSequence].

use nom::sequence::{separated_pair, tuple};

use crate::parser::{
    ast::{
        comment::wsoc::WSoC,
        tag::{datatype::DataTypeTag, parameter::ParameterName},
        token::Token,
        ProgramAST,
    },
    context::ParserContext,
    input::ParserInput,
    span::Span,
    ParserResult,
};

const CONTEXT: ParserContext = ParserContext::DeclareNameTypePair;

/// A pair of a name and a data type.
#[derive(Debug, Clone)]
pub struct NameTypePair<'a> {
    _span: Span<'a>,
    name: ParameterName<'a>,
    datatype: DataTypeTag<'a>,
}

impl<'a> ProgramAST<'a> for NameTypePair<'a> {
    fn children(&'a self) -> Vec<&'a dyn ProgramAST> {
        vec![&self.name, &self.datatype]
    }

    fn span(&self) -> Span<'a> {
        self._span
    }

    fn parse(input: ParserInput<'a>) -> ParserResult<'a, Self>
    where
        Self: Sized + 'a,
    {
        let input_span = input.span;
        separated_pair(
            ParameterName::parse,
            tuple((WSoC::parse, Token::name_datatype_separator, WSoC::parse)),
            DataTypeTag::parse,
        )(input)
        .map(|(rest, (name, datatype))| {
            let rest_span = rest.span;
            (
                rest,
                NameTypePair {
                    _span: input_span.until_rest(&rest_span),
                    name,
                    datatype,
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

    use crate::{
        parser::{
            ast::{
                sequence::{declare::NameTypePair, Sequence},
                tag::parameter::Parameter,
                ProgramAST,
            },
            input::ParserInput,
            ParserState,
        },
        rule_model::components::datatype::DataType,
    };

    #[test]
    fn parse_expression_sequence_simple() {
        let test = vec![(
            "_, test: string, _: int, name: any",
            vec![
                (Parameter::Named("test".to_string()), DataType::String),
                (Parameter::Unnamed, DataType::Integer),
                (Parameter::Named("name".to_string()), DataType::Any),
            ],
        )];

        for (input, expected) in test {
            let parser_input = ParserInput::new(input, ParserState::default());
            let result = all_consuming(Sequence::<NameTypePair>::parse)(parser_input);

            assert!(result.is_ok());

            let result = result.unwrap();
            assert_eq!(
                expected,
                result
                    .1
                    .into_iter()
                    .map(|NameTypePair { name, datatype, .. }| (
                        name.parameter().clone(),
                        datatype.data_type()
                    ))
                    .collect::<Vec<_>>()
            );
        }
    }
}
