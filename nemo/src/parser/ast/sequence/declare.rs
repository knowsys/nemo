//! This module defines [NameTypePair].

use nom::{
    combinator::opt,
    sequence::{pair, preceded, tuple},
};

use crate::parser::{
    ParserResult,
    ast::{
        ProgramAST,
        comment::wsoc::WSoC,
        tag::{datatype::DataTypeTag, parameter::ParameterName},
        token::Token,
    },
    context::ParserContext,
    input::ParserInput,
    span::Span,
};

const CONTEXT: ParserContext = ParserContext::DeclareNameTypePair;

/// A pair of a name and a data type.
#[derive(Debug, Clone)]
pub struct NameTypePair<'a> {
    _span: Span<'a>,
    name: ParameterName<'a>,
    datatype: Option<DataTypeTag<'a>>,
}

impl<'a> ProgramAST<'a> for NameTypePair<'a> {
    fn children(&self) -> Vec<&dyn ProgramAST<'a>> {
        if let Some(datatype) = &self.datatype {
            vec![&self.name, datatype]
        } else {
            vec![&self.name]
        }
    }

    fn span(&self) -> Span<'a> {
        self._span
    }

    fn parse(input: ParserInput<'a>) -> ParserResult<'a, Self>
    where
        Self: Sized + 'a,
    {
        let input_span = input.span;
        pair(
            ParameterName::parse,
            opt(preceded(
                tuple((WSoC::parse, Token::name_datatype_separator, WSoC::parse)),
                DataTypeTag::parse,
            )),
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
            ParserState,
            ast::{
                ProgramAST,
                sequence::{Sequence, declare::NameTypePair},
                tag::parameter::Parameter,
            },
            input::ParserInput,
        },
        rule_model::components::import_export::io_type::IOType,
    };

    #[test]
    fn parse_expression_sequence_simple() {
        let test = vec![(
            "_, test: string, _: int, name:any",
            vec![
                (Parameter::Unnamed, None),
                (Parameter::Named("test".to_string()), Some(IOType::String)),
                (Parameter::Unnamed, Some(IOType::Integer)),
                (Parameter::Named("name".to_string()), Some(IOType::Any)),
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
                        datatype.map(|data| data.data_type())
                    ))
                    .collect::<Vec<_>>()
            );
        }
    }
}
