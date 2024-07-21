//! This module defines [DeclareSequence].

use std::vec::IntoIter;

use nom::{
    multi::separated_list1,
    sequence::{separated_pair, tuple},
};

use crate::parser::{
    ast::{
        comment::wsoc::WSoC,
        tag::{datatype::DataTypeTag, parameter::ParameterName},
        token::Token,
        ProgramAST,
    },
    input::ParserInput,
    span::ProgramSpan,
    ParserResult,
};

/// Sequence of name-type declarations
#[derive(Debug)]
pub struct DeclareSequence<'a> {
    /// [ProgramSpan] associated with this sequence
    _span: ProgramSpan<'a>,

    /// List of name-type pairs
    pairs: Vec<(ParameterName<'a>, DataTypeTag<'a>)>,
}

impl<'a> DeclareSequence<'a> {
    /// Return an iterator over the name-type pairs.
    pub fn iter(&self) -> impl Iterator<Item = &(ParameterName<'a>, DataTypeTag<'a>)> {
        self.into_iter()
    }

    /// Parse a single name-type pair
    fn parse_name_type_pair(
        input: ParserInput<'a>,
    ) -> ParserResult<'a, (ParameterName<'a>, DataTypeTag<'a>)> {
        separated_pair(
            ParameterName::parse,
            tuple((WSoC::parse, Token::colon, WSoC::parse)),
            DataTypeTag::parse,
        )(input)
    }

    /// Parse a comma separated list of [Expression]s.
    pub fn parse(input: ParserInput<'a>) -> ParserResult<'a, Self> {
        let input_span = input.span;

        separated_list1(
            tuple((WSoC::parse, Token::comma, WSoC::parse)),
            Self::parse_name_type_pair,
        )(input)
        .map(|(rest, pairs)| {
            let rest_span = rest.span;

            (
                rest,
                Self {
                    _span: input_span.until_rest(&rest_span),
                    pairs,
                },
            )
        })
    }
}

impl<'a, 'b> IntoIterator for &'b DeclareSequence<'a> {
    type Item = &'b (ParameterName<'a>, DataTypeTag<'a>);
    type IntoIter = std::slice::Iter<'b, (ParameterName<'a>, DataTypeTag<'a>)>;

    fn into_iter(self) -> Self::IntoIter {
        self.pairs.iter()
    }
}

impl<'a> IntoIterator for DeclareSequence<'a> {
    type Item = (ParameterName<'a>, DataTypeTag<'a>);
    type IntoIter = IntoIter<(ParameterName<'a>, DataTypeTag<'a>)>;

    fn into_iter(self) -> Self::IntoIter {
        self.pairs.into_iter()
    }
}

#[cfg(test)]
mod test {
    use nom::combinator::all_consuming;

    use crate::{
        parser::{
            ast::{sequence::declare::DeclareSequence, tag::parameter::Parameter},
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
            let result = all_consuming(DeclareSequence::parse)(parser_input);

            assert!(result.is_ok());

            let result = result.unwrap();
            assert_eq!(
                expected,
                result
                    .1
                    .into_iter()
                    .map(|(name, datatype)| (name.parameter().clone(), datatype.data_type()))
                    .collect::<Vec<_>>()
            );
        }
    }
}
