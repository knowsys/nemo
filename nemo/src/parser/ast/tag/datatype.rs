//! This module defines [DataTypeTag].

use nom::bytes::complete::tag;
use nom_supreme::error::{BaseErrorKind, Expectation};
use strum::IntoEnumIterator;

use crate::{
    parser::{
        ast::ProgramAST,
        context::{context, ParserContext},
        error::ParserErrorTree,
        input::ParserInput,
        span::ProgramSpan,
        ParserResult,
    },
    rule_model::components::datatype::DataType,
};

/// Tags that are used to identify operations
#[derive(Debug)]
pub struct DataTypeTag<'a> {
    /// [ProgramSpan] associated with this node
    span: ProgramSpan<'a>,

    /// Data type
    data_type: DataType,
}

impl<'a> DataTypeTag<'a> {
    /// Return the [DataType] that was parsed.
    pub fn data_type(&self) -> DataType {
        self.data_type
    }
}

const CONTEXT: ParserContext = ParserContext::DataType;

impl<'a> ProgramAST<'a> for DataTypeTag<'a> {
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
        let keyword_parser = |input: ParserInput<'a>| {
            for data_type in DataType::iter() {
                let result =
                    tag::<&str, ParserInput<'_>, ParserErrorTree>(data_type.name())(input.clone());
                if let Ok((rest, _matched)) = result {
                    return Ok((rest, data_type));
                }
            }
            Err(nom::Err::Error(ParserErrorTree::Base {
                location: input,
                kind: BaseErrorKind::Expected(Expectation::Tag("data type")),
            }))
        };

        let input_span = input.span;

        context(CONTEXT, keyword_parser)(input).map(|(rest, data_type)| {
            let rest_span = rest.span;

            (
                rest,
                Self {
                    span: input_span.until_rest(&rest_span),
                    data_type,
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
            ast::{tag::datatype::DataTypeTag, ProgramAST},
            input::ParserInput,
            ParserState,
        },
        rule_model::components::datatype::DataType,
    };

    #[test]
    fn parse_datatype() {
        let test = vec![
            ("int", DataType::Integer),
            ("float", DataType::Float),
            ("double", DataType::Double),
            ("string", DataType::String),
            ("any", DataType::Any),
        ];

        for (input, expected) in test {
            let parser_input = ParserInput::new(input, ParserState::default());
            let result = all_consuming(DataTypeTag::parse)(parser_input);

            assert!(result.is_ok());

            let result = result.unwrap();
            assert_eq!(expected, result.1.data_type());
        }
    }
}
