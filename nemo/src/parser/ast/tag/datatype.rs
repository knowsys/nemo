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
        span::Span,
        ParserResult,
    },
    rule_model::components::import_export::io_type::IOType,
};

/// Tags that are used to identify operations
#[derive(Debug, Clone)]
pub struct DataTypeTag<'a> {
    /// [Span] associated with this node
    span: Span<'a>,

    /// Data type
    data_type: IOType,
}

impl DataTypeTag<'_> {
    /// Return the [IOType] that was parsed.
    pub fn data_type(&self) -> IOType {
        self.data_type
    }
}

const CONTEXT: ParserContext = ParserContext::DataType;

impl<'a> ProgramAST<'a> for DataTypeTag<'a> {
    fn children(&self) -> Vec<&dyn ProgramAST<'a>> {
        Vec::default()
    }

    fn span(&self) -> Span<'a> {
        self.span
    }

    fn parse(input: ParserInput<'a>) -> ParserResult<'a, Self>
    where
        Self: Sized + 'a,
    {
        let keyword_parser = |input: ParserInput<'a>| {
            for data_type in IOType::iter() {
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
        rule_model::components::import_export::io_type::IOType,
    };

    #[test]
    fn parse_datatype() {
        let test = vec![
            ("int", IOType::Integer),
            ("float", IOType::Float),
            ("double", IOType::Double),
            ("string", IOType::String),
            ("any", IOType::Any),
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
