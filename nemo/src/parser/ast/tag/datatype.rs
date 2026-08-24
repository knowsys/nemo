//! This module defines [DataTypeTag].

use std::sync::LazyLock;

use strum::IntoEnumIterator;

use crate::{
    parser::{
        ParserResult,
        ast::{
            ProgramAST,
            tag::{KeywordTable, parse_keyword},
        },
        context::{ParserContext, context},
        input::ParserInput,
        span::Span,
    },
    rule_model::components::import_export::io_type::IOType,
};

/// All [IOType]s
static DATA_TYPES: LazyLock<KeywordTable<IOType>> = LazyLock::new(|| {
    KeywordTable::new(IOType::iter().map(|data_type| (data_type.name(), data_type)))
});

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
        let keyword_parser = |input: ParserInput<'a>| parse_keyword(input, &DATA_TYPES);

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
            ParserState,
            ast::{ProgramAST, tag::datatype::DataTypeTag},
            input::ParserInput,
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
