//! This module defines [OperationTag].

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
    rule_model::components::term::operation::operation_kind::OperationKind,
};

/// All [OperationKind]s
static OPERATIONS: LazyLock<KeywordTable<OperationKind>> = LazyLock::new(|| {
    KeywordTable::new_ignore_ascii_case(
        OperationKind::iter().map(|operation| (operation.name(), operation)),
    )
});

/// Tags that are used to identify operations
#[derive(Debug)]
pub struct OperationTag<'a> {
    /// [Span] associated with this node
    span: Span<'a>,

    /// Type of operation
    kind: OperationKind,
}

impl OperationTag<'_> {
    /// Return the [OperationKind] that was parsed.
    pub fn operation(&self) -> OperationKind {
        self.kind
    }
}

const CONTEXT: ParserContext = ParserContext::OperationTag;

impl<'a> ProgramAST<'a> for OperationTag<'a> {
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
        let keyword_parser = |input: ParserInput<'a>| parse_keyword(input, &OPERATIONS);

        let input_span = input.span;

        context(CONTEXT, keyword_parser)(input).map(|(rest, kind)| {
            let rest_span = rest.span;

            (
                rest,
                Self {
                    span: input_span.until_rest(&rest_span),
                    kind,
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
            ast::{ProgramAST, tag::operation::OperationTag},
            input::ParserInput,
        },
        rule_model::components::term::operation::operation_kind::OperationKind,
    };

    #[test]
    fn parse_tag() {
        let test = vec![
            ("sum", OperationKind::NumericSum),
            ("STRLEN", OperationKind::StringLength),
            ("IsNumeric", OperationKind::CheckIsNumeric),
            ("STR", OperationKind::LexicalValue),
            ("NUMGREATER", OperationKind::NumericGreaterthan),
            ("NUMGREATEREQ", OperationKind::NumericGreaterthaneq),
        ];

        for (input, expected) in test {
            let parser_input = ParserInput::new(input, ParserState::default());
            let result = all_consuming(OperationTag::parse)(parser_input);

            assert!(result.is_ok(), "`{input}` should parse as an operation tag");

            let result = result.unwrap();
            assert_eq!(expected, result.1.kind);
        }
    }

    #[test]
    fn reject_names_that_merely_start_with_an_operation() {
        for input in ["SUMX", "STRX", "STRLENGTH", "sum_", "count2"] {
            let parser_input = ParserInput::new(input, ParserState::default());

            assert!(
                all_consuming(OperationTag::parse)(parser_input).is_err(),
                "`{input}` should not parse as an operation tag"
            );
        }
    }
}
