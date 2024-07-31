//! This module defines [OperationTag].

use nom::bytes::complete::tag_no_case;
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
    rule_model::components::term::operation::operation_kind::OperationKind,
};

/// Tags that are used to identify operations
#[derive(Debug)]
pub struct OperationTag<'a> {
    /// [Span] associated with this node
    span: Span<'a>,

    /// Type of operation
    kind: OperationKind,
}

impl<'a> OperationTag<'a> {
    /// Return the [OperationKind] that was parsed.
    pub fn operation(&self) -> OperationKind {
        self.kind
    }
}

const CONTEXT: ParserContext = ParserContext::OperationTag;

impl<'a> ProgramAST<'a> for OperationTag<'a> {
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
        let keyword_parser = |input: ParserInput<'a>| {
            for operation in OperationKind::iter() {
                let result = tag_no_case::<&str, ParserInput<'_>, ParserErrorTree>(
                    operation.name(),
                )(input.clone());
                if let Ok((rest, _matched)) = result {
                    return Ok((rest, operation));
                }
            }
            Err(nom::Err::Error(ParserErrorTree::Base {
                location: input,
                kind: BaseErrorKind::Expected(Expectation::Tag("operation name")),
            }))
        };

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
            ast::{tag::operation::OperationTag, ProgramAST},
            input::ParserInput,
            ParserState,
        },
        rule_model::components::term::operation::operation_kind::OperationKind,
    };

    #[test]
    fn parse_tag() {
        let test = vec![
            ("sum", OperationKind::NumericSum),
            ("STRLEN", OperationKind::StringLength),
            ("IsNumeric", OperationKind::CheckIsNumeric),
        ];

        for (input, expected) in test {
            let parser_input = ParserInput::new(input, ParserState::default());
            let result = all_consuming(OperationTag::parse)(parser_input);

            assert!(result.is_ok());

            let result = result.unwrap();
            assert_eq!(expected, result.1.kind);
        }
    }
}
