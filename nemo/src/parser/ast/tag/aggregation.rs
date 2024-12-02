//! This module defines [AggregationTag].

use nom::{branch::alt, bytes::complete::tag_no_case, combinator::map};
use nom_supreme::error::{BaseErrorKind, Expectation};
use strum::IntoEnumIterator;

use crate::{
    parser::{
        ast::{token::Token, ProgramAST},
        context::{context, ParserContext},
        error::ParserErrorTree,
        input::ParserInput,
        span::Span,
        ParserResult,
    },
    rule_model::components::term::aggregate::AggregateKind,
};

/// Tags that is used to identify aggregations
#[derive(Debug)]
pub struct AggregationTag<'a> {
    /// [Span] associated with this node
    span: Span<'a>,

    /// Type of aggregation, if known
    kind: Option<AggregateKind>,
}

impl AggregationTag<'_> {
    /// Return the [AggregateKind] that was parsed, if it is known.
    pub fn operation(&self) -> Option<AggregateKind> {
        self.kind
    }

    /// Return a string representation of the content of this tag.
    pub fn content(&self) -> String {
        self.span.fragment().to_string()
    }
}

const CONTEXT: ParserContext = ParserContext::AggregationTag;

impl<'a> ProgramAST<'a> for AggregationTag<'a> {
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
            for operation in AggregateKind::iter() {
                let result = tag_no_case::<&str, ParserInput<'_>, ParserErrorTree>(
                    operation.name(),
                )(input.clone());
                if let Ok((rest, _matched)) = result {
                    return Ok((rest, operation));
                }
            }
            Err(nom::Err::Error(ParserErrorTree::Base {
                location: input,
                kind: BaseErrorKind::Expected(Expectation::Tag("aggregation name")),
            }))
        };

        let input_span = input.span;

        context(
            CONTEXT,
            alt((map(keyword_parser, Some), map(Token::name, |_| None))),
        )(input)
        .map(|(rest, kind)| {
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
            ast::{tag::aggregation::AggregationTag, ProgramAST},
            input::ParserInput,
            ParserState,
        },
        rule_model::components::term::aggregate::AggregateKind,
    };

    #[test]
    fn parse_tag() {
        let test = vec![
            ("sum", AggregateKind::SumOfNumbers),
            ("COUNT", AggregateKind::CountValues),
            ("Min", AggregateKind::MinNumber),
            ("Max", AggregateKind::MaxNumber),
        ];

        for (input, expected) in test {
            let parser_input = ParserInput::new(input, ParserState::default());
            let result = all_consuming(AggregationTag::parse)(parser_input);

            assert!(result.is_ok());

            let result = result.unwrap();
            assert_eq!(Some(expected), result.1.kind);
        }
    }
}
