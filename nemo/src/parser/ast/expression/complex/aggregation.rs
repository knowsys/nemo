//! This module defines [Aggregation].

use nom::{
    combinator::opt,
    sequence::{delimited, pair, preceded, tuple},
};

use crate::{
    parser::{
        ParserResult,
        ast::{
            ProgramAST, comment::wsoc::WSoC, expression::Expression,
            sequence::simple::ExpressionSequenceSimple, tag::aggregation::AggregationTag,
            token::Token,
        },
        context::{ParserContext, context},
        input::ParserInput,
        span::Span,
    },
    rule_model::components::term::aggregate::AggregateKind,
};

/// A known Aggregation applied to a series of [Expression]s.
#[derive(Debug)]
pub struct Aggregation<'a> {
    /// [Span] associated with this node
    span: Span<'a>,

    /// Type of Aggregation,
    tag: AggregationTag<'a>,
    /// Aggregate expression
    aggregate: Box<Expression<'a>>,
    /// List of distinct variables
    distinct: Option<ExpressionSequenceSimple<'a>>,
}

impl<'a> Aggregation<'a> {
    /// Return the aggregate expression
    pub fn aggregate(&self) -> &Expression<'a> {
        &self.aggregate
    }

    /// Return the tag that specifies the aggregate operation.
    pub fn tag(&self) -> &AggregationTag<'a> {
        &self.tag
    }

    /// Return the expressions specifying the distinct variables
    pub fn distinct(&self) -> impl Iterator<Item = &Expression<'a>> {
        self.distinct.iter().flat_map(|distinct| distinct.iter())
    }

    /// Return the type of this Aggregation.
    pub fn kind(&self) -> Option<AggregateKind> {
        self.tag.operation()
    }
}

const CONTEXT: ParserContext = ParserContext::Aggregation;

impl<'a> ProgramAST<'a> for Aggregation<'a> {
    fn children(&self) -> Vec<&dyn ProgramAST<'a>> {
        let mut result: Vec<&dyn ProgramAST> = vec![];
        result.push(&self.tag);
        result.push(&*self.aggregate);

        for expression in self.distinct() {
            result.push(expression)
        }

        result
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
            pair(
                preceded(Token::aggregate_indicator, AggregationTag::parse),
                delimited(
                    pair(Token::aggregate_open, WSoC::parse),
                    pair(
                        Expression::parse,
                        opt(preceded(
                            tuple((
                                WSoC::parse,
                                Token::aggregate_distinct_separator,
                                WSoC::parse,
                            )),
                            ExpressionSequenceSimple::parse,
                        )),
                    ),
                    pair(WSoC::parse, Token::aggregate_close),
                ),
            ),
        )(input)
        .map(|(rest, (tag, (aggregate, distinct)))| {
            let rest_span = rest.span;

            (
                rest,
                Self {
                    span: input_span.until_rest(&rest_span),
                    tag,
                    aggregate: Box::new(aggregate),
                    distinct,
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
            ast::{ProgramAST, expression::complex::aggregation::Aggregation},
            input::ParserInput,
        },
        rule_model::components::term::aggregate::AggregateKind,
    };

    #[test]
    fn parse_aggregation() {
        let test = vec![
            ("#sum(?x)", (AggregateKind::SumOfNumbers, 0)),
            ("#max(?x, ?y, ?z)", (AggregateKind::MaxNumber, 2)),
        ];

        for (input, expected) in test {
            let parser_input = ParserInput::new(input, ParserState::default());
            let result = all_consuming(Aggregation::parse)(parser_input);

            assert!(result.is_ok());

            let result = result.unwrap();
            assert_eq!(
                expected,
                (result.1.kind().unwrap(), result.1.distinct().count())
            );
        }
    }
}
