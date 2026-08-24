//! This module defines [Rule].

use nom::sequence::tuple;

use crate::parser::{
    ParserResult,
    context::{ParserContext, context},
    error::ParserErrors,
    input::ParserInput,
    span::Span,
};

use super::{ProgramAST, comment::wsoc::WSoC, guard::Guard, sequence::Sequence, token::Token};

/// A rule describing a logical implication
#[derive(Debug)]
pub struct Rule<'a> {
    /// [Span] associated with this node
    span: Span<'a>,

    /// Head of the rule
    head: Sequence<'a, Guard<'a>>,
    /// Body of the rule,
    body: Sequence<'a, Guard<'a>>,
}

/// What a statement beginning with a [Guard] turned out to be.
#[derive(Debug)]
pub(crate) enum RuleOrFact<'a> {
    /// A rule
    Rule(Rule<'a>),
    /// A guard that no rule arrow followed
    Fact(Guard<'a>),
}

impl<'a> Rule<'a> {
    /// Create a new [Rule] from parts that have already been parsed.
    pub(crate) fn new(
        span: Span<'a>,
        head: Sequence<'a, Guard<'a>>,
        body: Sequence<'a, Guard<'a>>,
    ) -> Self {
        Self { span, head, body }
    }

    /// Continue parsing a rule whose first head guard has already been parsed.
    pub(crate) fn parse_continued(
        first: Guard<'a>,
        input_span: Span<'a>,
        input: ParserInput<'a>,
    ) -> ParserResult<'a, RuleOrFact<'a>> {
        let Ok((rest_head, mut head)) = Sequence::<Guard>::parse_continued(input.clone()) else {
            return Ok((input, RuleOrFact::Fact(first)));
        };

        let arrow = tuple((WSoC::parse, Token::rule_arrow, WSoC::parse))(rest_head);
        let Ok((rest_arrow, _)) = arrow else {
            return Ok((input, RuleOrFact::Fact(first)));
        };

        let (rest, body) = Sequence::<Guard>::parse(rest_arrow)?;
        let span = input_span.until_rest(&rest.span);

        head.insert(0, first);

        Ok((
            rest,
            RuleOrFact::Rule(Self::new(span, Sequence::new(span, head), body)),
        ))
    }

    /// Return an iterator of the [Guard]s contained in the head.
    pub fn head(&self) -> impl Iterator<Item = &Guard<'a>> {
        self.head.iter()
    }

    /// Return an iterator of the [Guard]s contained in the body.
    pub fn body(&self) -> impl Iterator<Item = &Guard<'a>> {
        self.body.iter()
    }
}

const CONTEXT: ParserContext = ParserContext::Rule;

impl<'a> ProgramAST<'a> for Rule<'a> {
    fn children(&self) -> Vec<&dyn ProgramAST<'a>> {
        let mut result = Vec::<&dyn ProgramAST>::new();

        for expression in self.head().chain(self.body()) {
            result.push(expression);
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

        let parse_rule = |input: ParserInput<'a>| {
            let (rest, first) = Guard::parse(input)?;

            match Self::parse_continued(first, input_span, rest)? {
                (rest, RuleOrFact::Rule(rule)) => Ok((rest, rule)),
                (rest, RuleOrFact::Fact(_)) => Err(nom::Err::Error(ParserErrors::at(rest.span))),
            }
        };

        context(CONTEXT, parse_rule)(input)
    }

    fn context(&self) -> ParserContext {
        CONTEXT
    }
}

#[cfg(test)]
mod test {
    use nom::combinator::all_consuming;

    use crate::parser::{
        ParserState,
        ast::{ProgramAST, rule::Rule},
        input::ParserInput,
    };

    #[test]
    #[cfg_attr(miri, ignore)]
    fn parse_rule() {
        let test = vec![
            ("a(?x, ?y) :- b(?x, ?y)", (1, 1)),
            ("a(?x,?y), d(1), c(1) :- b(?x, ?y), c(1, 2)", (3, 2)),
            ("result(?x) :- test(?x)", (1, 1)),
        ];

        for (input, expected) in test {
            let parser_input = ParserInput::new(input, ParserState::default());
            let result = all_consuming(Rule::parse)(parser_input);

            assert!(result.is_ok());

            let result = result.unwrap();
            assert_eq!(expected, (result.1.head().count(), result.1.body().count()));
        }
    }
}
