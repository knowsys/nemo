//! This module defines [Rule].

use nom::sequence::{separated_pair, tuple};

use crate::parser::{
    context::{context, ParserContext},
    input::ParserInput,
    span::Span,
    ParserResult,
};

use super::{comment::wsoc::WSoC, guard::Guard, sequence::Sequence, token::Token, ProgramAST};

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

impl<'a> Rule<'a> {
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

        context(
            CONTEXT,
            separated_pair(
                Sequence::<Guard>::parse,
                tuple((WSoC::parse, Token::rule_arrow, WSoC::parse)),
                Sequence::<Guard>::parse,
            ),
        )(input)
        .map(|(rest, (head, body))| {
            let rest_span = rest.span;

            (
                rest,
                Self {
                    span: input_span.until_rest(&rest_span),
                    head,
                    body,
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

    use crate::parser::{
        ast::{rule::Rule, ProgramAST},
        input::ParserInput,
        ParserState,
    };

    #[test]
    fn parse_rule() {
        let test = vec![
            ("a(?x, ?y) :- b(?x, ?y)", (1, 1)),
            ("a(?x,?y), d(1), c(1) :- b(?x, ?y), c(1, 2)", (3, 2)),
            ("#[name(\"test\")]\nresult(?x) :- test(?x)", (1, 1)),
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
