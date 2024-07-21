//! This module defines [Rule].

use nom::{
    multi::many0,
    sequence::{separated_pair, tuple},
};

use crate::parser::{
    context::{context, ParserContext},
    input::ParserInput,
    span::ProgramSpan,
    ParserResult,
};

use super::{
    attribute::Attribute, comment::wsoc::WSoC, expression::Expression,
    sequence::simple::ExpressionSequenceSimple, token::Token, ProgramAST,
};

/// A rule describing a logical implication
#[derive(Debug)]
pub struct Rule<'a> {
    /// [ProgramSpan] associated with this node
    span: ProgramSpan<'a>,

    /// Attributes attached to this rule
    attributes: Vec<Attribute<'a>>,

    /// Head of the rule
    head: ExpressionSequenceSimple<'a>,
    /// Body of the rule,
    body: ExpressionSequenceSimple<'a>,
}

impl<'a> Rule<'a> {
    /// Return an iterator of the [Expression]s contained in the head.
    pub fn head(&self) -> impl Iterator<Item = &Expression<'a>> {
        self.head.iter()
    }

    /// Return an iterator of the [Expression]s contained in the body.
    pub fn body(&self) -> impl Iterator<Item = &Expression<'a>> {
        self.body.iter()
    }

    /// Return an iterator over the [Attribute]s attached to this rule.
    pub fn attributes(&self) -> impl Iterator<Item = &Attribute<'a>> {
        self.attributes.iter()
    }
}

const CONTEXT: ParserContext = ParserContext::Rule;

impl<'a> ProgramAST<'a> for Rule<'a> {
    fn children(&self) -> Vec<&dyn ProgramAST> {
        let mut result = Vec::<&dyn ProgramAST>::new();

        for expression in self.head().chain(self.body()) {
            result.push(expression);
        }

        result
    }

    fn span(&self) -> ProgramSpan<'a> {
        self.span
    }

    fn parse(input: ParserInput<'a>) -> ParserResult<'a, Self>
    where
        Self: Sized + 'a,
    {
        let input_span = input.span;

        context(
            CONTEXT,
            tuple((
                many0(Attribute::parse),
                (separated_pair(
                    ExpressionSequenceSimple::parse,
                    tuple((WSoC::parse, Token::arrow, WSoC::parse)),
                    ExpressionSequenceSimple::parse,
                )),
            )),
        )(input)
        .map(|(rest, (attributes, (head, body)))| {
            let rest_span = rest.span;

            (
                rest,
                Self {
                    span: input_span.until_rest(&rest_span),
                    attributes,
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
