//! This module defines [ExpressionSequenceSimple].

use std::vec::IntoIter;

use nom::{multi::separated_list1, sequence::tuple};

use crate::parser::{
    ast::{comment::wsoc::WSoC, expression::Expression, token::Token, ProgramAST},
    input::ParserInput,
    span::Span,
    ParserResult,
};

/// Sequence of comma-delimited expressions
#[derive(Debug)]
pub struct ExpressionSequenceSimple<'a> {
    /// [Span] associated with this sequence
    _span: Span<'a>,

    /// List of expressions
    expressions: Vec<Expression<'a>>,
}

impl<'a> ExpressionSequenceSimple<'a> {
    /// Return an iterator over the [Expression]s.
    pub fn iter(&self) -> impl Iterator<Item = &Expression<'a>> {
        self.into_iter()
    }

    /// Parse a comma separated list of [Expression]s.
    pub fn parse(input: ParserInput<'a>) -> ParserResult<'a, Self> {
        let input_span = input.span;

        separated_list1(
            tuple((WSoC::parse, Token::seq_sep, WSoC::parse)),
            Expression::parse,
        )(input)
        .map(|(rest, expressions)| {
            let rest_span = rest.span;

            (
                rest,
                Self {
                    _span: input_span.until_rest(&rest_span),
                    expressions,
                },
            )
        })
    }
}

impl<'a, 'b> IntoIterator for &'b ExpressionSequenceSimple<'a> {
    type Item = &'b Expression<'a>;
    type IntoIter = std::slice::Iter<'b, Expression<'a>>;

    fn into_iter(self) -> Self::IntoIter {
        self.expressions.iter()
    }
}

impl<'a> IntoIterator for ExpressionSequenceSimple<'a> {
    type Item = Expression<'a>;
    type IntoIter = IntoIter<Expression<'a>>;

    fn into_iter(self) -> Self::IntoIter {
        self.expressions.into_iter()
    }
}

#[cfg(test)]
mod test {
    use nom::combinator::all_consuming;

    use crate::parser::{
        ast::sequence::simple::ExpressionSequenceSimple, input::ParserInput, ParserState,
    };

    #[test]
    fn parse_expression_sequence_simple() {
        let test = vec![
            ("12", 1),
            ("1,?x,2", 3),
            ("1,     ?x, 2", 3),
            ("1, ?x, 2", 3),
            ("1  ,   ?x,   2", 3),
        ];

        for (input, expected) in test {
            let parser_input = ParserInput::new(input, ParserState::default());
            let result = all_consuming(ExpressionSequenceSimple::parse)(parser_input);

            assert!(result.is_ok());

            let result = result.unwrap();
            assert_eq!(expected, result.1.into_iter().count());
        }
    }
}
