//! This module defines [KeyValueSequence].

use std::vec::IntoIter;

use nom::{
    multi::separated_list0,
    sequence::{separated_pair, tuple},
};

use crate::parser::{
    ast::{comment::wsoc::WSoC, expression::Expression, token::Token, ProgramAST},
    input::ParserInput,
    span::Span,
    ParserResult,
};

/// Sequence of comma-delimited expressions
#[derive(Debug)]
pub struct KeyValueSequence<'a> {
    /// [ProgramSpan] associated with this sequence
    _span: Span<'a>,

    /// List of key-value pairs
    expressions: Vec<(Expression<'a>, Expression<'a>)>,
}

impl<'a> KeyValueSequence<'a> {
    /// Return an iterator over the [Expression] pairs.
    pub fn iter(&self) -> impl Iterator<Item = &(Expression<'a>, Expression<'a>)> {
        self.into_iter()
    }

    /// Parse a single key-value pair
    fn parse_key_value_pair(
        input: ParserInput<'a>,
    ) -> ParserResult<'a, (Expression<'a>, Expression<'a>)> {
        separated_pair(
            Expression::parse,
            tuple((WSoC::parse, Token::colon, WSoC::parse)),
            Expression::parse,
        )(input)
    }

    /// Parse a comma separated list of [Expression]s.
    pub fn parse(input: ParserInput<'a>) -> ParserResult<'a, Self> {
        let input_span = input.span;

        separated_list0(
            tuple((WSoC::parse, Token::comma, WSoC::parse)),
            Self::parse_key_value_pair,
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

impl<'a, 'b> IntoIterator for &'b KeyValueSequence<'a> {
    type Item = &'b (Expression<'a>, Expression<'a>);
    type IntoIter = std::slice::Iter<'b, (Expression<'a>, Expression<'a>)>;

    fn into_iter(self) -> Self::IntoIter {
        self.expressions.iter()
    }
}

impl<'a> IntoIterator for KeyValueSequence<'a> {
    type Item = (Expression<'a>, Expression<'a>);
    type IntoIter = IntoIter<(Expression<'a>, Expression<'a>)>;

    fn into_iter(self) -> Self::IntoIter {
        self.expressions.into_iter()
    }
}

#[cfg(test)]
mod test {
    use nom::combinator::all_consuming;

    use crate::parser::{
        ast::sequence::key_value::KeyValueSequence, input::ParserInput, ParserState,
    };

    #[test]
    fn parse_expression_sequence_simple() {
        let test = vec![
            ("", 0),
            ("?x:3", 1),
            ("?x: 7, ?y: ?z, ?z: 1", 3),
            ("x:3,     ?x:12, ?x :    7", 3),
            ("x:3, ?x : 2, 2 : 5", 3),
            ("x:3  ,   ?x     : 12,   2:  1", 3),
            ("x:POW(1,2)", 1),
        ];

        for (input, expected) in test {
            let parser_input = ParserInput::new(input, ParserState::default());
            let result = all_consuming(KeyValueSequence::parse)(parser_input);

            assert!(result.is_ok());

            let result = result.unwrap();
            assert_eq!(expected, result.1.into_iter().count());
        }
    }
}
