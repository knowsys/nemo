//! This module defines helper parsers for sequences of expressions.

pub mod declare;
pub mod key_value;
pub mod one;
pub mod simple;

use std::vec::IntoIter;

use nom::{
    combinator::opt,
    multi::separated_list1,
    sequence::{terminated, tuple},
};

use crate::parser::{
    ParserResult,
    ast::{ProgramAST, comment::wsoc::WSoC, token::Token},
    context::ParserContext,
    input::ParserInput,
    span::Span,
};

const CONTEXT: ParserContext = ParserContext::Sequence;

/// Sequence of comma-delimited AST nodes.
#[derive(Debug, Clone)]
pub struct Sequence<'a, T> {
    /// [Span] associated with this sequence
    _span: Span<'a>,

    /// The elements of the sequence
    elements: Vec<T>,
}

impl<T> Sequence<'_, T> {
    /// Return an iterator over the elements.
    pub fn iter(&self) -> impl Iterator<Item = &T> {
        self.into_iter()
    }
}

impl<'a, T: ProgramAST<'a> + 'a> Sequence<'a, T> {
    /// Parse one element with a trailing [SequenceSeparator](crate::parser::ast::token::TokenKind::SequenceSeparator)
    pub fn parse_first_trailing(input: ParserInput<'a>) -> ParserResult<'a, Self> {
        let input_span = input.span;
        tuple((T::parse, WSoC::parse, Token::seq_sep))(input).map(|(rest, (t, _, _))| {
            let rest_span = rest.span;
            (
                rest,
                Self {
                    _span: input_span.until_rest(&rest_span),
                    elements: vec![t],
                },
            )
        })
    }

    /// Parse a sequence, where the first element must have a trailing separator and after
    /// that zero or more elements.
    pub fn parse_with_first_trailing(input: ParserInput<'a>) -> ParserResult<'a, Self> {
        let input_span = input.span;
        tuple((Self::parse_first_trailing, WSoC::parse, Self::parse))(input).map(
            |(rest, (first, _, rest_seq))| {
                let rest_span = rest.span;
                let mut first_vec = first.elements;
                let mut rest_vec = rest_seq.elements;
                first_vec.append(&mut rest_vec);
                (
                    rest,
                    Self {
                        _span: input_span.until_rest(&rest_span),
                        elements: first_vec,
                    },
                )
            },
        )
    }

    /// The same as [Self::parse], but it must return at least one element.
    pub fn parse1(input: ParserInput<'a>) -> ParserResult<'a, Self> {
        let input_span = input.span;
        terminated(
            separated_list1(tuple((WSoC::parse, Token::seq_sep, WSoC::parse)), T::parse),
            opt(tuple((WSoC::parse, Token::seq_sep, WSoC::parse))),
        )(input)
        .map(|(rest, vec)| {
            let rest_span = rest.span;
            (
                rest,
                Sequence {
                    _span: input_span.until_rest(&rest_span),
                    elements: vec,
                },
            )
        })
    }
}

impl<'a, T: std::fmt::Debug + Sync + ProgramAST<'a>> ProgramAST<'a> for Sequence<'a, T> {
    fn children(&self) -> Vec<&dyn ProgramAST<'a>> {
        let mut vec: Vec<&dyn ProgramAST> = Vec::new();
        for elem in &self.elements {
            vec.push(elem);
        }
        vec
    }

    fn span(&self) -> Span<'a> {
        self._span
    }

    fn parse(input: ParserInput<'a>) -> ParserResult<'a, Self>
    where
        Self: Sized + 'a,
    {
        let input_span = input.span;
        opt(terminated(
            separated_list1(tuple((WSoC::parse, Token::seq_sep, WSoC::parse)), T::parse),
            opt(tuple((WSoC::parse, Token::seq_sep, WSoC::parse))),
        ))(input)
        .map(|(rest, vec)| {
            let rest_span = rest.span;
            (
                rest,
                Sequence {
                    _span: input_span.until_rest(&rest_span),
                    elements: vec.unwrap_or(Vec::new()),
                },
            )
        })
    }

    fn context(&self) -> ParserContext {
        CONTEXT
    }
}

impl<'b, T> IntoIterator for &'b Sequence<'_, T> {
    type Item = &'b T;
    type IntoIter = std::slice::Iter<'b, T>;

    fn into_iter(self) -> Self::IntoIter {
        self.elements.iter()
    }
}

impl<T> IntoIterator for Sequence<'_, T> {
    type Item = T;
    type IntoIter = IntoIter<T>;

    fn into_iter(self) -> Self::IntoIter {
        self.elements.into_iter()
    }
}

#[cfg(test)]
mod test {
    use nom::combinator::all_consuming;

    use crate::parser::{ParserState, ast::expression::basic::number::Number};

    use super::*;

    #[test]
    fn with_trailing() {
        let test = [
            "1,", "1 ,", "1,2", "1 ,2", "1, 2", "1 , 2", "1,2,", "1 ,2,", "1, 2,", "1 ,2 ,",
            "1 , 2,", "1 , 2 ,",
        ];

        for input in test {
            let parser_input = ParserInput::new(input, ParserState::default());
            let result = all_consuming(Sequence::<Number>::parse_with_first_trailing)(parser_input);

            assert!(result.is_ok());
        }
    }
}
