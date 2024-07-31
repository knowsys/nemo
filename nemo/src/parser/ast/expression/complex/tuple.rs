//! This module defines [Tuple].

use nom::sequence::{delimited, pair};

use crate::parser::{
    ast::{
        comment::wsoc::WSoC, expression::Expression, sequence::Sequence, token::Token, ProgramAST,
    },
    context::{context, ParserContext},
    input::ParserInput,
    span::Span,
    ParserResult,
};

/// A sequence of [Expression]s.
#[derive(Debug)]
pub struct Tuple<'a> {
    /// [Span] associated with this node
    span: Span<'a>,

    /// List of underlying expressions
    expressions: Sequence<'a, Expression<'a>>,
}

impl<'a> Tuple<'a> {
    /// Return an iterator over the underlying [Expression]s.
    pub fn expressions(&self) -> impl Iterator<Item = &Expression<'a>> {
        self.expressions.iter()
    }
}

const CONTEXT: ParserContext = ParserContext::Tuple;

impl<'a> ProgramAST<'a> for Tuple<'a> {
    fn children(&self) -> Vec<&dyn ProgramAST> {
        let mut result = Vec::<&dyn ProgramAST>::new();

        for expression in &self.expressions {
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
            delimited(
                pair(Token::tuple_open, WSoC::parse),
                Sequence::parse_with_first_trailing,
                pair(WSoC::parse, Token::tuple_close),
            ),
        )(input)
        .map(|(rest, expressions)| {
            let rest_span = rest.span;

            (
                rest,
                Self {
                    span: input_span.until_rest(&rest_span),
                    expressions,
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
        ast::{expression::complex::tuple::Tuple, ProgramAST},
        input::ParserInput,
        ParserState,
    };

    #[test]
    fn parse_tuple() {
        let test = vec![
            ("(1,)", 1),
            ("(1,2)", 2),
            ("( 1 ,)", 1),
            ("( 1 , 2 )", 2),
            ("( 1 , 2 ,)", 2),
            ("( 1, 2, 3 )", 3),
            ("(1,2,3,)", 3),
        ];

        for (input, expected) in test {
            let parser_input = ParserInput::new(input, ParserState::default());
            let result = all_consuming(Tuple::parse)(parser_input);
            if result.is_err() {
                dbg!(&result);
            }

            assert!(result.is_ok());

            let result = result.unwrap();
            assert_eq!(expected, result.1.expressions().count());
        }
    }
}
