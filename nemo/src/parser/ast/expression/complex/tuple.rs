//! This module defines [Tuple].

use nom::{
    combinator::opt,
    sequence::{delimited, pair, terminated, tuple},
};

use crate::parser::{
    ast::{
        expression::{sequence::one::ExpressionSequenceOne, Expression},
        token::Token,
        ProgramAST,
    },
    context::{context, ParserContext},
    span::ProgramSpan,
};

/// A sequence of [Expression]s.
#[derive(Debug)]
pub struct Tuple<'a> {
    /// [ProgramSpan] associated with this node
    span: ProgramSpan<'a>,

    /// List of underlying expressions
    expressions: ExpressionSequenceOne<'a>,
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
        let mut result: Vec<&dyn ProgramAST> = vec![];
        for expression in &self.expressions {
            result.push(expression)
        }

        result
    }

    fn span(&self) -> ProgramSpan {
        self.span
    }

    fn parse(input: crate::parser::input::ParserInput<'a>) -> crate::parser::ParserResult<'a, Self>
    where
        Self: Sized + 'a,
    {
        let input_span = input.span;

        context(
            CONTEXT,
            delimited(
                pair(Token::open_parenthesis, opt(Token::whitespace)),
                terminated(
                    ExpressionSequenceOne::parse,
                    opt(tuple((
                        opt(Token::whitespace),
                        Token::comma,
                        opt(Token::whitespace),
                    ))),
                ),
                pair(opt(Token::whitespace), Token::closed_parenthesis),
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
        ];

        for (input, expected) in test {
            let parser_input = ParserInput::new(input, ParserState::default());
            let result = all_consuming(Tuple::parse)(parser_input);

            assert!(result.is_ok());

            let result = result.unwrap();
            assert_eq!(expected, result.1.expressions().count());
        }
    }
}
