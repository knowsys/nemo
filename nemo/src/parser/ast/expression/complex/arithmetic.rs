//! This module defines [Arithmetic].
#![allow(missing_docs)]

use ascii_tree::write_tree;
use enum_assoc::Assoc;
use nom::{
    branch::alt,
    multi::many0,
    sequence::{delimited, pair, preceded, separated_pair},
};
use nom_supreme::error::{BaseErrorKind, Expectation};

use crate::parser::{
    ast::{
        ast_to_ascii_tree,
        comment::wsoc::WSoC,
        expression::Expression,
        token::{Token, TokenKind},
        ProgramAST,
    },
    context::{context, ParserContext},
    error::ParserErrorTree,
    input::ParserInput,
    span::Span,
    ParserResult,
};

/// Types of arithmetic operations
#[derive(Assoc, Debug, Copy, Clone, PartialEq, Eq)]
#[func(pub fn token(token: TokenKind) -> Option<Self>)]
pub enum ArithmeticOperation {
    /// Addition
    #[assoc(token = TokenKind::Plus)]
    Addition,
    /// Subtraction
    #[assoc(token = TokenKind::Minus)]
    Subtraction,
    /// Multiplication
    #[assoc(token = TokenKind::Multiplication)]
    Multiplication,
    /// Division
    #[assoc(token = TokenKind::Division)]
    Division,
}

impl ArithmeticOperation {
    /// Parse additive operation.
    pub fn parse_additive<'a>(input: ParserInput<'a>) -> ParserResult<'a, Self> {
        alt((Token::plus, Token::minus))(input).map(|(rest, result)| {
            (
                rest,
                ArithmeticOperation::token(result.kind())
                    .expect(&format!("unexpected token: {:?}", result.kind())),
            )
        })
    }

    /// Parse multiplicative operation.
    pub fn parse_multiplicative<'a>(input: ParserInput<'a>) -> ParserResult<'a, Self> {
        alt((Token::star, Token::division))(input).map(|(rest, result)| {
            (
                rest,
                ArithmeticOperation::token(result.kind())
                    .expect(&format!("unexpected token: {:?}", result.kind())),
            )
        })
    }
}

/// Arithmetic expression on numbers
#[derive(Debug)]
pub struct Arithmetic<'a> {
    /// [ProgramSpan] associated with this node
    span: Span<'a>,

    /// Type of arithmetic operation
    kind: ArithmeticOperation,
    /// Left input
    left: Box<Expression<'a>>,
    /// Right input
    right: Box<Expression<'a>>,
}

impl<'a> Arithmetic<'a> {
    /// Return the kind of arithmetic operation.
    pub fn kind(&self) -> ArithmeticOperation {
        self.kind
    }

    /// Return the left part of the operation.
    pub fn left(&self) -> &Expression<'a> {
        &self.left
    }

    /// Return the right part of the operation.
    pub fn right(&self) -> &Expression<'a> {
        &self.right
    }

    /// Return a formatted ascii tree to pretty print the AST
    pub fn ascii_tree(&self) -> String {
        let mut output = String::new();
        write_tree(&mut output, &ast_to_ascii_tree(self)).unwrap();
        format!("{output}")
    }
}

impl std::fmt::Display for Arithmetic<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.ascii_tree())
    }
}

#[derive(Debug)]
struct ArithmeticChain<'a> {
    initial: Expression<'a>,
    sequence: Vec<(ArithmeticOperation, Expression<'a>)>,
}

impl<'a> ArithmeticChain<'a> {
    fn fold(mut self, input_span: &Span<'a>) -> Expression<'a> {
        if self.sequence.is_empty() {
            self.initial
        } else {
            let sequence_rest = self.sequence.split_off(1);
            let sequence_first = self.sequence.remove(0);

            let start = Arithmetic {
                span: input_span.enclose(&self.initial.span(), &sequence_first.1.span()),
                kind: sequence_first.0,
                left: Box::new(self.initial),
                right: Box::new(sequence_first.1),
            };

            Expression::Arithmetic(sequence_rest.into_iter().fold(
                start,
                |acc, (kind, expression)| Arithmetic {
                    span: input_span.enclose(&acc.span, &expression.span()),
                    kind,
                    left: Box::new(Expression::Arithmetic(acc)),
                    right: Box::new(expression),
                },
            ))
        }
    }
}

impl<'a> Arithmetic<'a> {
    fn parse_non_arithmetic(input: ParserInput<'a>) -> ParserResult<'a, Expression<'a>> {
        alt((Expression::parse_complex, Expression::parse_basic))(input)
    }

    /// Parse an expression enclosed in parenthesis.
    fn parse_parenthesized_expression(input: ParserInput<'a>) -> ParserResult<'a, Expression<'a>> {
        delimited(
            pair(Token::open_parenthesis, WSoC::parse),
            Self::parse,
            pair(WSoC::parse, Token::closed_parenthesis),
        )(input)
        .map(|(rest, arithmetic_expr)| (rest, Expression::Arithmetic(arithmetic_expr)))
    }

    // /// Parse an arithmetic expression enclosed in parenthesis.
    // fn parse_parenthesized_arithmetic(input: ParserInput<'a>) -> ParserResult<'a, Self> {
    //     delimited(
    //         pair(Token::open_parenthesis, WSoC::parse),
    //         Self::parse,
    //         pair(WSoC::parse, Token::closed_parenthesis),
    //     )(input)
    // }

    /// Parse factor.
    fn parse_factor(input: ParserInput<'a>) -> ParserResult<'a, Expression<'a>> {
        alt((
            Self::parse_non_arithmetic,
            Self::parse_parenthesized_expression,
        ))(input)
    }

    ///
    fn parse_product(input: ParserInput<'a>) -> ParserResult<'a, ArithmeticChain<'a>> {
        pair(
            Self::parse_factor,
            many0(preceded(
                WSoC::parse,
                separated_pair(
                    ArithmeticOperation::parse_multiplicative,
                    WSoC::parse,
                    Self::parse_factor,
                ),
            )),
        )(input)
        .map(|(rest, (initial, sequence))| (rest, ArithmeticChain { initial, sequence }))
    }

    fn parse_sum(input: ParserInput<'a>) -> ParserResult<'a, Expression<'a>> {
        let input_span = input.span;

        pair(
            Self::parse_product,
            many0(preceded(
                WSoC::parse,
                separated_pair(
                    ArithmeticOperation::parse_additive,
                    WSoC::parse,
                    Self::parse_product,
                ),
            )),
        )(input)
        .map(|(rest, (initial, sequence))| {
            (
                rest,
                ArithmeticChain {
                    initial: initial.fold(&input_span),
                    sequence: sequence
                        .into_iter()
                        .map(|(operation, chain)| (operation, chain.fold(&input_span)))
                        .collect(),
                }
                .fold(&input_span),
            )
        })
    }
}

const CONTEXT: ParserContext = ParserContext::Arithmetic;

impl<'a> ProgramAST<'a> for Arithmetic<'a> {
    fn children(&self) -> Vec<&dyn ProgramAST> {
        vec![&*self.left, &*self.right]
    }

    fn span(&self) -> Span<'a> {
        self.span
    }

    fn parse(input: ParserInput<'a>) -> ParserResult<'a, Self>
    where
        Self: Sized + 'a,
    {
        let arithmetic_parser = |input: ParserInput<'a>| {
            if let Ok((rest, expression)) = Self::parse_sum(input.clone()) {
                if let Expression::Arithmetic(result) = expression {
                    return Ok((rest, result));
                }
            }

            Err(nom::Err::Error(ParserErrorTree::Base {
                location: input,
                kind: BaseErrorKind::Expected(Expectation::Tag("arithmetic expression")),
            }))
        };

        context(CONTEXT, arithmetic_parser)(input)
    }

    fn context(&self) -> ParserContext {
        CONTEXT
    }
}

#[cfg(test)]
mod test {
    use nom::combinator::all_consuming;

    use crate::parser::{
        ast::{
            expression::{complex::arithmetic::Arithmetic, Expression},
            ProgramAST,
        },
        input::ParserInput,
        ParserState,
    };

    /// Count the number of expressions contained in an arithmetic expression
    fn count_expression<'a>(expression: &Expression<'a>) -> usize {
        match expression {
            Expression::Arithmetic(arithmetic) => {
                count_expression(arithmetic.left()) + count_expression(arithmetic.right())
            }
            _ => 1,
        }
    }

    #[test]
    fn parse_arithmetic() {
        let test = vec![
            ("1 * 2", 2),
            ("1 * 2 * ?y", 3),
            ("1 * (2 / ?y)", 3), // FIXME: Span has missing `)`
            ("(1 / 2) * ?y", 3),
            ("1 + 2", 2),
            ("1 + 2 + ?x", 3),
            ("1 + 2 * (3 * ?y)", 4), // FIXME: This test produces weird spans
            ("1 + (2 * 3) * ?y + 4", 5), // FIXME: Here the spans are also wrong
            ("1 + (2 * ((3 * ?y)))", 4),
            ("1 + 2 * POW(3, 4)", 3), // FIXME: The same
            ("2 * (((18 + 3)))", 3),
        ];

        for (input, expected) in test {
            let parser_input = ParserInput::new(input, ParserState::default());
            let result = all_consuming(Arithmetic::parse)(parser_input);
            match &result {
                Ok((_, ast)) => println!("{ast}"),
                Err(_) => assert!(false),
            }

            let result = result.unwrap();
            assert_eq!(
                expected,
                count_expression(&Expression::Arithmetic(result.1))
            );
        }
    }
}