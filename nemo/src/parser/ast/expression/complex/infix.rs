//! This module defines [InfixExpression].
#![allow(missing_docs)]

use enum_assoc::Assoc;
use nom::{
    branch::alt,
    combinator::map,
    sequence::{delimited, tuple},
};

use crate::parser::{
    ast::{
        comment::wsoc::WSoC,
        expression::Expression,
        token::{Token, TokenKind},
        ProgramAST,
    },
    context::{context, ParserContext},
    input::ParserInput,
    span::Span,
    ParserResult,
};

use super::{
    aggregation::Aggregation, arithmetic::Arithmetic, atom::Atom, map::Map, negation::Negation,
    operation::Operation, tuple::Tuple,
};

/// Types of infix expression connectives
#[derive(Assoc, Debug, Copy, Clone, PartialEq, Eq)]
#[func(pub fn token(token: TokenKind) -> Option<Self>)]
pub enum InfixExpressionKind {
    /// Equality
    #[assoc(token = TokenKind::Equal)]
    Equality,
    /// Inequality
    #[assoc(token = TokenKind::Unequal)]
    Inequality,
    /// Greater than or equal
    #[assoc(token = TokenKind::GreaterEqual)]
    GreaterEqual,
    /// Grater than
    #[assoc(token = TokenKind::Greater)]
    Greater,
    /// Less than or equal
    #[assoc(token = TokenKind::LessEqual)]
    LessEqual,
    /// Less than
    #[assoc(token = TokenKind::Less)]
    Less,
}

/// Expressions connected by an infix operation
#[derive(Debug)]
pub struct InfixExpression<'a> {
    /// [ProgramSpan] associated with this node
    span: Span<'a>,

    /// Kind of infix expression
    kind: InfixExpressionKind,
    /// Left part of the expression
    left: Box<Expression<'a>>,
    /// Right part of the expression
    right: Box<Expression<'a>>,
}

impl<'a> InfixExpression<'a> {
    /// Return the pair of [Expression]s.
    pub fn pair(&self) -> (&Expression<'a>, &Expression<'a>) {
        (&self.left, &self.right)
    }

    /// Return the [InfixExpressionKind] of this expression.
    pub fn kind(&self) -> InfixExpressionKind {
        self.kind
    }

    /// Parse a [InfixExpressionKind].
    fn parse_infix_kind(input: ParserInput<'a>) -> ParserResult<'a, InfixExpressionKind> {
        alt((
            Token::equal,
            Token::unequal,
            Token::greater_equal,
            Token::greater,
            Token::less_equal,
            Token::less,
        ))(input)
        .map(|(rest, result)| {
            (
                rest,
                InfixExpressionKind::token(result.kind())
                    .expect(&format!("unexpected token: {:?}", result.kind())),
            )
        })
    }

    /// Parse non-infix [Expression]s
    pub fn parse_non_infix(input: ParserInput<'a>) -> ParserResult<'a, Expression<'a>> {
        alt((
            map(Arithmetic::parse, Expression::Arithmetic),
            map(Aggregation::parse, Expression::Aggregation),
            map(Operation::parse, Expression::Operation),
            map(Atom::parse, Expression::Atom),
            map(Tuple::parse, Expression::Tuple),
            map(Map::parse, Expression::Map),
            map(Negation::parse, Expression::Negation),
            Expression::parse_basic,
        ))(input)
    }
}

const CONTEXT: ParserContext = ParserContext::Infix;

impl<'a> ProgramAST<'a> for InfixExpression<'a> {
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
        let input_span = input.span;

        context(
            CONTEXT,
            tuple((
                Self::parse_non_infix,
                delimited(WSoC::parse, Self::parse_infix_kind, WSoC::parse),
                Self::parse_non_infix,
            )),
        )(input)
        .map(|(rest, (left, kind, right))| {
            let rest_span = rest.span;

            (
                rest,
                Self {
                    span: input_span.until_rest(&rest_span),
                    kind,
                    left: Box::new(left),
                    right: Box::new(right),
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
        ast::{expression::complex::infix::InfixExpression, ProgramAST},
        input::ParserInput,
        ParserState,
    };

    use super::InfixExpressionKind;

    #[test]
    fn parse_infix() {
        let test = vec![
            ("?x=7", InfixExpressionKind::Equality),
            ("?x != ?y", InfixExpressionKind::Inequality),
            ("?x < ?y", InfixExpressionKind::Less),
            ("?x <= ?y", InfixExpressionKind::LessEqual),
            ("?x > ?y", InfixExpressionKind::Greater),
            ("?x >= ?y", InfixExpressionKind::GreaterEqual),
        ];

        for (input, expected) in test {
            let parser_input = ParserInput::new(input, ParserState::default());
            let result = all_consuming(InfixExpression::parse)(parser_input);

            assert!(result.is_ok());

            let result = result.unwrap();
            assert_eq!(expected, result.1.kind());
        }
    }
}
