//! This module defines [Expression].

pub mod basic;
pub mod complex;

use basic::{
    blank::Blank, boolean::Boolean, constant::Constant, number::Number, enc_number::EncodedNumber,rdf_literal::RdfLiteral, string::StringLiteral, variable::Variable
};
use complex::{
    aggregation::Aggregation, arithmetic::Arithmetic, atom::Atom, fstring::FormatString, map::Map,
    negation::Negation, operation::Operation, parenthesized::ParenthesizedExpression, tuple::Tuple,
};
use nom::{branch::alt, combinator::map};

use crate::parser::{
    context::{context, ParserContext},
    input::ParserInput,
    span::Span,
    ParserResult,
};

use super::ProgramAST;

/// An expression of potentially complex terms
#[derive(Debug)]
pub enum Expression<'a> {
    /// Aggregation
    Aggregation(Aggregation<'a>),
    /// Arithmetic
    Arithmetic(Arithmetic<'a>),
    /// Atom
    Atom(Atom<'a>),
    /// Blank
    Blank(Blank<'a>),
    /// Boolean
    Boolean(Boolean<'a>),
    /// Constant
    Constant(Constant<'a>),
    /// Format String
    FormatString(FormatString<'a>),
    /// Map
    Map(Map<'a>),
    /// Negation
    Negation(Negation<'a>),
    /// Number
    Number(Number<'a>),
    /// Encoded number
    EncodedNumber(EncodedNumber<'a>),
    /// Operation
    Operation(Operation<'a>),
    /// Parenthesized expression
    Parenthesized(ParenthesizedExpression<'a>),
    /// Rdf literal
    RdfLiteral(RdfLiteral<'a>),
    /// String
    String(StringLiteral<'a>),
    /// Tuple
    Tuple(Tuple<'a>),
    /// Variable
    Variable(Variable<'a>),
}

impl<'a> Expression<'a> {
    /// Return the [ParserContext] of the underlying expression type.
    pub fn context_type(&self) -> ParserContext {
        match self {
            Expression::Aggregation(expression) => expression.context(),
            Expression::Arithmetic(expression) => expression.context(),
            Expression::Atom(expression) => expression.context(),
            Expression::Blank(expression) => expression.context(),
            Expression::Boolean(expression) => expression.context(),
            Expression::Constant(expression) => expression.context(),
            Expression::FormatString(expression) => expression.context(),
            Expression::Map(expression) => expression.context(),
            Expression::Number(expression) => expression.context(),
            Expression::EncodedNumber(expression) => expression.context(),
            Expression::Negation(expression) => expression.context(),
            Expression::Operation(expression) => expression.context(),
            Expression::Parenthesized(expression) => expression.context(),
            Expression::RdfLiteral(expression) => expression.context(),
            Expression::String(expression) => expression.context(),
            Expression::Tuple(expression) => expression.context(),
            Expression::Variable(expression) => expression.context(),
        }
    }

    /// Parse basic expressions.
    pub fn parse_basic(input: ParserInput<'a>) -> ParserResult<'a, Self> {
        alt((
            map(Blank::parse, Self::Blank),
            map(Boolean::parse, Self::Boolean),
            map(Constant::parse, Self::Constant),
            map(EncodedNumber::parse, Self::EncodedNumber),
            map(Number::parse, Self::Number),
            map(RdfLiteral::parse, Self::RdfLiteral),
            map(StringLiteral::parse, Self::String),
            map(Variable::parse, Self::Variable),
        ))(input)
    }

    /// Parse complex expressions, except arithmetic and infix.
    pub fn parse_complex(input: ParserInput<'a>) -> ParserResult<'a, Self> {
        alt((
            map(Aggregation::parse, Self::Aggregation),
            map(Operation::parse, Self::Operation),
            map(Atom::parse, Self::Atom),
            map(Map::parse, Self::Map),
            map(Negation::parse, Self::Negation),
            map(Tuple::parse, Self::Tuple),
            map(FormatString::parse, Self::FormatString),
        ))(input)
    }
}

const CONTEXT: ParserContext = ParserContext::Expression;

impl<'a> ProgramAST<'a> for Expression<'a> {
    fn children(&self) -> Vec<&dyn ProgramAST<'a>> {
        vec![match self {
            Expression::Aggregation(expression) => expression,
            Expression::Arithmetic(expression) => expression,
            Expression::Atom(expression) => expression,
            Expression::Blank(expression) => expression,
            Expression::Boolean(expression) => expression,
            Expression::Constant(expression) => expression,
            Expression::FormatString(expression) => expression,
            Expression::Map(expression) => expression,
            Expression::Number(expression) => expression,
            Expression::EncodedNumber(expression) => expression,
            Expression::Negation(expression) => expression,
            Expression::Operation(expression) => expression,
            Expression::Parenthesized(expression) => expression,
            Expression::RdfLiteral(expression) => expression,
            Expression::String(expression) => expression,
            Expression::Tuple(expression) => expression,
            Expression::Variable(expression) => expression,
        }]
    }

    fn span(&self) -> Span<'a> {
        match self {
            Expression::Aggregation(expression) => expression.span(),
            Expression::Arithmetic(expression) => expression.span(),
            Expression::Atom(expression) => expression.span(),
            Expression::Blank(expression) => expression.span(),
            Expression::Boolean(expression) => expression.span(),
            Expression::Constant(expression) => expression.span(),
            Expression::FormatString(expression) => expression.span(),
            Expression::Map(expression) => expression.span(),
            Expression::Number(expression) => expression.span(),
            Expression::EncodedNumber(expression) => expression.span(),
            Expression::Negation(expression) => expression.span(),
            Expression::Operation(expression) => expression.span(),
            Expression::Parenthesized(expression) => expression.span(),
            Expression::RdfLiteral(expression) => expression.span(),
            Expression::String(expression) => expression.span(),
            Expression::Tuple(expression) => expression.span(),
            Expression::Variable(expression) => expression.span(),
        }
    }

    fn parse(input: ParserInput<'a>) -> ParserResult<'a, Self>
    where
        Self: Sized + 'a,
    {
        context(
            CONTEXT,
            alt((
                map(Arithmetic::parse, Self::Arithmetic),
                map(ParenthesizedExpression::parse, Self::Parenthesized),
                Self::parse_complex,
                Self::parse_basic,
            )),
        )(input)
    }

    fn context(&self) -> ParserContext {
        CONTEXT
    }
}

#[cfg(test)]
mod test {
    use nom::combinator::all_consuming;

    use crate::parser::{
        ast::{expression::Expression, ProgramAST},
        context::ParserContext,
        input::ParserInput,
        ParserState,
    };

    #[test]
    fn parse_expression() {
        let test = vec![
            ("#sum(1 + POW(?x, 2), ?y, ?z)", ParserContext::Aggregation),
            ("1 + 2", ParserContext::Arithmetic),
            ("test(?x, (1,), (1 + 2))", ParserContext::Atom),
            ("_:12", ParserContext::Blank),
            ("true", ParserContext::Boolean),
            ("constant", ParserContext::Constant),
            ("{a=1,b=POW(1, 2)}", ParserContext::Map),
            ("12", ParserContext::Number),
            ("~test(1)", ParserContext::Negation),
            ("0o1", ParserContext::EncodedNumber),
            ("substr(\"string\", 1+?x)", ParserContext::Operation),
            ("(int)", ParserContext::ParenthesizedExpression),
            (
                "\"true\"^^<http://www.w3.org/2001/XMLSchema#boolean>",
                ParserContext::RdfLiteral,
            ),
            ("\"string\"", ParserContext::String),
            ("\"\"", ParserContext::String),
            ("(1,)", ParserContext::Tuple),
            ("?variable", ParserContext::Variable),
            ("f\"{?x + ?y}\"", ParserContext::FormatString),
        ];

        for (input, expect) in test {
            let parser_input = ParserInput::new(input, ParserState::default());
            let result = all_consuming(Expression::parse)(parser_input);

            assert!(result.is_ok());

            let result = result.unwrap();
            assert_eq!(result.1.context_type(), expect);
        }
    }

    #[test]
    fn complex_expression() {
        let input = "?distance = SQRT(POW(?Xp - ?Xr, 2.0) + POW(?Yp - ?Yr, 2.0))";
        let parser_input = ParserInput::new(input, ParserState::default());
        let result = Expression::parse(parser_input);
        assert!(result.is_ok());
    }
}
