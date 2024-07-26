//! This module defines the ast node for numbers.

use enum_assoc::Assoc;
use nom::{
    branch::alt,
    combinator::opt,
    sequence::{pair, preceded, tuple},
};

use crate::parser::{
    ast::{
        token::{Token, TokenKind},
        ProgramAST,
    },
    context::{context, ParserContext},
    span::Span,
    ParserInput, ParserResult,
};

/// Marker that indicates the type of a number
#[derive(Assoc, Debug, Clone, Copy, PartialEq, Eq)]
#[func(pub fn token(token: &TokenKind) -> Option<Self>)]
#[func(pub fn print(&self) -> &'static str)]
enum NumberTypeMarker {
    /// Marks a number as a 32-bit floating point number
    #[assoc(token = TokenKind::TypeMarkerFloat)]
    #[assoc(print = "f")]
    Float,
    /// Marks a number as a 64-bit floating point number
    #[assoc(token = TokenKind::TypeMarkerDouble)]
    #[assoc(print = "d")]
    Double,
}

/// Sign of a number
#[derive(Assoc, Default, Debug, Clone, Copy, PartialEq, Eq)]
#[func(pub fn token(token: &TokenKind) -> Option<Self>)]
#[func(pub fn print(&self) -> &'static str)]
enum NumberSign {
    /// Positive
    #[assoc(token = TokenKind::Plus)]
    #[assoc(print = "+")]
    #[default]
    Positive,
    //// Negative
    #[assoc(token = TokenKind::Minus)]
    #[assoc(print = "-")]
    Negative,
}

/// AST Node representing a number
#[derive(Debug)]
pub struct Number<'a> {
    /// [ProgramSpan] associated with this node
    span: Span<'a>,

    /// Sign of the integer part
    _integer_sign: NumberSign,
    /// The integer part of the number
    _integer: Token<'a>,
    /// The fractional part of the number
    _fractional: Option<Token<'a>>,
    /// Sign and exponent of the number
    _exponent: Option<(NumberSign, Token<'a>)>,
    /// Type
    _type_marker: Option<NumberTypeMarker>,
}

impl<'a> Number<'a> {
    /// Parse the sign of the number
    fn parse_sign(input: ParserInput<'a>) -> ParserResult<'a, NumberSign> {
        alt((Token::plus, Token::minus))(input).map(|(rest, sign)| {
            (
                rest,
                NumberSign::token(&sign.kind())
                    .expect(&format!("unexpected token: {:?}", sign.kind())),
            )
        })
    }

    /// Parser the integer part of the number.
    fn parse_integer(input: ParserInput<'a>) -> ParserResult<'a, Token<'a>> {
        Token::digits(input)
    }

    /// Parse the fractional part of the number.
    fn parse_fractional(input: ParserInput<'a>) -> ParserResult<'a, Token<'a>> {
        preceded(Token::dot, Token::digits)(input)
    }

    /// Parse the exponent part of the number.
    fn parse_exponent(input: ParserInput<'a>) -> ParserResult<'a, (NumberSign, Token<'a>)> {
        preceded(
            alt((Token::exponent_lower, Token::exponent_upper)),
            pair(opt(Self::parse_sign), Self::parse_integer),
        )(input)
        .map(|(rest, (sign, integer))| (rest, (sign.unwrap_or_default(), integer)))
    }

    /// Parse the type marker of the number.
    fn parse_type(input: ParserInput<'a>) -> ParserResult<'a, NumberTypeMarker> {
        alt((Token::type_marker_float, Token::type_marker_double))(input).map(|(rest, marker)| {
            (
                rest,
                NumberTypeMarker::token(&marker.kind()).expect("unknown token"),
            )
        })
    }
}

const CONTEXT: ParserContext = ParserContext::Number;

impl<'a> ProgramAST<'a> for Number<'a> {
    fn children(&self) -> Vec<&dyn ProgramAST> {
        Vec::default()
    }

    fn span(&self) -> Span<'a> {
        self.span
    }

    fn parse(input: ParserInput<'a>) -> ParserResult<'a, Self>
    where
        Self: Sized,
    {
        let input_span = input.span;

        context(
            CONTEXT,
            tuple((
                opt(Self::parse_sign),
                Self::parse_integer,
                opt(Self::parse_fractional),
                opt(Self::parse_exponent),
                opt(Self::parse_type),
            )),
        )(input)
        .map(
            |(rest, (integer_sign, integer, fractional, exponent, type_marker))| {
                let rest_span = rest.span;

                (
                    rest,
                    Number {
                        span: input_span.until_rest(&rest_span),
                        _integer_sign: integer_sign.unwrap_or_default(),
                        _integer: integer,
                        _fractional: fractional,
                        _exponent: exponent,
                        _type_marker: type_marker,
                    },
                )
            },
        )
    }

    fn context(&self) -> ParserContext {
        CONTEXT
    }
}

#[cfg(test)]
mod test {
    use nom::combinator::all_consuming;

    use crate::parser::{
        ast::{expression::basic::number::Number, ProgramAST},
        ParserInput, ParserState,
    };

    #[test]
    fn parse_numbers() {
        let valid_numbers = vec![
            "123",
            "-210",
            "0012",
            "-0012",
            "0.2",
            "4534.34534345",
            "1e545",
            "1.1e435",
            "0.1e232d",
            "1.0e343f",
            "112E+12",
            "12312.1231",
            "0.1231f",
            "1231",
            "-1e+0",
            "1e-1",
        ];

        let invalid_numbers = vec![".1", "1.", "E9", ".e3", "7E", "."];

        for valid in valid_numbers {
            let input = ParserInput::new(valid, ParserState::default());
            let result = all_consuming(Number::parse)(input);

            assert!(result.is_ok())
        }

        for invalid in invalid_numbers {
            let input = ParserInput::new(invalid, ParserState::default());
            let result = all_consuming(Number::parse)(input);

            assert!(result.is_err())
        }
    }
}
