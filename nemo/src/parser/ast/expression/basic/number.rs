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
    span::ProgramSpan,
    ParserInput, ParserResult,
};

/// Marker that indicates the type of a number
#[derive(Assoc, Debug, Clone, Copy, PartialEq, Eq)]
#[func(pub fn token(token: &TokenKind) -> Option<Self>)]
#[func(pub fn print(&self) -> &'static str)]
pub enum NumberTypeMarker {
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
    span: ProgramSpan<'a>,

    /// Sign of the integer part
    integer_sign: NumberSign,
    /// The integer part of the number
    integer: Token<'a>,
    /// The fractional part of the number
    fractional: Option<Token<'a>>,
    /// Sign and exponent of the number
    exponent: Option<(NumberSign, Token<'a>)>,
    /// Type
    type_marker: Option<NumberTypeMarker>,
}

/// Value of [Number]
#[derive(Debug)]
pub enum NumberValue {
    /// Integer value
    Integer(i64),
    /// 32-bit floating point value
    Float(f32),
    /// 64-bit floating point value
    Double(f64),
    /// Value doesn't fit into the above types
    Large(String),
}

impl<'a> Number<'a> {
    /// Return whether the number contains a fractional part.
    pub fn is_fractional(&self) -> bool {
        self.fractional.is_some()
    }

    /// Return whether the number contains an exponential part.
    pub fn is_exponential(&self) -> bool {
        self.exponent.is_some()
    }

    /// Return the [NumberTypeMarker] of this number.
    pub fn type_marker(&self) -> Option<NumberTypeMarker> {
        self.type_marker
    }

    /// Recreate the number string without the type marker.
    fn number_string(&self) -> String {
        let integer = format!(
            "{}{}",
            self.integer_sign.print(),
            self.integer.span().0.to_string()
        );

        let fractional = if let Some(fractional) = &self.fractional {
            format!(".{}", fractional.span().0.to_string())
        } else {
            String::default()
        };

        let exponent = if let Some((sign, exponent)) = &self.exponent {
            format!("e{}{}", sign.print(), exponent.span().0.to_string())
        } else {
            String::default()
        };

        format!("{}{}{}", integer, fractional, exponent)
    }

    /// Return the value of this number, represented as a [NumberValue].
    pub fn value(&self) -> NumberValue {
        let string = self.number_string();

        if let Ok(integer) = str::parse::<i64>(&string) {
            return NumberValue::Integer(integer);
        }

        if let Some(NumberTypeMarker::Float) = self.type_marker {
            if let Ok(float) = str::parse::<f32>(&string) {
                return NumberValue::Float(float);
            }
        }

        if let Ok(double) = str::parse::<f64>(&string) {
            return NumberValue::Double(double);
        }

        if let Ok(float) = str::parse::<f32>(&string) {
            return NumberValue::Float(float);
        }

        NumberValue::Large(string)
    }

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

    fn span(&self) -> ProgramSpan<'a> {
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
                        integer_sign: integer_sign.unwrap_or_default(),
                        integer,
                        fractional,
                        exponent,
                        type_marker,
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
