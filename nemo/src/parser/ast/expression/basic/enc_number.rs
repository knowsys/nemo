//! This module defines the ast node for binary, octal and hexadezimal numbers.
#![allow(missing_docs)]

use enum_assoc::Assoc;
use nom::{branch::alt, sequence::preceded};

use crate::parser::{
    ast::{
        token::{Token, TokenKind},
        ProgramAST,
    },
    context::{context, ParserContext},
    span::Span,
    ParserInput, ParserResult,
};
use num::{BigInt,Num};

/// Define a different type for each prefix token
#[derive(Assoc, Debug, Clone, Copy, PartialEq, Eq)]
#[func(pub fn token(token: &TokenKind) -> Option<Self>)]
#[func(pub fn print(&self) -> &'static str)]
enum Encoding {
    #[assoc(token = TokenKind::BinaryPrefix)]
    #[assoc(print = "0b")]
    Binary,
    #[assoc(token = TokenKind::OctalPrefix)]
    #[assoc(print = "0o")]
    Octal,
    #[assoc(token = TokenKind::HexPrefix)]
    #[assoc(print = "0x")]
    Hexadecimal,
}

impl Encoding {
    /// Returns the base of each encoding type
    pub fn radix(&self) -> u32 {
        match self {
            Encoding::Binary => 2,
            Encoding::Octal => 8,
            Encoding::Hexadecimal => 16,
        }
    }
}

/// AST Node representing an encoded number
#[derive(Debug)]
pub struct EncodedNumber<'a> {
    /// [Span] associated with this node
    span: Span<'a>,

    /// The prefix of the encoded number
    prefix: Encoding,
    /// The suffix of the encoded number
    suffix: Token<'a>,
}

/// Value of [EncodedNumber]
#[derive(Debug)]
pub enum EncodedNumberValue {
    /// Integer value
    Integer(i64),
    /// Value doesn't fit into the above types
    Large(String),
}

impl<'a> EncodedNumber<'a> {
    /// Removes the binary prefix (0b) and returns the binary suffix
    fn parse_binary(input: ParserInput<'a>) -> ParserResult<'a, (Encoding, Token<'a>)> {
        preceded(Token::binary_prefix, Token::bin_number)(input)
            .map(|(remaining, bin_digits)| (remaining, (Encoding::Binary, bin_digits)))
    }

    /// Removes the octal prefix (0o) and returns the octal suffix
    fn parse_octal(input: ParserInput<'a>) -> ParserResult<'a, (Encoding, Token<'a>)> {
        preceded(Token::octal_prefix, Token::oct_number)(input)
            .map(|(remaining, oct_digits)| (remaining, (Encoding::Octal, oct_digits)))
    }

    /// Removes the hex prefix (0x) and returns the hex suffix
    fn parse_hex(input: ParserInput<'a>) -> ParserResult<'a, (Encoding, Token<'a>)> {
        preceded(Token::hex_prefix, Token::hex_number)(input)
            .map(|(remaining, hex_chars)| (remaining, (Encoding::Hexadecimal, hex_chars)))
    }

    /// Return the value of this number, represented as an [EncodedNumberValue].
    pub fn value(&self) -> EncodedNumberValue {
        let string = format!("{}{}", self.prefix.print(), self.suffix);

        // Retrieves the base of encoded number based on [Encoding]
        let nr_encoding = self.prefix.radix();
        let span = &self.suffix.span();
        let suffix = span.fragment();

        // Returns decoded number as <i64> if it is not too big
        // Otherwise, return string representation of the decoded number
        if let Ok(integer) = <i64>::from_str_radix(suffix, nr_encoding) {
            return EncodedNumberValue::Integer(integer);
        } else if let Ok(bigint) = BigInt::from_str_radix(suffix, nr_encoding) {
            return EncodedNumberValue::Large(bigint.to_string());
        }
        EncodedNumberValue::Large(string)
    }
}

const CONTEXT: ParserContext = ParserContext::EncodedNumber;

impl<'a> ProgramAST<'a> for EncodedNumber<'a> {
    fn children(&self) -> Vec<&dyn ProgramAST<'a>> {
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
            alt((Self::parse_binary, Self::parse_octal, Self::parse_hex)),
        )(input)
        .map(|(rest, (prefix, suffix))| {
            let rest_span = rest.span;

            (
                rest,
                EncodedNumber {
                    span: input_span.until_rest(&rest_span),
                    prefix,
                    suffix,
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
        ast::{
            expression::basic::enc_number::{EncodedNumber, EncodedNumberValue},
            ProgramAST,
        },
        ParserInput, ParserState,
    };

    #[test]
    fn parse_numbers() {
        let valid_numbers = vec![
            ("0x11", 17),
            ("0o11", 9),
            ("0b11", 3),
            ("0xAB2CE", 701134),
            ("0xab2ce", 701134),
            ("0o777", 511),
            ("0b01010101", 85),
        ];

        let invalid_numbers = vec!["0xG", "0o8", "0b2", "0x", "0b", "0o"];

        for (valid, exp_value) in valid_numbers {
            let input = ParserInput::new(valid, ParserState::default());
            let result = all_consuming(EncodedNumber::parse)(input);
            assert!(result.is_ok());

            if let Ok((_, ast_node)) = result {
                if let EncodedNumberValue::Integer(integer) = ast_node.value() {
                    assert_eq!(integer, exp_value);
                }
            };
        }

        for invalid in invalid_numbers {
            let input = ParserInput::new(invalid, ParserState::default());
            let result = all_consuming(EncodedNumber::parse)(input);

            assert!(result.is_err())
        }
    }
}
