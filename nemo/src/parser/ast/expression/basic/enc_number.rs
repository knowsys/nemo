//! This module defines the ast node for binary, octal and hexadezimal numbers.
#![allow(missing_docs)]

use enum_assoc::Assoc;
use nom::{
    branch::alt,
    sequence::preceded,
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
    pub fn radix(&self) -> u32 {
        match self {
            Encoding::Binary => 2,
            Encoding::Octal => 8,
            Encoding::Hexadecimal => 16,
        }
    }
}

#[derive(Debug)]
pub struct EncodedNumber<'a>{
    /// [Span] associated with this node
    span: Span<'a>,
    /// The prefix of the encoded number
    prefix: Encoding,
    /// The suffix of the encoded number
    suffix: Token<'a>,
}


/// Value of [EncodedNumber]
#[derive(Debug)]
pub enum NumberValue {
    /// Integer value
    Integer(i64),
    /// Value doesn't fit into the above types
    Large(String),
}

impl<'a> EncodedNumber<'a> {

    /// is_bin_digit, is_oct_digit, is_hex_digit exist already in nom
    /// this function can be simplified, once nom > 7.1.3 with chraracter::complete::bin_digit1
    fn parse_binary(input: ParserInput<'a>) -> ParserResult<'a, (Encoding, Token<'a>)> {
        preceded(Token::binary_prefix, Token::bin_number)(input).map(
            |(remaining, digits)| {
                (remaining,(Encoding::Binary,digits))
        })
    }

    /// Todo: Return an Encoding GPT:
    /// Loesung: digits kommt von Nom direkt und hat den output typ der als input kommt
    /// alle anderen Funktionen sind custom und returnen Token<'a>
    fn parse_octal(input: ParserInput<'a>) -> ParserResult<'a, (Encoding, Token<'a>)> {
        preceded(Token::octal_prefix, Token::oct_number)(input).map(
            |(remaining, digits)| {
                (remaining,(Encoding::Octal,digits))
        })
    }


    fn parse_hex(input: ParserInput<'a>) -> ParserResult<'a, (Encoding, Token<'a>)> {
        preceded(Token::hex_prefix, Token::hex_number)(input).map(
            |(remaining, digits)| {
                (remaining,(Encoding::Hexadecimal,digits))
        })
    }


    /// Return the value of this number, represented as a [NumberValue].
    pub fn value(&self) -> NumberValue {
        let string = format!("{}{}",self.prefix.print(),self.suffix);

        let nr_encoding = self.prefix.radix();
        let span = &self.suffix.span();
        let suffix = span.fragment();
        if let Ok(integer) = <i64>::from_str_radix(suffix, nr_encoding) {
            return NumberValue::Integer(integer);
        }
        NumberValue::Large(string)
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
            alt((
                Self::parse_binary,
                Self::parse_octal,
                Self::parse_hex,
            )),
        )(input)
        .map(
            |(rest, (prefix,suffix))| {
                let rest_span = rest.span;

                (
                    rest,
                    EncodedNumber {
                        span: input_span.until_rest(&rest_span),
                        prefix,
                        suffix,
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
        ast::{expression::basic::enc_number::{EncodedNumber,NumberValue}, ProgramAST},
        ParserInput, ParserState,
    };

    #[test]
    fn parse_numbers() {
        let valid_numbers = vec![
            ("0x11",17),
            ("0o11",9),
            ("0b11",3),
            ("0xAB2CE",701134),
            ("0xab2ce",701134),
            ("0o777",511),
            ("0b01010101",85),
        ];

        let invalid_numbers = vec!["0xG", "0o8", "0b2", "0x","0b", "0o"];

        for (valid, exp_value) in valid_numbers {
            let input = ParserInput::new(valid, ParserState::default());
            let result = all_consuming(EncodedNumber::parse)(input);
            assert!(result.is_ok());

            match result{
                Ok((_,ast_node)) => {
                    if let NumberValue::Integer(integer) = ast_node.value() {
                        assert_eq!(integer,exp_value);
                    }else{
                        // should we throw an error if parsed as Large ?
                    }
                }
                ,
                Err(e) => println!("{}",e),
            }
            
        }

        for invalid in invalid_numbers {
            let input = ParserInput::new(invalid, ParserState::default());
            let result = all_consuming(EncodedNumber::parse)(input);

            assert!(result.is_err())
        }
    }
}
