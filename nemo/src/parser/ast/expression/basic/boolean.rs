//! This module defines [Boolean]
#![allow(missing_docs)]

use enum_assoc::Assoc;
use nom::branch::alt;

use crate::parser::{
    ast::{
        token::{Token, TokenKind},
        ProgramAST,
    },
    context::{context, ParserContext},
    input::ParserInput,
    span::Span,
    ParserResult,
};

/// Boolean values
#[derive(Assoc, Debug, Clone, Copy, PartialEq, Eq)]
#[func(pub fn token(token: TokenKind) -> Option<Self>)]
pub enum BooleanValue {
    /// False
    #[assoc(token = TokenKind::False)]
    False,
    /// True
    #[assoc(token = TokenKind::True)]
    True,
}

/// AST node representing a Boolean node
#[derive(Debug)]
pub struct Boolean<'a> {
    /// [ProgramSpan] associated with this node
    span: Span<'a>,

    /// Value
    value: BooleanValue,
}

impl<'a> Boolean<'a> {
    /// Return the name of the Boolean node.
    pub fn value(&self) -> BooleanValue {
        self.value
    }

    /// Parse boolean
    fn parse_boolean_value(input: ParserInput<'a>) -> ParserResult<'a, BooleanValue> {
        alt((Token::boolean_true, Token::boolean_false))(input).map(|(rest, result)| {
            (
                rest,
                BooleanValue::token(result.kind())
                    .expect(&format!("unexpected token: {:?}", result.kind())),
            )
        })
    }
}

const CONTEXT: ParserContext = ParserContext::Boolean;

impl<'a> ProgramAST<'a> for Boolean<'a> {
    fn children(&self) -> Vec<&dyn ProgramAST> {
        Vec::default()
    }

    fn span(&self) -> Span<'a> {
        self.span
    }

    fn parse(input: ParserInput<'a>) -> ParserResult<'a, Self>
    where
        Self: Sized + 'a,
    {
        let input_span = input.span;

        context(CONTEXT, Self::parse_boolean_value)(input).map(|(rest, value)| {
            let rest_span = rest.span;

            (
                rest,
                Boolean {
                    span: input_span.until_rest(&rest_span),
                    value,
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
        ast::{expression::basic::boolean::Boolean, ProgramAST},
        input::ParserInput,
        ParserState,
    };

    use super::BooleanValue;

    #[test]
    fn parse_boolean() {
        let test = vec![("true", BooleanValue::True), ("false", BooleanValue::False)];

        for (input, expected) in test {
            let parser_input = ParserInput::new(input, ParserState::default());
            let result = all_consuming(Boolean::parse)(parser_input);

            assert!(result.is_ok());

            let result = result.unwrap();
            assert_eq!(expected, result.1.value());
        }
    }
}
