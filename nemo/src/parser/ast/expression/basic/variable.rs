//! This module defines [Variable]
#![allow(missing_docs)]

use enum_assoc::Assoc;
use nom::{
    branch::alt,
    combinator::{cut, map},
    sequence::pair,
};

use crate::parser::{
    ParserResult,
    ast::{
        ProgramAST,
        token::{Token, TokenKind},
    },
    context::{ParserContext, context},
    input::ParserInput,
    span::Span,
};

/// Marker that indicates the type of variable
#[derive(Assoc, Debug, Clone, Copy, PartialEq, Eq)]
#[func(pub fn token(token: TokenKind) -> Option<Self>)]
pub enum VariableType {
    /// Universal variable
    #[assoc(token = TokenKind::UniversalIndicator)]
    Universal,
    /// Existential variable
    #[assoc(token = TokenKind::ExistentialIndicator)]
    Existential,
    /// Global variable
    #[assoc(token = TokenKind::GlobalIndicator)]
    Global,
    /// Anonymous variable
    #[assoc(token = TokenKind::AnonVal)]
    Anonymous,
}

/// AST node representing a variable
#[derive(Debug)]
pub struct Variable<'a> {
    /// [Span] associated with this node
    span: Span<'a>,

    /// Type of variable
    kind: VariableType,

    /// Name of the variable
    name: Option<Token<'a>>,
}

impl<'a> Variable<'a> {
    /// Return the name of the variable, if given
    pub fn name(&self) -> Option<String> {
        self.name.as_ref().map(|token| token.to_string())
    }

    /// Return the type of variable
    pub fn kind(&self) -> VariableType {
        self.kind
    }

    /// Parse a named variable
    fn parse_named_variable(input: ParserInput<'a>) -> ParserResult<'a, (VariableType, Token<'a>)> {
        pair(
            map(
                alt((
                    Token::universal_indicator,
                    Token::existential_indicator,
                    Token::global_indicator,
                )),
                |indicator| {
                    VariableType::token(indicator.kind()).expect("unknown variable indicator")
                },
            ),
            cut(Token::name),
        )(input)
    }

    /// Parse an anonymous variable
    fn parse_anonymous_variable(input: ParserInput<'a>) -> ParserResult<'a, VariableType> {
        map(Token::underscore, |indicator| {
            VariableType::token(indicator.kind()).expect("unknown variable indicator")
        })(input)
    }
}

const CONTEXT: ParserContext = ParserContext::Variable;

impl<'a> ProgramAST<'a> for Variable<'a> {
    fn children(&self) -> Vec<&dyn ProgramAST<'a>> {
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

        context(
            CONTEXT,
            alt((
                map(Self::parse_named_variable, |(typ, name)| (typ, Some(name))),
                map(Self::parse_anonymous_variable, |typ| (typ, None)),
            )),
        )(input)
        .map(|(rest, (kind, name))| {
            let rest_span = rest.span;

            (
                rest,
                Variable {
                    span: input_span.until_rest(&rest_span),
                    kind,
                    name,
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
    use std::assert_matches;

    use crate::parser::{
        ParserState,
        ast::{ProgramAST, expression::basic::variable::Variable},
        input::ParserInput,
    };

    use super::VariableType;

    #[test]
    fn parse_variable() {
        let test = vec![
            (
                "?universal",
                (Some("universal".to_string()), VariableType::Universal),
            ),
            (
                "!existential",
                (Some("existential".to_string()), VariableType::Existential),
            ),
            ("_", (None, VariableType::Anonymous)),
        ];

        for (input, expected) in test {
            let parser_input = ParserInput::new(input, ParserState::default());
            let result = all_consuming(Variable::parse)(parser_input);

            assert_matches!(result, Ok(_));

            let result = result.unwrap();
            assert_eq!(expected, (result.1.name(), result.1.kind()));
        }
    }
}
