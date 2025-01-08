//! This module defines [Variable]
#![allow(missing_docs)]

use enum_assoc::Assoc;
use nom::{branch::alt, combinator::opt, sequence::pair};

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

    /// Parse the variable prefix
    fn parse_variable_prefix(input: ParserInput<'a>) -> ParserResult<'a, VariableType> {
        alt((
            Token::universal_indicator,
            Token::existential_indicator,
            Token::underscore,
        ))(input)
        .map(|(rest, result)| {
            (
                rest,
                VariableType::token(result.kind()).expect("unknown token"),
            )
        })
    }

    /// Parse the name of the variable
    fn parse_variable_name(input: ParserInput<'a>) -> ParserResult<'a, Token<'a>> {
        Token::name(input)
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
            pair(Self::parse_variable_prefix, opt(Self::parse_variable_name)),
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

    use crate::parser::{
        ast::{expression::basic::variable::Variable, ProgramAST},
        input::ParserInput,
        ParserState,
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
            ("?", (None, VariableType::Universal)),
        ];

        for (input, expected) in test {
            let parser_input = ParserInput::new(input, ParserState::default());
            let result = all_consuming(Variable::parse)(parser_input);

            assert!(result.is_ok());

            let result = result.unwrap();
            assert_eq!(expected, (result.1.name(), result.1.kind()));
        }
    }
}
