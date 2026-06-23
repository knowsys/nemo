//! This module defines [N3Variable].

use nom::{combinator::cut, sequence::preceded};

use crate::parser::{
    ParserResult,
    ast::{
        ProgramAST,
        expression::basic::variable::{Variable, VariableType},
        token::Token,
    },
    context::{Notation3Context, ParserContext, context},
    input::ParserInput,
    span::Span,
};

/// A Notation3 variable
#[derive(Clone, Debug)]
pub struct N3Variable<'a> {
    span: Span<'a>,

    name: Token<'a>,
}

impl<'a> N3Variable<'a> {
    /// Return the name of the variable
    pub fn name(&self) -> String {
        self.name.to_string()
    }

    /// Return the type of variable
    pub fn kind(&self) -> VariableType {
        VariableType::Universal
    }
}

impl<'a> From<N3Variable<'a>> for Variable<'a> {
    fn from(value: N3Variable<'a>) -> Self {
        Variable::universal_from_span_and_name(value.span, value.name)
    }
}

const CONTEXT: ParserContext = ParserContext::notation3(Notation3Context::Variable);

impl<'a> ProgramAST<'a> for N3Variable<'a> {
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
            preceded(Token::universal_indicator, cut(Token::name)),
        )(input)
        .map(|(rest, name)| {
            let rest_span = rest.span;

            (
                rest,
                Self {
                    span: input_span.until_rest(&rest_span),
                    name,
                },
            )
        })
    }

    fn context(&self) -> ParserContext {
        CONTEXT
    }
}
