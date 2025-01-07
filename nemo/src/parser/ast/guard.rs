//! This module defines [Guard].

use nom::{branch::alt, combinator::map};

use crate::parser::context::{context, ParserContext};

use super::{
    expression::{complex::infix::InfixExpression, Expression},
    ProgramAST,
};

/// An expression that is the building block of rules.
#[derive(Debug)]
pub enum Guard<'a> {
    /// A normal expression
    Expression(Expression<'a>),
    /// Infix
    Infix(InfixExpression<'a>),
}

impl Guard<'_> {
    /// Return the [ParserContext] of the underlying expression type.
    pub fn context_type(&self) -> ParserContext {
        match self {
            Guard::Expression(expression) => expression.context_type(),
            Guard::Infix(infix) => infix.context(),
        }
    }
}

const CONTEXT: ParserContext = ParserContext::Guard;

impl<'a> ProgramAST<'a> for Guard<'a> {
    fn children(&self) -> Vec<&dyn ProgramAST<'a>> {
        match self {
            Guard::Expression(expression) => expression.children(),
            Guard::Infix(infix) => infix.children(),
        }
    }

    fn span(&self) -> crate::parser::span::Span<'a> {
        match self {
            Guard::Expression(expression) => expression.span(),
            Guard::Infix(infix) => infix.span(),
        }
    }

    fn parse(input: crate::parser::input::ParserInput<'a>) -> crate::parser::ParserResult<'a, Self>
    where
        Self: Sized + 'a,
    {
        context(
            CONTEXT,
            alt((
                map(InfixExpression::parse, Self::Infix),
                map(Expression::parse_complex, Self::Expression),
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
        ast::{guard::Guard, ProgramAST},
        context::ParserContext,
        input::ParserInput,
        ParserState,
    };

    #[test]
    fn parse_guard() {
        let test = vec![
            ("test(?x, (1,), (1 + 2))", ParserContext::Atom),
            ("2 + 3 = 5", ParserContext::Infix),
        ];

        for (input, expect) in test {
            let parser_input = ParserInput::new(input, ParserState::default());
            let result = all_consuming(Guard::parse)(parser_input);

            assert!(result.is_ok());

            let result = result.unwrap();
            assert_eq!(result.1.context_type(), expect);
        }
    }
}
