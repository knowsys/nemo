//! This module defines [Guard].

use nom::sequence::delimited;

use crate::parser::{
    ParserResult,
    context::{ParserContext, context},
    error::deepest,
    input::ParserInput,
};

use super::{
    ProgramAST,
    comment::wsoc::WSoC,
    expression::{Expression, complex::infix::InfixExpression},
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

    fn parse(input: ParserInput<'a>) -> ParserResult<'a, Self>
    where
        Self: Sized + 'a,
    {
        // Parsing the expression before looking for an operator avoids parsing
        // it again as a complex expression when none follows.
        let parse_guard = |input: ParserInput<'a>| {
            let input_span = input.span;
            let (rest, left) = Expression::parse(input.clone())?;

            let infix = delimited(WSoC::parse, InfixExpression::parse_infix_kind, WSoC::parse)(
                rest.clone(),
            )
            .and_then(|(rest, kind)| {
                Expression::parse(rest).map(|(rest, right)| (rest, kind, right))
            });

            match infix {
                Ok((rest, kind, right)) => {
                    let span = input_span.until_rest(&rest.span);

                    Ok((
                        rest,
                        Self::Infix(InfixExpression::new(span, kind, left, right)),
                    ))
                }
                // Without an operator, only a complex expression is a guard.
                Err(infix_error) if !left.is_complex() => Expression::parse_complex(input)
                    .map(|(rest, expression)| (rest, Self::Expression(expression)))
                    .map_err(|error| deepest(infix_error, error)),
                Err(_) => Ok((rest, Self::Expression(left))),
            }
        };

        context(CONTEXT, parse_guard)(input)
    }

    fn context(&self) -> ParserContext {
        CONTEXT
    }
}

#[cfg(test)]
mod test {
    use nom::combinator::all_consuming;

    use crate::parser::{
        ParserState,
        ast::{ProgramAST, guard::Guard},
        context::ParserContext,
        input::ParserInput,
    };

    #[test]
    #[cfg_attr(miri, ignore)]
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
