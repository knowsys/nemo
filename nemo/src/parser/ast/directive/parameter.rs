//! This module defines the [ParameterDeclaration] directive.

use nom::{
    combinator::opt,
    sequence::{delimited, pair, preceded, tuple},
};

use crate::parser::{
    ParserResult,
    ast::{
        ProgramAST,
        comment::wsoc::WSoC,
        expression::{Expression, basic::variable::Variable},
        token::Token,
    },
    context::{ParserContext, context},
    input::ParserInput,
    span::Span,
};

/// Parameter directive, indicating a global parameter (i.e. global variable)
#[derive(Debug)]
pub struct ParameterDeclaration<'a> {
    /// [Span] associated with this node
    span: Span<'a>,
    /// [Variable] to identify this parameter
    variable: Variable<'a>,
    /// [Expression] assigned to this variable
    expression: Option<Expression<'a>>,
}

impl<'a> ParameterDeclaration<'a> {
    fn parse_body(
        input: ParserInput<'a>,
    ) -> ParserResult<'a, (Variable<'a>, Option<Expression<'a>>)> {
        context(
            ParserContext::ParameterDeclBody,
            pair(
                delimited(WSoC::parse, Variable::parse, WSoC::parse),
                opt(preceded(
                    tuple((Token::equal, WSoC::parse)),
                    Expression::parse,
                )),
            ),
        )(input)
    }

    pub fn variable(&self) -> &Variable<'a> {
        &self.variable
    }

    pub fn expression(&self) -> Option<&Expression<'a>> {
        self.expression.as_ref()
    }
}

const CONTEXT: ParserContext = ParserContext::ParameterDecl;

impl<'a> ProgramAST<'a> for ParameterDeclaration<'a> {
    fn children(&self) -> Vec<&dyn ProgramAST<'a>> {
        vec![]
    }

    fn span(&self) -> Span<'a> {
        self.span
    }

    fn parse(input: ParserInput<'a>) -> ParserResult<'a, Self>
    where
        Self: Sized + 'a,
    {
        let input_span = input.span;

        let (rest, (variable, expression)) = context(
            CONTEXT,
            preceded(
                tuple((
                    Token::directive_indicator,
                    Token::directive_parameter,
                    WSoC::parse,
                )),
                Self::parse_body,
            ),
        )(input)?;

        let span = input_span.until_rest(&rest.span);
        Ok((
            rest,
            Self {
                span,
                variable,
                expression,
            },
        ))
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
        ast::{ProgramAST, directive::parameter::ParameterDeclaration},
        input::ParserInput,
    };

    #[test]
    fn parse_parameter() {
        let test = vec![
            (
                "@parameter $foo = 5 + $bar",
                "foo".to_string(),
                Some("5 + $bar".to_string()),
            ),
            ("@parameter $test", "test".to_string(), None),
        ];

        for (input, expected_variable, expected_value) in test {
            let parser_input = ParserInput::new(input, ParserState::default());
            let Ok((_, result)) = all_consuming(ParameterDeclaration::parse)(parser_input) else {
                panic!("cannot parse test directive {input:?}")
            };

            assert_eq!(Some(expected_variable), result.variable().name());
            assert_eq!(
                expected_value,
                result.expression().map(|e| (*e.span()).to_string())
            );
        }
    }
}
