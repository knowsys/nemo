//! This module defines [Statement].

use nom::{
    branch::alt,
    character::complete::line_ending,
    combinator::{map, opt},
    sequence::{delimited, pair, terminated},
};

use crate::parser::{
    context::{context, ParserContext},
    input::ParserInput,
    span::ProgramSpan,
    ParserResult,
};

use super::{
    comment::{doc::DocComment, wsoc::WSoC},
    directive::Directive,
    expression::Expression,
    rule::Rule,
    token::Token,
    ProgramAST,
};

/// Types of [Statement]s
#[derive(Debug)]
pub enum StatementKind<'a> {
    /// Fact
    Fact(Expression<'a>),
    /// Rule
    Rule(Rule<'a>),
    /// Directive
    Directive(Directive<'a>),
}

impl<'a> StatementKind<'a> {
    /// Return the [ParserContext] of the underlying statement.
    pub fn context(&self) -> ParserContext {
        match self {
            StatementKind::Fact(statement) => statement.context(),
            StatementKind::Rule(statement) => statement.context(),
            StatementKind::Directive(statement) => statement.context(),
        }
    }

    /// Parse the [StatementKind].
    pub fn parse(input: ParserInput<'a>) -> ParserResult<'a, Self> {
        alt((
            map(Directive::parse, Self::Directive),
            map(Rule::parse, Self::Rule),
            map(Expression::parse, Self::Fact),
        ))(input)
    }
}

/// Statement in a program
#[derive(Debug)]
pub struct Statement<'a> {
    /// [ProgramSpan] associated with this node
    span: ProgramSpan<'a>,

    /// Doc comment associated with this statement
    comment: Option<DocComment<'a>>,
    /// The statement
    statement: StatementKind<'a>,
}

impl<'a> Statement<'a> {
    /// Return the comment attached to this statement,
    /// if there is any
    pub fn comment(&self) -> Option<&DocComment<'a>> {
        self.comment.as_ref()
    }

    /// Return the [StatementKind].
    pub fn statement(&self) -> &StatementKind<'a> {
        &self.statement
    }
}

const CONTEXT: ParserContext = ParserContext::Statement;

impl<'a> ProgramAST<'a> for Statement<'a> {
    fn children(&self) -> Vec<&dyn ProgramAST> {
        vec![match &self.statement {
            StatementKind::Fact(statement) => statement,
            StatementKind::Rule(statement) => statement,
            StatementKind::Directive(statement) => statement,
        }]
    }

    fn span(&self) -> ProgramSpan<'a> {
        self.span
    }

    fn parse(input: ParserInput<'a>) -> ParserResult<'a, Self>
    where
        Self: Sized + 'a,
    {
        let input_span = input.span;

        context(
            CONTEXT,
            pair(
                opt(terminated(DocComment::parse, line_ending)),
                delimited(
                    WSoC::parse,
                    StatementKind::parse,
                    pair(WSoC::parse, Token::dot),
                ),
            ),
        )(input)
        .map(|(rest, (comment, statement))| {
            let rest_span = rest.span;

            (
                rest,
                Self {
                    span: input_span.until_rest(&rest_span),
                    comment,
                    statement,
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
        ast::{statement::Statement, ProgramAST},
        context::ParserContext,
        input::ParserInput,
        ParserState,
    };

    #[test]
    fn parse_directive() {
        let test = vec![
            ("/// A fact \n a(1, 2) .", ParserContext::Expression),
            ("/// A rule \n a(1, 2) :- b(2, 1) .", ParserContext::Rule),
            (
                "/// A directive \n   \t@declare a(_: int, _: int) .",
                ParserContext::Directive,
            ),
        ];

        for (input, expect) in test {
            let parser_input = ParserInput::new(input, ParserState::default());
            let result = all_consuming(Statement::parse)(parser_input);

            assert!(result.is_ok());

            let result = result.unwrap();
            assert_eq!(result.1.statement.context(), expect);
        }
    }
}
