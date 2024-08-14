//! This module defines [Statement].

use nom::{
    branch::alt,
    combinator::{map, opt},
    sequence::{pair, terminated},
};
use nom_locate::LocatedSpan;

use crate::parser::{
    context::{context, ParserContext},
    input::ParserInput,
    span::Span,
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
    /// This represents a statement, that has an error that could not get recovered in a child node.
    Error(Token<'a>),
}

impl<'a> StatementKind<'a> {
    /// Return the [ParserContext] of the underlying statement.
    pub fn context(&self) -> ParserContext {
        match self {
            StatementKind::Fact(statement) => statement.context(),
            StatementKind::Rule(statement) => statement.context(),
            StatementKind::Directive(statement) => statement.context(),
            StatementKind::Error(_statement) => todo!(),
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
    /// [Span] associated with this node
    pub(crate) span: Span<'a>,

    /// Doc comment associated with this statement
    pub(crate) comment: Option<DocComment<'a>>,
    /// The statement
    pub(crate) kind: StatementKind<'a>,
}

impl<'a> Statement<'a> {
    /// Return the comment attached to this statement,
    /// if there is any
    pub fn comment(&self) -> Option<&DocComment<'a>> {
        self.comment.as_ref()
    }

    /// Return the [StatementKind].
    pub fn kind(&self) -> &StatementKind<'a> {
        &self.kind
    }
}

const CONTEXT: ParserContext = ParserContext::Statement;

impl<'a> ProgramAST<'a> for Statement<'a> {
    fn children(&self) -> Vec<&dyn ProgramAST> {
        match &self.kind {
            StatementKind::Fact(statement) => vec![statement],
            StatementKind::Rule(statement) => vec![statement],
            StatementKind::Directive(statement) => vec![statement],
            StatementKind::Error(_) => vec![],
        }
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
            pair(
                opt(DocComment::parse),
                terminated(StatementKind::parse, pair(WSoC::parse, Token::dot)),
            ),
        )(input)
        .map(|(rest, (comment, statement))| {
            let rest_span = rest.span;

            (
                rest,
                Self {
                    span: input_span.until_rest(&rest_span),
                    comment,
                    kind: statement,
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
    fn parse_statement() {
        let test = vec![
            (
                "/// A fact\n/// with a multiline doc comment. \n a(1, 2) .",
                ParserContext::Expression,
            ),
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
            assert_eq!(result.1.kind.context(), expect);
        }
    }
}
