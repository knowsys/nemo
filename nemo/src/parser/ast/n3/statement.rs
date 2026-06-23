//! This module defines [N3Statement].

use nom::{
    branch::alt,
    combinator::{map, opt},
    multi::many0,
    sequence::{delimited, pair, tuple},
};

use crate::parser::{
    ParserResult,
    context::{ParserContext, context},
    input::ParserInput,
    span::Span,
};

use super::{
    super::{
        ProgramAST,
        attribute::Attribute,
        n3::comment::{N3Comment, WSoC},
        rule::Rule,
        token::Token,
    },
    triple::N3Triple,
};

use super::directive::N3Directive;

#[allow(clippy::large_enum_variant)]
/// Types of [Statement]s
#[derive(Debug)]
pub enum N3StatementKind<'a> {
    /// Triple
    Triple(N3Triple<'a>),
    // /// Rule
    // Rule(Rule<'a>),
    /// Directive
    Directive(N3Directive<'a>),
    /// This represents a statement, that has an error that could not get recovered in a child node.
    Error(Token<'a>),
}

impl<'a> N3StatementKind<'a> {
    /// Return the [ParserContext] of the underlying statement.
    pub fn context(&self) -> ParserContext {
        match self {
            Self::Triple(statement) => statement.context(),
            // Self::Rule(statement) => statement.context(),
            Self::Directive(statement) => statement.context(),
            Self::Error(_statement) => ParserContext::Error,
        }
    }

    /// Parse the [StatementKind].
    pub fn parse(input: ParserInput<'a>) -> ParserResult<'a, Self> {
        context(
            ParserContext::StatementKind,
            alt((
                map(N3Directive::parse, Self::Directive),
                // map(Rule::parse, Self::Rule),
                map(N3Triple::parse, Self::Triple),
            )),
        )(input)
    }
}

/// Statement in a program
#[derive(Debug)]
pub struct N3Statement<'a> {
    /// [Span] associated with this node
    pub(crate) span: Span<'a>,

    /// Comment associated with this statement
    pub(crate) comment: Option<N3Comment<'a>>,
    /// The statement
    pub(crate) kind: N3StatementKind<'a>,
    /// Attributes associated with this statement
    pub(crate) attributes: Vec<Attribute<'a>>,
}

impl<'a> N3Statement<'a> {
    /// Return the comment attached to this statement,
    /// if there is any
    pub fn comment(&self) -> Option<&N3Comment<'a>> {
        self.comment.as_ref()
    }

    /// Return the [N3StatementKind].
    pub fn kind(&self) -> &N3StatementKind<'a> {
        &self.kind
    }

    /// Return the attributes attached to this statement
    pub fn attributes(&self) -> &[Attribute<'a>] {
        &self.attributes
    }
}

const CONTEXT: ParserContext = ParserContext::Statement;

impl<'a> ProgramAST<'a> for N3Statement<'a> {
    fn children(&self) -> Vec<&dyn ProgramAST<'a>> {
        match &self.kind {
            N3StatementKind::Triple(statement) => vec![statement],
            // N3StatementKind::Rule(statement) => vec![statement],
            N3StatementKind::Directive(statement) => vec![statement],
            N3StatementKind::Error(_) => vec![],
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
            tuple((
                opt(N3Comment::parse),
                WSoC::parse,
                many0(Attribute::parse),
                delimited(
                    WSoC::parse,
                    N3StatementKind::parse,
                    pair(WSoC::parse, Token::dot),
                ),
            )),
        )(input)
        .map(|(rest, (comment, _, attributes, statement))| {
            let rest_span = rest.span;

            (
                rest,
                Self {
                    span: input_span.until_rest(&rest_span),
                    comment,
                    attributes,
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
    use std::assert_matches;
    use test_log::test;

    use crate::parser::{
        ParserState,
        ast::{
            ProgramAST,
            n3::statement::{N3Statement, N3StatementKind},
        },
        input::ParserInput,
    };

    #[test]
    #[cfg_attr(miri, ignore)]
    fn parse_triple() {
        let triple = r#":doerthe a :Person ."#;

        let parser_input = ParserInput::new(triple, ParserState::default());
        let result = N3StatementKind::parse(parser_input);

        assert_matches!(result, Ok(_));

        let parser_input = ParserInput::new(triple, ParserState::default());
        let result = all_consuming(N3Statement::parse)(parser_input);

        assert_matches!(result, Ok(_));
    }

    #[test]
    #[cfg_attr(miri, ignore)]
    fn parse_prefix() {
        let prefix = r#"@prefix : <http://example.com/> ."#;

        let parser_input = ParserInput::new(prefix, ParserState::default());
        let result = N3StatementKind::parse(parser_input);

        assert_matches!(result, Ok(_));

        let parser_input = ParserInput::new(prefix, ParserState::default());
        let result = all_consuming(N3Statement::parse)(parser_input);

        assert_matches!(result, Ok(_));
    }
}
