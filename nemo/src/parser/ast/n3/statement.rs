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

use super::super::{
    ProgramAST,
    attribute::Attribute,
    comment::{doc::DocComment, wsoc::WSoC},
    directive::Directive,
    guard::Guard,
    rule::Rule,
    token::Token,
};

#[allow(clippy::large_enum_variant)]
/// Types of [Statement]s
#[derive(Debug)]
pub enum N3StatementKind<'a> {
    /// Fact
    Fact(Guard<'a>),
    /// Rule
    Rule(Rule<'a>),
    /// Directive
    Directive(Directive<'a>),
    /// This represents a statement, that has an error that could not get recovered in a child node.
    Error(Token<'a>),
}

impl<'a> N3StatementKind<'a> {
    /// Return the [ParserContext] of the underlying statement.
    pub fn context(&self) -> ParserContext {
        match self {
            Self::Fact(statement) => statement.context(),
            Self::Rule(statement) => statement.context(),
            Self::Directive(statement) => statement.context(),
            Self::Error(_statement) => ParserContext::Error,
        }
    }

    /// Parse the [StatementKind].
    pub fn parse(input: ParserInput<'a>) -> ParserResult<'a, Self> {
        context(
            ParserContext::StatementKind,
            alt((
                map(Directive::parse, Self::Directive),
                map(Rule::parse, Self::Rule),
                map(Guard::parse, Self::Fact),
            )),
        )(input)
    }
}

/// Statement in a program
#[derive(Debug)]
pub struct N3Statement<'a> {
    /// [Span] associated with this node
    pub(crate) span: Span<'a>,

    /// Doc comment associated with this statement
    pub(crate) comment: Option<DocComment<'a>>,
    /// The statement
    pub(crate) kind: N3StatementKind<'a>,
    /// Attributes associated with this statement
    pub(crate) attributes: Vec<Attribute<'a>>,
}

impl<'a> N3Statement<'a> {
    /// Return the comment attached to this statement,
    /// if there is any
    pub fn comment(&self) -> Option<&DocComment<'a>> {
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
            N3StatementKind::Fact(statement) => vec![statement],
            N3StatementKind::Rule(statement) => vec![statement],
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
                opt(DocComment::parse),
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

// #[cfg(test)]
// mod test {
//     use nom::combinator::all_consuming;

//     use crate::parser::{
//         ParserState,
//         ast::{ProgramAST, statement::Statement},
//         context::ParserContext,
//         input::ParserInput,
//     };

//     #[test]
//     #[cfg_attr(miri, ignore)]
//     fn parse_statement() {
//         let test = vec![
//             (
//                 "%%% A fact\n%%% with a multiline doc comment. \n a(1, 2) .",
//                 ParserContext::Guard,
//             ),
//             (
//                 "%%% A fact\n%%% with a multiline doc comment. \n #[external(?x)] \n a(1, ?x) .",
//                 ParserContext::Guard,
//             ),
//             (
//                 "%%% A rule \n#[name(\"test\")] \n a(1, 2) :- b(2, 1) .",
//                 ParserContext::Rule,
//             ),
//             ("%%% A rule \n a(1, 2) :- b(2, 1) .", ParserContext::Rule),
//             (
//                 "%%% A directive \n   \t@declare a(_: int, _: int) .",
//                 ParserContext::Directive,
//             ),
//             (
//                 "#[external(?x)] \n @export test :- csv{resource = ?x}.",
//                 ParserContext::Directive,
//             ),
//             (
//                 "@export test :- csv{resource = \"\"}.",
//                 ParserContext::Directive,
//             ),
//         ];

//         for (input, expect) in test {
//             let parser_input = ParserInput::new(input, ParserState::default());
//             let result = all_consuming(Statement::parse)(parser_input);

//             assert!(result.is_ok());

//             let result = result.unwrap();
//             assert_eq!(result.1.kind.context(), expect);
//         }
//     }
// }
