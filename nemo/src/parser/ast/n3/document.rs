//! This module defines [N3Document].

use nom::{
    combinator::opt,
    multi::many0,
    sequence::{delimited, pair, terminated},
};

use crate::parser::{
    ParserErrorReport, ParserResult,
    ast::{
        ProgramAST,
        comment::{doc::DocComment, toplevel::TopLevelComment},
        program::Program,
        statement::{Statement, StatementKind},
        token::Token,
    },
    context::{Notation3Context, ParserContext, context},
    error::{recover_n3, report_error},
    input::ParserInput,
    span::Span,
};

use super::{
    comment::{N3Comment, WSoC},
    statement::{N3Statement, N3StatementKind},
};

/// A Notation3 Graph.
#[derive(Debug)]
pub struct N3Document<'a> {
    span: Span<'a>,
    comment: Option<N3Comment<'a>>,
    statements: Vec<N3Statement<'a>>,
}

const CONTEXT: ParserContext = ParserContext::notation3(Notation3Context::Document);

impl<'a> N3Document<'a> {
    /// Return the top-level comment attached to this graph, if there
    /// is any.
    pub fn comment(&self) -> Option<&N3Comment<'a>> {
        self.comment.as_ref()
    }

    /// Return an iterator of statements in the graph.
    pub fn statements(&self) -> impl Iterator<Item = &N3Statement<'a>> {
        self.statements.iter()
    }

    /// Try to convert into a [Program].
    pub fn try_into_program(self) -> Result<Program<'a>, (Box<N3Document<'a>>, ParserErrorReport)> {
        let comment = self.comment.clone().map(TopLevelComment::from);
        let mut statements = Vec::new();

        for statement in self.statements {
            let span = statement.span;
            let comment = statement.comment.clone().map(DocComment::from);
            let attributes = Vec::new();
            match statement.kind {
                N3StatementKind::Triples(n3_triples) => {
                    statements.extend(n3_triples.into_statements());
                }
                N3StatementKind::Directive(n3_directive) => statements.push(Statement {
                    kind: StatementKind::Directive(n3_directive.into_inner()),
                    span,
                    comment,
                    attributes,
                }),
                N3StatementKind::Error(token) => statements.push(Statement {
                    kind: StatementKind::Error(token.clone()),
                    span,
                    comment,
                    attributes,
                }),
            };
        }

        Ok(Program::from_span_comment_and_statements(
            self.span, comment, statements,
        ))
    }
}

impl<'a> ProgramAST<'a> for N3Document<'a> {
    fn children(&self) -> Vec<&dyn ProgramAST<'a>> {
        let mut result = Vec::<&dyn ProgramAST>::new();

        if let Some(comment) = self.comment() {
            result.push(comment);
        }

        for statement in self.statements() {
            result.push(statement);
        }

        result
    }

    fn span(&self) -> Span<'a> {
        self.span
    }

    fn parse(input: ParserInput<'a>) -> ParserResult<'a, Self> {
        let input_span = input.span;

        context(
            CONTEXT,
            pair(
                opt(N3Comment::parse),
                many0(delimited(
                    WSoC::parse,
                    terminated(
                        recover_n3(report_error(N3Statement::parse)),
                        delimited(WSoC::parse, Token::dot, WSoC::parse),
                    ),
                    WSoC::parse,
                )),
            ),
        )(input)
        .map(|(rest, (comment, statements))| {
            let rest_span = rest.span;

            (
                rest,
                Self {
                    span: input_span.until_rest(&rest_span),
                    comment,
                    statements,
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
        ast::{ProgramAST, n3::document::N3Document},
        input::ParserInput,
    };

    #[test]
    #[cfg_attr(miri, ignore)]
    fn parse_document() {
        let graph = r#"
          ## from the N3 test suite
          @prefix : <http://example.com/> .
          :william a :Person .
          :doerthe a :Person .
          { ?x a :Person } => { ?x a :Animal } .
        "#;

        let input = ParserInput::new(graph, ParserState::default());
        let result = all_consuming(N3Document::parse)(input);

        assert_matches!(result, Ok(_));

        let result = result.unwrap();
        assert_eq!(result.1.statements.len(), 2);
    }
}
