//! This module defines [N3Graph].

use nom::{
    combinator::opt,
    multi::many0,
    sequence::{delimited, pair},
};

use crate::parser::{
    ParserResult,
    ast::ProgramAST,
    context::{ParserContext, context},
    input::ParserInput,
    span::Span,
};

use super::{
    comment::{N3Comment, WSoC},
    statement::N3Statement,
};

/// A Notation3 Graph.
#[derive(Debug)]
pub struct N3Graph<'a> {
    span: Span<'a>,
    comment: Option<N3Comment<'a>>,
    statements: Vec<N3Statement<'a>>,
}

const CONTEXT: ParserContext = ParserContext::Program; // TODO(mx): add custom context

impl<'a> N3Graph<'a> {
    /// Return the top-level comment attached to this graph, if there
    /// is any.
    pub fn comment(&self) -> Option<&N3Comment<'a>> {
        self.comment.as_ref()
    }

    /// Return an iterator of statements in the graph.
    pub fn statements(&self) -> impl Iterator<Item = &N3Statement<'a>> {
        self.statements.iter()
    }
}

impl<'a> ProgramAST<'a> for N3Graph<'a> {
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
                    //                    recover(report_error(N3Statement::parse)),
                    N3Statement::parse,
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
        todo!()
    }
}

#[cfg(test)]
mod test {
    use nom::combinator::all_consuming;
    use std::assert_matches;
    use test_log::test;

    use crate::parser::{
        ParserState,
        ast::{ProgramAST, n3::graph::N3Graph},
        input::ParserInput,
    };

    #[test]
    #[cfg_attr(miri, ignore)]
    fn parse_graph() {
        let graph = r#"
          ## from the N3 test suite
          @prefix : <http://example.com/> .
          :william a :Person .
          :doerthe a :Person .
          { ?x a :Person } => { ?x a :Animal } .
        "#;

        let parser_input = ParserInput::new(graph, ParserState::default());
        let result = all_consuming(N3Graph::parse)(parser_input);

        assert_matches!(result, Ok(_));

        let result = result.unwrap();
        assert_eq!(result.1.statements.len(), 3);
    }
}
