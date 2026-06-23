//! This module defines [N3Graph].

use crate::parser::{ParserResult, ast::ProgramAST, context::ParserContext, input::ParserInput, span::Span};

use super::statement::N3Statement;

#[derive(Debug)]
pub struct N3Graph<'a> {
    span: Span<'a>,
    statements: Vec<N3Statement<'a>>,
}

impl<'a> ProgramAST<'a> for N3Graph<'a> {
    fn children(&self) -> Vec<& dyn ProgramAST<'a>> {
        todo!()
    }

    fn span(&self) -> Span<'a> {
        self.span
    }

    fn parse(input: ParserInput<'a>) -> ParserResult<'a, Self> {
        todo!()
    }

    fn context(&self) -> ParserContext {
        todo!()
    }
}

#[cfg(test)]
mod test {
    use std::assert_matches;

    use nom::combinator::all_consuming;

    use crate::parser::{ParserState, ast::{ProgramAST, n3::graph::N3Graph}, input::ParserInput};

    #[test]
    #[cfg_attr(miri, ignore)]
    fn parse_graph() {
        // from the N3 Test suite
        let graph = r#"@prefix : <http://example.com/> .
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
