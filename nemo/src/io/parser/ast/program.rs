use super::statement::Statement;
use super::AstNode;
use crate::io::lexer::{Span, Token};

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Program<'a> {
    pub(crate) span: Span<'a>,
    pub(crate) tl_doc_comment: Option<Token<'a>>,
    pub(crate) statements: Vec<Statement<'a>>,
}
impl AstNode for Program<'_> {
    fn children(&self) -> Option<Vec<&dyn AstNode>> {
        let mut vec = Vec::new();
        if let Some(dc) = &self.tl_doc_comment {
            vec.push(dc as &dyn AstNode);
        };
        // NOTE: The current implementation puts the doc comment and all the
        // statements in the same vec, so there is no need to implement AstNode
        // for Vec<T>, which would be hard for the fn span() implementation
        for statement in &self.statements {
            vec.push(statement);
        }
        Some(vec)
    }

    fn span(&self) -> Span {
        self.span
    }

    fn position(&self) -> Position {
        Position {
            offset: self.span.location_offset(),
            line: self.span.location_line(),
            column: self.span.get_column() as u32,
        }
    }
}
