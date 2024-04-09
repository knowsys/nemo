use super::term::Term;
use super::{AstNode, List};
use crate::io::lexer::{Span, Token};

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct NamedTuple<'a> {
    pub(crate) span: Span<'a>,
    pub(crate) identifier: Token<'a>,
    pub(crate) ws1: Option<Token<'a>>,
    pub(crate) open_paren: Token<'a>,
    pub(crate) ws2: Option<Token<'a>>,
    pub(crate) terms: Option<List<'a, Term<'a>>>,
    pub(crate) ws3: Option<Token<'a>>,
    pub(crate) close_paren: Token<'a>,
}
impl AstNode for NamedTuple<'_> {
    fn children(&self) -> Option<Vec<&dyn AstNode>> {
        let mut vec = Vec::new();
        vec.push(&self.identifier as &dyn AstNode);
        if let Some(ws) = &self.ws1 {
            vec.push(ws);
        }
        vec.push(&self.open_paren);
        if let Some(ws) = &self.ws2 {
            vec.push(ws);
        }
        if let Some(terms) = &self.terms {
            vec.push(terms);
        }
        if let Some(ws) = &self.ws3 {
            vec.push(ws);
        }
        vec.push(&self.close_paren);
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
