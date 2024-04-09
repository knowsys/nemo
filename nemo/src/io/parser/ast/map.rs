use super::term::Term;
use super::{AstNode, List, Position};
use crate::io::lexer::{Span, Token};
use std::fmt::Debug;

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Map<'a> {
    pub(crate) span: Span<'a>,
    pub(crate) identifier: Option<Token<'a>>,
    pub(crate) ws1: Option<Token<'a>>,
    pub(crate) open_brace: Token<'a>,
    pub(crate) ws2: Option<Token<'a>>,
    pub(crate) pairs: Option<List<'a, Pair<'a, Term<'a>, Term<'a>>>>,
    pub(crate) ws3: Option<Token<'a>>,
    pub(crate) close_brace: Token<'a>,
}
impl AstNode for Map<'_> {
    fn children(&self) -> Option<Vec<&dyn AstNode>> {
        let mut vec = Vec::new();
        if let Some(identifier) = &self.identifier {
            vec.push(identifier as &dyn AstNode);
        };
        if let Some(ws) = &self.ws1 {
            vec.push(ws);
        }
        vec.push(&self.open_brace);
        if let Some(ws) = &self.ws2 {
            vec.push(ws);
        }
        if let Some(pairs) = &self.pairs {
            vec.push(pairs);
        };
        if let Some(ws) = &self.ws3 {
            vec.push(ws);
        }
        vec.push(&self.close_brace);
        Some(vec)
    }

    fn span(&self) -> Span {
        self.span
    }

    fn position(&self) -> Position {
        Position {
            offset: self.span.location_offset(),
            line: self.span.location_line(),
            column: self.span.get_utf8_column() as u32,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Pair<'a, K, V> {
    pub(crate) span: Span<'a>,
    pub(crate) key: K,
    pub(crate) ws1: Option<Token<'a>>,
    pub(crate) equal: Token<'a>,
    pub(crate) ws2: Option<Token<'a>>,
    pub(crate) value: V,
}
impl<K: AstNode + Debug, V: AstNode + Debug> AstNode for Pair<'_, K, V> {
    fn children(&self) -> Option<Vec<&dyn AstNode>> {
        let mut vec = Vec::new();
        vec.push(&self.key as &dyn AstNode);
        if let Some(ws) = &self.ws1 {
            vec.push(ws);
        }
        vec.push(&self.equal);
        if let Some(ws) = &self.ws2 {
            vec.push(ws);
        }
        vec.push(&self.value);
        Some(vec)
    }

    fn span(&self) -> Span {
        self.span
    }

    fn position(&self) -> Position {
        Position {
            offset: self.span.location_offset(),
            line: self.span.location_line(),
            column: self.span.get_utf8_column() as u32,
        }
    }
}
