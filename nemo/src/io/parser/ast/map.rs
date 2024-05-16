use super::term::Term;
use super::{ast_to_ascii_tree, AstNode, List, Position};
use crate::io::lexer::{Span, Token};
use ascii_tree::write_tree;
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
        let mut vec: Vec<&dyn AstNode> = Vec::new();
        if let Some(identifier) = &self.identifier {
            vec.push(identifier);
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

    fn is_token(&self) -> bool {
        false
    }

    fn name(&self) -> String {
        String::from("Map")
    }
}
impl std::fmt::Display for Map<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut output = String::new();
        write_tree(&mut output, &ast_to_ascii_tree(self))?;
        write!(f, "{output}")
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
        let mut vec: Vec<&dyn AstNode> = Vec::new();
        vec.push(&self.key);
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

    fn is_token(&self) -> bool {
        false
    }

    fn name(&self) -> String {
        format!(
            "Pair \x1b[34m@{}:{} \x1b[92m{:?}\x1b[0m",
            self.span.location_line(),
            self.span.get_utf8_column(),
            self.span.fragment()
        )
    }
}
impl<K: AstNode + Debug, V: AstNode + Debug> std::fmt::Display for Pair<'_, K, V> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut output = String::new();
        write_tree(&mut output, &ast_to_ascii_tree(self))?;
        write!(f, "{output}")
    }
}
