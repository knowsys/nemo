use tower_lsp::lsp_types::SymbolKind;

use super::term::Term;
use super::{ast_to_ascii_tree, AstNode, List, Position, Range, Wsoc};
use crate::io::lexer::{Span, Token};
use ascii_tree::write_tree;
use std::fmt::Debug;

#[derive(Debug, Clone, PartialEq)]
pub struct Map<'a> {
    pub span: Span<'a>,
    pub identifier: Option<Span<'a>>,
    pub open_brace: Span<'a>,
    pub pairs: Option<List<'a, Pair<'a>>>,
    pub close_brace: Span<'a>,
}
impl AstNode for Map<'_> {
    fn children(&self) -> Option<Vec<&dyn AstNode>> {
        let mut vec: Vec<&dyn AstNode> = Vec::new();
        if let Some(identifier) = &self.identifier {
            vec.push(identifier);
        };
        vec.push(&self.open_brace);
        if let Some(pairs) = &self.pairs {
            vec.push(pairs);
        };
        vec.push(&self.close_brace);
        Some(vec)
    }

    fn span(&self) -> Span {
        self.span
    }

    fn is_leaf(&self) -> bool {
        false
    }

    fn name(&self) -> String {
        String::from("Map")
    }

    fn lsp_identifier(&self) -> Option<(String, String)> {
        None
    }

    fn lsp_range_to_rename(&self) -> Option<Range> {
        None
    }

    fn lsp_symbol_info(&self) -> Option<(String, SymbolKind)> {
        Some((String::from("Map"), SymbolKind::STRUCT))
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
pub struct Pair<'a> {
    pub span: Span<'a>,
    pub key: Term<'a>,
    pub equal: Span<'a>,
    pub value: Term<'a>,
}
impl AstNode for Pair<'_> {
    fn children(&self) -> Option<Vec<&dyn AstNode>> {
        let mut vec: Vec<&dyn AstNode> = Vec::new();
        vec.push(&self.key);
        vec.push(&self.equal);
        vec.push(&self.value);
        Some(vec)
    }

    fn span(&self) -> Span {
        self.span
    }

    fn is_leaf(&self) -> bool {
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

    fn lsp_identifier(&self) -> Option<(String, String)> {
        None
    }

    fn lsp_range_to_rename(&self) -> Option<Range> {
        None
    }

    fn lsp_symbol_info(&self) -> Option<(String, SymbolKind)> {
        Some((String::from("Pair"), SymbolKind::ARRAY))
    }
}
impl std::fmt::Display for Pair<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut output = String::new();
        write_tree(&mut output, &ast_to_ascii_tree(self))?;
        write!(f, "{output}")
    }
}