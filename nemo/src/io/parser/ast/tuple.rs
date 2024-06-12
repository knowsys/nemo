use tower_lsp::lsp_types::SymbolKind;

use super::term::Term;
use super::{ast_to_ascii_tree, AstNode, List, Position, Range, Wsoc};
use crate::io::lexer::{Span, Token};
use ascii_tree::write_tree;

#[derive(Debug, Clone, PartialEq)]
pub struct Tuple<'a> {
    pub span: Span<'a>,
    pub identifier: Option<Token<'a>>,
    pub ws1: Option<Wsoc<'a>>,
    pub open_paren: Token<'a>,
    pub ws2: Option<Wsoc<'a>>,
    pub terms: Option<List<'a, Term<'a>>>,
    pub ws3: Option<Wsoc<'a>>,
    pub close_paren: Token<'a>,
}

impl AstNode for Tuple<'_> {
    fn children(&self) -> Option<Vec<&dyn AstNode>> {
        let mut vec: Vec<&dyn AstNode> = Vec::new();
        if let Some(identifier) = &self.identifier {
            vec.push(identifier);
        }
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

    fn is_token(&self) -> bool {
        false
    }

    fn name(&self) -> String {
        format!(
            "Tuple \x1b[34m@{}:{} \x1b[92m{:?}\x1b[0m",
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
        None
    }
}
impl std::fmt::Display for Tuple<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut output = String::new();
        write_tree(&mut output, &ast_to_ascii_tree(self))?;
        write!(f, "{output}")
    }
}
