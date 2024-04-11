use super::term::Term;
use super::{ast_to_ascii_tree, AstNode, List, Position};
use crate::io::lexer::{Span, Token};
use ascii_tree::write_tree;

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Tuple<'a> {
    pub(crate) span: Span<'a>,
    pub(crate) identifier: Option<Token<'a>>,
    pub(crate) ws1: Option<Token<'a>>,
    pub(crate) open_paren: Token<'a>,
    pub(crate) ws2: Option<Token<'a>>,
    pub(crate) terms: Option<List<'a, Term<'a>>>,
    pub(crate) ws3: Option<Token<'a>>,
    pub(crate) close_paren: Token<'a>,
}
impl AstNode for Tuple<'_> {
    fn children(&self) -> Option<Vec<&dyn AstNode>> {
        let mut vec = Vec::new();
        #[allow(trivial_casts)]
        if let Some(identifier) = &self.identifier {
            vec.push(identifier as &dyn AstNode);
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
        String::from("Named Tuple")
    }
}
impl std::fmt::Display for Tuple<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut output = String::new();
        write_tree(&mut output, &ast_to_ascii_tree(self))?;
        write!(f, "{output}")
    }
}
