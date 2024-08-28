use std::fmt::Display;

use crate::io::{lexer::Span, parser::ast::ast_to_ascii_tree};

use super::{tuple::Tuple, AstNode};
use ascii_tree::write_tree;

#[derive(Debug, Clone, PartialEq)]
pub struct NamedTuple<'a> {
    pub span: Span<'a>,
    pub identifier: Span<'a>,
    pub tuple: Tuple<'a>,
}

impl AstNode for NamedTuple<'_> {
    // NOTE: This flattens the tuple children into the vec. An alternative could be
    // vec![&self.identifier, &self.tuple] but then you always have an `Tuple` as an
    // child
    fn children(&self) -> Option<Vec<&dyn AstNode>> {
        let mut vec: Vec<&dyn AstNode> = vec![&self.identifier];
        if let Some(mut children) = self.tuple.children() {
            vec.append(&mut children);
        };
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
            "NamedTuple \x1b[34m@{}:{} \x1b[92m{:?}\x1b[0m",
            self.span.location_line(),
            self.span.get_utf8_column(),
            self.span.fragment()
        )
    }

    fn lsp_identifier(&self) -> Option<(String, String)> {
        // This was todo!() before but we don't want to panic here.
        None
    }

    fn lsp_symbol_info(&self) -> Option<(String, tower_lsp::lsp_types::SymbolKind)> {
        // This was todo!() before but we don't want to panic here.
        None
    }

    fn lsp_range_to_rename(&self) -> Option<super::Range> {
        // This was todo!() before but we don't want to panic here.
        None
    }
}

impl Display for NamedTuple<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut output = String::new();
        write_tree(&mut output, &ast_to_ascii_tree(self))?;
        write!(f, "{output}")
    }
}
