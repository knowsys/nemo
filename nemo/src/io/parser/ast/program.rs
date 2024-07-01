use tower_lsp::lsp_types::SymbolKind;

use super::{ast_to_ascii_tree, statement::Statement, AstNode, Position, Range};
use crate::io::lexer::{Span, Token};
use ascii_tree::write_tree;

#[derive(Debug, Clone, PartialEq)]
pub struct Program<'a> {
    pub span: Span<'a>,
    pub tl_doc_comment: Option<Span<'a>>,
    pub statements: Vec<Statement<'a>>,
}
impl AstNode for Program<'_> {
    fn children(&self) -> Option<Vec<&dyn AstNode>> {
        let mut vec: Vec<&dyn AstNode> = Vec::new();
        if let Some(dc) = &self.tl_doc_comment {
            vec.push(dc);
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

    fn is_token(&self) -> bool {
        false
    }

    fn name(&self) -> String {
        if self.span.fragment().len() < 60 {
            format!(
                "Program \x1b[34m@{}:{} \x1b[92m{:?}\x1b[0m",
                self.span.location_line(),
                self.span.get_utf8_column(),
                &self.span.fragment(),
            )
        } else {
            format!(
                "Program \x1b[34m@{}:{} \x1b[92m{:?}[…]\x1b[0m",
                self.span.location_line(),
                self.span.get_utf8_column(),
                &self.span.fragment()[..60],
            )
        }
    }

    fn lsp_identifier(&self) -> Option<(String, String)> {
        None
    }

    fn lsp_range_to_rename(&self) -> Option<Range> {
        None
    }

    fn lsp_symbol_info(&self) -> Option<(String, SymbolKind)> {
        Some(("File".to_string(), SymbolKind::FILE))
    }
}

impl std::fmt::Display for Program<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut output = String::new();
        write_tree(&mut output, &ast_to_ascii_tree(self))?;
        write!(f, "{output}")
    }
}
