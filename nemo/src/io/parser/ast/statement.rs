use tower_lsp::lsp_types::SymbolKind;

use super::atom::Atom;
use super::directive::Directive;
use super::{ast_to_ascii_tree, AstNode, List, Position, Wsoc};
use crate::io::lexer::{Span, Token};
use ascii_tree::write_tree;

#[derive(Debug, Clone, PartialEq)]
pub enum Statement<'a> {
    Directive(Directive<'a>),
    Fact {
        span: Span<'a>,
        doc_comment: Option<Token<'a>>,
        atom: Atom<'a>,
        ws: Option<Wsoc<'a>>,
        dot: Token<'a>,
    },
    Rule {
        span: Span<'a>,
        doc_comment: Option<Token<'a>>,
        head: List<'a, Atom<'a>>,
        ws1: Option<Wsoc<'a>>,
        arrow: Token<'a>,
        ws2: Option<Wsoc<'a>>,
        body: List<'a, Atom<'a>>,
        ws3: Option<Wsoc<'a>>,
        dot: Token<'a>,
    },
    Whitespace(Token<'a>),
    Comment(Token<'a>),
    Error(Token<'a>),
}
impl AstNode for Statement<'_> {
    fn children(&self) -> Option<Vec<&dyn AstNode>> {
        match self {
            Statement::Directive(directive) => Some(vec![directive]),
            Statement::Fact {
                doc_comment,
                atom,
                ws,
                dot,
                ..
            } => {
                let mut vec: Vec<&dyn AstNode> = Vec::new();
                if let Some(dc) = doc_comment {
                    vec.push(dc);
                };
                vec.push(atom);
                if let Some(ws) = ws {
                    vec.push(ws);
                }
                vec.push(dot);
                Some(vec)
            }
            Statement::Rule {
                doc_comment,
                head,
                ws1,
                arrow,
                ws2,
                body,
                ws3,
                dot,
                ..
            } => {
                let mut vec: Vec<&dyn AstNode> = Vec::new();
                if let Some(dc) = doc_comment {
                    vec.push(dc);
                };
                vec.push(head);
                if let Some(ws) = ws1 {
                    vec.push(ws);
                };
                vec.push(arrow);
                if let Some(ws) = ws2 {
                    vec.push(ws);
                };
                vec.push(body);
                if let Some(ws) = ws3 {
                    vec.push(ws);
                };
                vec.push(dot);
                Some(vec)
            }
            Statement::Whitespace(ws) => Some(vec![ws]),
            Statement::Comment(c) => Some(vec![c]),
            Statement::Error(t) => Some(vec![t]),
        }
    }

    fn span(&self) -> Span {
        match self {
            Statement::Directive(directive) => directive.span(),
            Statement::Fact { span, .. } => *span,
            Statement::Rule { span, .. } => *span,
            Statement::Whitespace(ws) => ws.span(),
            Statement::Comment(c) => c.span(),
            Statement::Error(t) => t.span,
        }
    }

    fn position(&self) -> Position {
        let span = self.span();
        Position {
            offset: span.location_offset(),
            line: span.location_line(),
            column: span.get_utf8_column() as u32,
        }
    }

    fn is_token(&self) -> bool {
        false
    }

    fn name(&self) -> String {
        macro_rules! name {
            ($name:literal) => {
                format!(
                    "{} \x1b[34m@{}:{} \x1b[92m{:?}\x1b[0m",
                    $name,
                    self.span().location_line(),
                    self.span().get_utf8_column(),
                    self.span().fragment()
                )
            };
        }

        match self {
            Statement::Directive(_) => name!("Directive"),
            Statement::Fact { .. } => name!("Fact"),
            Statement::Rule { .. } => name!("Rule"),
            Statement::Whitespace(_) => name!("Whitespace"),
            Statement::Comment(_) => name!("Comment"),
            Statement::Error(_) => name!("\x1b[1;31mERROR\x1b[0m"),
        }
    }

    fn lsp_identifier(&self) -> Option<(String, String)> {
        Some(("statement".to_string(), "statement".to_string()))
    }

    fn lsp_sub_node_to_rename(&self) -> Option<&dyn AstNode> {
        None
    }

    fn lsp_symbol_info(&self) -> Option<(String, SymbolKind)> {
        let name = match self {
            Statement::Directive(_) => "Directive",
            Statement::Fact { .. } => "Fact",
            Statement::Rule { .. } => "Rule",
            Statement::Whitespace(_ws) => return None,
            Statement::Comment(_) => return None,
            Statement::Error(_) => "Invalid",
        };

        Some((String::from(name), SymbolKind::CLASS))
    }
}

impl std::fmt::Display for Statement<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut output = String::new();
        write_tree(&mut output, &ast_to_ascii_tree(self))?;
        write!(f, "{output}")
    }
}
