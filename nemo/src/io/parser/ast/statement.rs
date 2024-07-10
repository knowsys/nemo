use tower_lsp::lsp_types::SymbolKind;

use super::atom::Atom;
use super::directive::Directive;
use super::map::Map;
use super::named_tuple::NamedTuple;
use super::{ast_to_ascii_tree, AstNode, List, Position, Range, Wsoc};
use crate::io::lexer::{Span, Token};
use ascii_tree::write_tree;

#[derive(Debug, Clone, PartialEq)]
pub enum Statement<'a> {
    Directive(Directive<'a>),
    Fact {
        span: Span<'a>,
        doc_comment: Option<Span<'a>>,
        fact: Fact<'a>,
        dot: Span<'a>,
    },
    Rule {
        span: Span<'a>,
        doc_comment: Option<Span<'a>>,
        head: List<'a, Atom<'a>>,
        arrow: Span<'a>,
        body: List<'a, Atom<'a>>,
        dot: Span<'a>,
    },
    Comment(Span<'a>),
    Error(Span<'a>),
}
impl AstNode for Statement<'_> {
    fn children(&self) -> Option<Vec<&dyn AstNode>> {
        match self {
            Statement::Directive(directive) => Some(vec![directive]),
            Statement::Fact {
                doc_comment,
                fact: atom,
                dot,
                ..
            } => {
                let mut vec: Vec<&dyn AstNode> = Vec::new();
                if let Some(dc) = doc_comment {
                    vec.push(dc);
                };
                vec.push(atom);
                vec.push(dot);
                Some(vec)
            }
            Statement::Rule {
                doc_comment,
                head,
                arrow,
                body,
                dot,
                ..
            } => {
                let mut vec: Vec<&dyn AstNode> = Vec::new();
                if let Some(dc) = doc_comment {
                    vec.push(dc);
                };
                vec.push(head);
                vec.push(arrow);
                vec.push(body);
                vec.push(dot);
                Some(vec)
            }
            Statement::Comment(c) => Some(vec![c]),
            Statement::Error(t) => Some(vec![t]),
        }
    }

    fn span(&self) -> Span {
        match self {
            Statement::Directive(directive) => directive.span(),
            Statement::Fact { span, .. } => *span,
            Statement::Rule { span, .. } => *span,
            Statement::Comment(c) => c.span(),
            Statement::Error(t) => *t,
        }
    }

    fn is_leaf(&self) -> bool {
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
            Statement::Comment(_) => name!("Comment"),
            Statement::Error(_) => name!("\x1b[1;31mERROR\x1b[0m"),
        }
    }

    fn lsp_identifier(&self) -> Option<(String, String)> {
        Some(("statement".to_string(), "statement".to_string()))
    }

    fn lsp_range_to_rename(&self) -> Option<Range> {
        None
    }

    fn lsp_symbol_info(&self) -> Option<(String, SymbolKind)> {
        let name = match self {
            Statement::Directive(_) => "Directive",
            Statement::Fact { .. } => "Fact",
            Statement::Rule { .. } => "Rule",
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

#[derive(Debug, Clone, PartialEq)]
pub enum Fact<'a> {
    NamedTuple(NamedTuple<'a>),
    Map(Map<'a>),
}
impl AstNode for Fact<'_> {
    fn children(&self) -> Option<Vec<&dyn AstNode>> {
        match self {
            Fact::NamedTuple(named_tuple) => named_tuple.children(),
            Fact::Map(map) => map.children(),
        }
    }

    fn span(&self) -> Span {
        match self {
            Fact::NamedTuple(named_tuple) => named_tuple.span(),
            Fact::Map(map) => map.span(),
        }
    }

    fn is_leaf(&self) -> bool {
        match self {
            Fact::NamedTuple(named_tuple) => named_tuple.is_leaf(),
            Fact::Map(map) => map.is_leaf(),
        }
    }

    fn name(&self) -> String {
        match self {
            Fact::NamedTuple(named_tuple) => named_tuple.name(),
            Fact::Map(map) => map.name(),
        }
    }

    fn lsp_identifier(&self) -> Option<(String, String)> {
        match self {
            Fact::NamedTuple(named_tuple) => named_tuple.lsp_identifier(),
            Fact::Map(map) => map.lsp_identifier(),
        }
    }

    fn lsp_symbol_info(&self) -> Option<(String, SymbolKind)> {
        match self {
            Fact::NamedTuple(named_tuple) => named_tuple.lsp_symbol_info(),
            Fact::Map(map) => map.lsp_symbol_info(),
        }
    }

    fn lsp_range_to_rename(&self) -> Option<Range> {
        match self {
            Fact::NamedTuple(named_tuple) => named_tuple.lsp_range_to_rename(),
            Fact::Map(map) => map.lsp_range_to_rename(),
        }
    }
}

impl std::fmt::Display for Fact<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Fact::NamedTuple(named_tuple) => named_tuple.fmt(f),
            Fact::Map(map) => map.fmt(f),
        }
    }
}
