use tower_lsp::lsp_types::SymbolKind;

use super::map::Map;
use super::{ast_to_ascii_tree, AstNode, List, Position, Wsoc};
use crate::io::lexer::{Span, Token};
use ascii_tree::write_tree;

#[derive(Debug, Clone, PartialEq)]
pub enum Directive<'a> {
    // "@base <some://iri.de/> ."
    Base {
        span: Span<'a>,
        doc_comment: Option<Token<'a>>,
        kw: Token<'a>,
        ws1: Option<Wsoc<'a>>,
        base_iri: Token<'a>,
        ws2: Option<Wsoc<'a>>,
        dot: Token<'a>,
    },
    // "@prefix wikidata: <http://www.wikidata.org/entity/> ."
    Prefix {
        span: Span<'a>,
        doc_comment: Option<Token<'a>>,
        kw: Token<'a>,
        ws1: Option<Wsoc<'a>>,
        prefix: Token<'a>,
        ws2: Option<Wsoc<'a>>,
        prefix_iri: Token<'a>,
        ws3: Option<Wsoc<'a>>,
        dot: Token<'a>,
    },
    // "@import table :- csv{resource="path/to/file.csv"} ."
    Import {
        span: Span<'a>,
        doc_comment: Option<Token<'a>>,
        kw: Token<'a>,
        ws1: Wsoc<'a>,
        predicate: Token<'a>,
        ws2: Option<Wsoc<'a>>,
        arrow: Token<'a>,
        ws3: Option<Wsoc<'a>>,
        map: Map<'a>,
        ws4: Option<Wsoc<'a>>,
        dot: Token<'a>,
    },
    // "@export result :- turtle{resource="out.ttl"} ."
    Export {
        span: Span<'a>,
        doc_comment: Option<Token<'a>>,
        kw: Token<'a>,
        ws1: Wsoc<'a>,
        predicate: Token<'a>,
        ws2: Option<Wsoc<'a>>,
        arrow: Token<'a>,
        ws3: Option<Wsoc<'a>>,
        map: Map<'a>,
        ws4: Option<Wsoc<'a>>,
        dot: Token<'a>,
    },
    // "@output A, B, C."
    Output {
        span: Span<'a>,
        doc_comment: Option<Token<'a>>,
        kw: Token<'a>,
        ws1: Wsoc<'a>,
        predicates: Option<List<'a, Token<'a>>>,
        ws2: Option<Wsoc<'a>>,
        dot: Token<'a>,
    },
}
impl AstNode for Directive<'_> {
    fn children(&self) -> Option<Vec<&dyn AstNode>> {
        match self {
            Directive::Base {
                doc_comment,
                kw,
                ws1,
                base_iri,
                ws2,
                dot,
                ..
            } => {
                let mut vec: Vec<&dyn AstNode> = Vec::new();
                if let Some(dc) = doc_comment {
                    vec.push(dc);
                };
                vec.push(kw);
                if let Some(ws) = ws1 {
                    vec.push(ws);
                };
                vec.push(base_iri);
                if let Some(ws) = ws2 {
                    vec.push(ws);
                };
                vec.push(dot);
                Some(vec)
            }
            Directive::Prefix {
                doc_comment,
                kw,
                ws1,
                prefix,
                ws2,
                prefix_iri,
                ws3,
                dot,
                ..
            } => {
                let mut vec: Vec<&dyn AstNode> = Vec::new();
                if let Some(dc) = doc_comment {
                    vec.push(dc);
                };
                vec.push(kw);
                if let Some(ws) = ws1 {
                    vec.push(ws);
                };
                vec.push(prefix);
                if let Some(ws) = ws2 {
                    vec.push(ws);
                };
                vec.push(prefix_iri);
                if let Some(ws) = ws3 {
                    vec.push(ws);
                };
                vec.push(dot);
                Some(vec)
            }
            Directive::Import {
                doc_comment,
                kw,
                ws1,
                predicate,
                ws2,
                arrow,
                ws3,
                map,
                ws4,
                dot,
                ..
            } => {
                let mut vec: Vec<&dyn AstNode> = Vec::new();
                if let Some(dc) = doc_comment {
                    vec.push(dc);
                };
                vec.push(kw);
                vec.push(ws1);
                vec.push(predicate);
                if let Some(ws) = ws2 {
                    vec.push(ws);
                };
                vec.push(arrow);
                if let Some(ws) = ws3 {
                    vec.push(ws);
                };
                vec.push(map);
                if let Some(ws) = ws4 {
                    vec.push(ws);
                };
                vec.push(dot);
                Some(vec)
            }
            Directive::Export {
                doc_comment,
                kw,
                ws1,
                predicate,
                ws2,
                arrow,
                ws3,
                map,
                ws4,
                dot,
                ..
            } => {
                let mut vec: Vec<&dyn AstNode> = Vec::new();
                if let Some(dc) = doc_comment {
                    vec.push(dc);
                };
                vec.push(kw);
                vec.push(ws1);
                vec.push(predicate);
                if let Some(ws) = ws2 {
                    vec.push(ws);
                };
                vec.push(arrow);
                if let Some(ws) = ws3 {
                    vec.push(ws);
                };
                vec.push(map);
                if let Some(ws) = ws4 {
                    vec.push(ws);
                };
                vec.push(dot);
                Some(vec)
            }
            Directive::Output {
                span,
                doc_comment,
                kw,
                ws1,
                predicates,
                ws2,
                dot,
            } => {
                let mut vec: Vec<&dyn AstNode> = Vec::new();
                if let Some(dc) = doc_comment {
                    vec.push(dc);
                };
                vec.push(kw);
                vec.push(ws1);
                if let Some(p) = predicates {
                    vec.push(p);
                };
                if let Some(ws) = ws2 {
                    vec.push(ws);
                };
                vec.push(dot);
                Some(vec)
            }
        }
    }

    fn span(&self) -> Span {
        match self {
            Directive::Base { span, .. } => *span,
            Directive::Prefix { span, .. } => *span,
            Directive::Import { span, .. } => *span,
            Directive::Export { span, .. } => *span,
            Directive::Output { span, .. } => *span,
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
            Directive::Base { .. } => name!("Base Directive"),
            Directive::Prefix { .. } => name!("Prefix Directive"),
            Directive::Import { .. } => name!("Import Directive"),
            Directive::Export { .. } => name!("Export Directive"),
            Directive::Output { .. } => name!("Output Directive"),
        }
    }

    fn lsp_identifier(&self) -> Option<(String, String)> {
        None
    }

    fn lsp_sub_node_to_rename(&self) -> Option<&dyn AstNode> {
        None
    }

    fn lsp_symbol_info(&self) -> Option<(String, SymbolKind)> {
        Some((String::from("Directive"), SymbolKind::FUNCTION))
    }
}

impl std::fmt::Display for Directive<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut output = String::new();
        write_tree(&mut output, &ast_to_ascii_tree(self))?;
        write!(f, "{output}")
    }
}
