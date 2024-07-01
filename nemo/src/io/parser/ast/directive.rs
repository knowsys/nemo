use tower_lsp::lsp_types::SymbolKind;

use super::map::Map;
use super::{ast_to_ascii_tree, AstNode, List, Range, Wsoc};
use crate::io::lexer::{Span, Token};
use ascii_tree::write_tree;

#[derive(Debug, Clone, PartialEq)]
pub enum Directive<'a> {
    // "@base <some://iri.de/> ."
    Base {
        span: Span<'a>,
        doc_comment: Option<Span<'a>>,
        base_iri: Span<'a>,
        dot: Span<'a>,
    },
    // "@prefix wikidata: <http://www.wikidata.org/entity/> ."
    Prefix {
        span: Span<'a>,
        doc_comment: Option<Span<'a>>,
        prefix: Span<'a>,
        prefix_iri: Span<'a>,
        dot: Span<'a>,
    },
    // "@import table :- csv{resource="path/to/file.csv"} ."
    Import {
        span: Span<'a>,
        doc_comment: Option<Span<'a>>,
        predicate: Span<'a>,
        arrow: Span<'a>,
        map: Map<'a>,
        dot: Span<'a>,
    },
    // "@export result :- turtle{resource="out.ttl"} ."
    Export {
        span: Span<'a>,
        doc_comment: Option<Span<'a>>,
        predicate: Span<'a>,
        arrow: Span<'a>,
        map: Map<'a>,
        dot: Span<'a>,
    },
    // "@output A, B, C."
    Output {
        span: Span<'a>,
        doc_comment: Option<Span<'a>>,
        predicates: Option<List<'a, Span<'a>>>,
        dot: Span<'a>,
    },
}
impl AstNode for Directive<'_> {
    fn children(&self) -> Option<Vec<&dyn AstNode>> {
        match self {
            Directive::Base {
                doc_comment,
                base_iri,
                dot,
                ..
            } => {
                let mut vec: Vec<&dyn AstNode> = Vec::new();
                if let Some(dc) = doc_comment {
                    vec.push(dc);
                };
                vec.push(base_iri);
                vec.push(dot);
                Some(vec)
            }
            Directive::Prefix {
                doc_comment,
                prefix,
                prefix_iri,
                dot,
                ..
            } => {
                let mut vec: Vec<&dyn AstNode> = Vec::new();
                if let Some(dc) = doc_comment {
                    vec.push(dc);
                };
                vec.push(prefix);
                vec.push(prefix_iri);
                vec.push(dot);
                Some(vec)
            }
            Directive::Import {
                doc_comment,
                predicate,
                arrow,
                map,
                dot,
                ..
            } => {
                let mut vec: Vec<&dyn AstNode> = Vec::new();
                if let Some(dc) = doc_comment {
                    vec.push(dc);
                };
                vec.push(predicate);
                vec.push(arrow);
                vec.push(map);
                vec.push(dot);
                Some(vec)
            }
            Directive::Export {
                doc_comment,
                predicate,
                arrow,
                map,
                dot,
                ..
            } => {
                let mut vec: Vec<&dyn AstNode> = Vec::new();
                if let Some(dc) = doc_comment {
                    vec.push(dc);
                };
                vec.push(predicate);
                vec.push(arrow);
                vec.push(map);
                vec.push(dot);
                Some(vec)
            }
            Directive::Output {
                span,
                doc_comment,
                predicates,
                dot,
            } => {
                let mut vec: Vec<&dyn AstNode> = Vec::new();
                if let Some(dc) = doc_comment {
                    vec.push(dc);
                };
                if let Some(p) = predicates {
                    vec.push(p);
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

    fn lsp_range_to_rename(&self) -> Option<Range> {
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
