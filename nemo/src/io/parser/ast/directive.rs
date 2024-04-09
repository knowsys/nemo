use super::AstNode;
use super::{map::Map, Position};
use crate::io::lexer::{Span, Token};

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum Directive<'a> {
    // "@base <some://iri.de/> ."
    Base {
        span: Span<'a>,
        doc_comment: Option<Token<'a>>,
        kw: Token<'a>,
        ws1: Option<Token<'a>>,
        base_iri: Token<'a>,
        ws2: Option<Token<'a>>,
        dot: Token<'a>,
    },
    // "@prefix wikidata: <http://www.wikidata.org/entity/> ."
    Prefix {
        span: Span<'a>,
        doc_comment: Option<Token<'a>>,
        kw: Token<'a>,
        ws1: Option<Token<'a>>,
        prefix: Token<'a>,
        ws2: Option<Token<'a>>,
        prefix_iri: Token<'a>,
        ws3: Option<Token<'a>>,
        dot: Token<'a>,
    },
    // "@import table :- csv{resource="path/to/file.csv"} ."
    Import {
        span: Span<'a>,
        doc_comment: Option<Token<'a>>,
        kw: Token<'a>,
        ws1: Token<'a>,
        predicate: Token<'a>,
        ws2: Option<Token<'a>>,
        arrow: Token<'a>,
        ws3: Option<Token<'a>>,
        map: Map<'a>,
        ws4: Option<Token<'a>>,
        dot: Token<'a>,
    },
    // "@export result :- turtle{resource="out.ttl"} ."
    Export {
        span: Span<'a>,
        doc_comment: Option<Token<'a>>,
        kw: Token<'a>,
        ws1: Token<'a>,
        predicate: Token<'a>,
        ws2: Option<Token<'a>>,
        arrow: Token<'a>,
        ws3: Option<Token<'a>>,
        map: Map<'a>,
        ws4: Option<Token<'a>>,
        dot: Token<'a>,
    },
    // maybe will get deprecated
    Output {
        span: Span<'a>,
        doc_comment: Option<Token<'a>>,
        kw: Token<'a>,
        predicates: Vec<Token<'a>>,
    },
}
impl AstNode for Directive<'_> {
    fn children(&self) -> Option<Vec<&dyn AstNode>> {
        match self {
            Directive::Base {
                span,
                doc_comment,
                kw,
                ws1,
                base_iri,
                ws2,
                dot,
            } => {
                let mut vec = Vec::new();
                if let Some(dc) = doc_comment {
                    vec.push(dc as &dyn AstNode);
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
                span,
                doc_comment,
                kw,
                ws1,
                prefix,
                ws2,
                prefix_iri,
                ws3,
                dot,
            } => {
                let mut vec = Vec::new();
                if let Some(dc) = doc_comment {
                    vec.push(dc as &dyn AstNode);
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
                span,
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
            } => {
                let mut vec = Vec::new();
                if let Some(dc) = doc_comment {
                    vec.push(dc as &dyn AstNode);
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
                span,
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
            } => {
                let mut vec = Vec::new();
                if let Some(dc) = doc_comment {
                    vec.push(dc as &dyn AstNode);
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
            Directive::Output { .. } => todo!(),
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
}
