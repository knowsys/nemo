use super::atom::Atom;
use super::directive::Directive;
use super::{AstNode, List, Position};
use crate::io::lexer::{Span, Token};

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum Statement<'a> {
    Directive(Directive<'a>),
    Fact {
        span: Span<'a>,
        doc_comment: Option<Token<'a>>,
        atom: Atom<'a>,
        ws: Option<Token<'a>>,
        dot: Token<'a>,
    },
    Rule {
        span: Span<'a>,
        doc_comment: Option<Token<'a>>,
        head: List<'a, Atom<'a>>,
        ws1: Option<Token<'a>>,
        arrow: Token<'a>,
        ws2: Option<Token<'a>>,
        body: List<'a, Atom<'a>>,
        ws3: Option<Token<'a>>,
        dot: Token<'a>,
    },
    Whitespace(Token<'a>),
    Comment(Token<'a>),
}
impl AstNode for Statement<'_> {
    fn children(&self) -> Option<Vec<&dyn AstNode>> {
        match self {
            Statement::Directive(directive) => directive.children(),
            Statement::Fact {
                doc_comment,
                atom,
                ws,
                dot,
                ..
            } => {
                let mut vec = Vec::new();
                if let Some(dc) = doc_comment {
                    vec.push(dc as &dyn AstNode);
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
                let mut vec = Vec::new();
                if let Some(dc) = doc_comment {
                    vec.push(dc as &dyn AstNode);
                };
                vec.push(head as &dyn AstNode);
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
        }
    }

    fn span(&self) -> Span {
        match self {
            Statement::Directive(directive) => directive.span(),
            Statement::Fact { span, .. } => *span,
            Statement::Rule { span, .. } => *span,
            Statement::Whitespace(ws) => ws.span(),
            Statement::Comment(c) => c.span(),
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
}
