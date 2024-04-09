use super::map::Map;
use super::named_tuple::NamedTuple;
use super::term::Term;
use super::AstNode;
use crate::io::lexer::{Span, Token};

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum Atom<'a> {
    Positive(NamedTuple<'a>),
    Negative {
        span: Span<'a>,
        neg: Token<'a>,
        atom: NamedTuple<'a>,
    },
    InfixAtom {
        span: Span<'a>,
        lhs: Term<'a>,
        ws1: Option<Token<'a>>,
        operation: Token<'a>,
        ws2: Option<Token<'a>>,
        rhs: Term<'a>,
    },
    Map(Map<'a>),
}
impl AstNode for Atom<'_> {
    fn children(&self) -> Option<Vec<&dyn AstNode>> {
        match self {
            Atom::Positive(named_tuple) => named_tuple.children(),
            Atom::Negative { neg, atom, .. } => Some(vec![neg, atom]),
            Atom::InfixAtom {
                lhs,
                ws1,
                operation,
                ws2,
                rhs,
                ..
            } => {
                let mut vec = Vec::new();
                vec.push(lhs as &dyn AstNode);
                if let Some(ws) = ws1 {
                    vec.push(ws);
                };
                vec.push(operation);
                if let Some(ws) = ws2 {
                    vec.push(ws);
                };
                vec.push(rhs);
                Some(vec)
            }
            Atom::Map(map) => map.children(),
        }
    }

    fn span(&self) -> Span {
        match self {
            Atom::Positive(named_tuple) => named_tuple.span(),
            Atom::Negative { span, .. } => *span,
            Atom::InfixAtom { span, .. } => *span,
            Atom::Map(map) => map.span(),
        }
    }

    fn position(&self) -> super::Position {
        let span = self.span();
        super::Position {
            offset: span.location_offset(),
            line: span.location_line(),
            column: span.get_column() as u32,
        }
    }
}
