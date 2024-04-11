use super::term::Term;
use super::tuple::Tuple;
use super::{ast_to_ascii_tree, AstNode};
use super::{map::Map, Position};
use crate::io::lexer::{Span, Token};
use ascii_tree::write_tree;

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum Atom<'a> {
    Positive(Tuple<'a>),
    Negative {
        span: Span<'a>,
        neg: Token<'a>,
        atom: Tuple<'a>,
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
            Atom::Positive(named_tuple) => Some(vec![named_tuple]),
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
                #[allow(trivial_casts)]
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
            Atom::Map(map) => Some(vec![map]),
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
        match self {
            Atom::Positive(_) => "Positive Atom".into(),
            Atom::Negative { .. } => "Negative Atom".into(),
            Atom::InfixAtom { .. } => "Infix Atom".into(),
            Atom::Map(_) => "Map Atom".into(),
        }
    }
}
impl std::fmt::Display for Atom<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut output = String::new();
        write_tree(&mut output, &ast_to_ascii_tree(self))?;
        write!(f, "{output}")
    }
}
