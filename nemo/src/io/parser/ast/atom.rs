use tower_lsp::lsp_types::SymbolKind;

use super::map::Map;
use super::named_tuple::NamedTuple;
use super::term::Term;
use super::{ast_to_ascii_tree, AstNode, Range};
use crate::io::lexer::Span;
use ascii_tree::write_tree;

#[derive(Debug, Clone, PartialEq)]
pub enum Atom<'a> {
    Positive(NamedTuple<'a>),
    Negative {
        span: Span<'a>,
        neg: Span<'a>,
        atom: NamedTuple<'a>,
    },
    InfixAtom {
        span: Span<'a>,
        lhs: Term<'a>,
        operation: Span<'a>,
        rhs: Term<'a>,
    },
    Map(Map<'a>),
}

impl Atom<'_> {
    fn named_tuple(&self) -> Option<&NamedTuple<'_>> {
        match &self {
            Atom::Positive(tuple) => Some(tuple),
            Atom::Negative { atom, .. } => Some(atom),
            _ => None,
        }
    }
}

impl AstNode for Atom<'_> {
    fn children(&self) -> Option<Vec<&dyn AstNode>> {
        match self {
            Atom::Positive(named_tuple) => Some(vec![named_tuple]),
            Atom::Negative { neg, atom, .. } => Some(vec![neg, atom]),
            Atom::InfixAtom {
                lhs,
                operation,
                rhs,
                ..
            } => {
                let mut vec: Vec<&dyn AstNode> = Vec::new();
                vec.push(lhs);
                vec.push(operation);
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
            Atom::Positive(_) => name!("Positive Atom"),
            Atom::Negative { .. } => name!("Negative Atom"),
            Atom::InfixAtom { .. } => name!("Infix Atom"),
            Atom::Map(_) => name!("Map Atom"),
        }
    }

    fn lsp_identifier(&self) -> Option<(String, String)> {
        self.named_tuple().map(|named_tuple| {
            (
                format!("atom/{}", named_tuple.identifier.fragment()),
                "file".to_string(),
            )
        })
    }

    fn lsp_range_to_rename(&self) -> Option<Range> {
        self.named_tuple()
            .and_then(|named_tuple| Some(named_tuple.identifier))
            .map(|identifier| identifier.range())
    }

    fn lsp_symbol_info(&self) -> Option<(String, SymbolKind)> {
        match self.named_tuple() {
            Some(tuple) => Some((
                format!("Atom: {}", tuple.identifier.fragment()),
                SymbolKind::FUNCTION,
            )),
            None => Some((String::from("Atom"), SymbolKind::FUNCTION)),
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
