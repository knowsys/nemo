use super::map::Map;
use super::named_tuple::NamedTuple;
use super::{ast_to_ascii_tree, AstNode, List, Position};
use crate::io::lexer::{Span, Token};
use ascii_tree::write_tree;

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum Term<'a> {
    Primitive(Token<'a>),
    Variable(Token<'a>),
    // TODO: Is whitespace needed? Figure out how unary terms look
    Unary {
        span: Span<'a>,
        operation: Token<'a>,
        term: Box<Term<'a>>,
    },
    Binary {
        span: Span<'a>,
        lhs: Box<Term<'a>>,
        ws1: Option<Token<'a>>,
        operation: Token<'a>,
        ws2: Option<Token<'a>>,
        rhs: Box<Term<'a>>,
    },
    Aggregation {
        span: Span<'a>,
        operation: Token<'a>,
        open_paren: Token<'a>,
        ws1: Option<Token<'a>>,
        terms: Box<List<'a, Term<'a>>>,
        ws2: Option<Token<'a>>,
        close_paren: Token<'a>,
    },
    Function(Box<NamedTuple<'a>>),
    Map(Box<Map<'a>>),
}
impl AstNode for Term<'_> {
    fn children(&self) -> Option<Vec<&dyn AstNode>> {
        match self {
            Term::Primitive(token) => Some(vec![token]),
            Term::Variable(token) => Some(vec![token]),
            Term::Unary {
                operation, term, ..
            } => Some(vec![operation, &**term]),
            Term::Binary {
                lhs,
                ws1,
                operation,
                ws2,
                rhs,
                ..
            } => {
                let mut vec = Vec::new();
                #[allow(trivial_casts)]
                vec.push(&**lhs as &dyn AstNode);
                if let Some(ws) = ws1 {
                    vec.push(ws);
                };
                vec.push(operation);
                if let Some(ws) = ws2 {
                    vec.push(ws);
                };
                vec.push(&**rhs);
                Some(vec)
            }
            Term::Aggregation {
                operation,
                open_paren,
                ws1,
                terms,
                ws2,
                close_paren,
                ..
            } => {
                let mut vec = Vec::new();
                #[allow(trivial_casts)]
                vec.push(operation as &dyn AstNode);
                vec.push(open_paren);
                if let Some(ws) = ws1 {
                    vec.push(ws);
                }
                vec.push(&**terms);
                if let Some(ws) = ws2 {
                    vec.push(ws);
                }
                vec.push(close_paren);
                Some(vec)
            }
            Term::Function(named_tuple) => named_tuple.children(),
            Term::Map(map) => map.children(),
        }
    }

    fn span(&self) -> Span {
        match self {
            Term::Primitive(t) => t.span(),
            Term::Variable(t) => t.span(),
            Term::Unary { span, .. } => *span,
            Term::Binary { span, .. } => *span,
            Term::Aggregation { span, .. } => *span,
            Term::Function(named_tuple) => named_tuple.span(),
            Term::Map(map) => map.span(),
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
            Term::Primitive(_) => "Primitive".into(),
            Term::Variable(_) => "Variable".into(),
            Term::Unary { .. } => "Unary Term".into(),
            Term::Binary { .. } => "Binary Term".into(),
            Term::Aggregation { .. } => "Aggregation".into(),
            Term::Function(_) => "Function Symbol".into(),
            Term::Map(_) => "Map".into(),
        }
    }
}
impl std::fmt::Display for Term<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut output = String::new();
        write_tree(&mut output, &ast_to_ascii_tree(self))?;
        write!(f, "{output}")
    }
}
