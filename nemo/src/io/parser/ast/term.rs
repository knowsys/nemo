use super::map::Map;
use super::tuple::Tuple;
use super::{ast_to_ascii_tree, AstNode, List, Position};
use crate::io::lexer::{Span, Token};
use ascii_tree::write_tree;

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum Term<'a> {
    Primitive(Token<'a>),
    Variable(Token<'a>),
    Existential(Token<'a>),
    // TODO: Is whitespace needed? Figure out how unary terms look
    UnaryPrefix {
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
    Tuple(Box<Tuple<'a>>),
    Map(Box<Map<'a>>),
}
impl AstNode for Term<'_> {
    fn children(&self) -> Option<Vec<&dyn AstNode>> {
        match self {
            Term::Primitive(token) => Some(vec![token]),
            Term::Variable(token) => Some(vec![token]),
            Term::Existential(token) => Some(vec![token]),
            Term::UnaryPrefix {
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
            Term::Tuple(named_tuple) => named_tuple.children(),
            Term::Map(map) => map.children(),
        }
    }

    fn span(&self) -> Span {
        match self {
            Term::Primitive(t) => t.span(),
            Term::Variable(t) => t.span(),
            Term::Existential(t) => t.span(),
            Term::UnaryPrefix { span, .. } => *span,
            Term::Binary { span, .. } => *span,
            Term::Aggregation { span, .. } => *span,
            Term::Tuple(named_tuple) => named_tuple.span(),
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
            Term::Primitive(_) => name!("Primitive"),
            Term::Variable(_) => name!("Variable"),
            Term::Existential(_) => name!("Existential Variable"),
            Term::UnaryPrefix { .. } => name!("Unary Term"),
            Term::Binary { .. } => name!("Binary Term"),
            Term::Aggregation { .. } => name!("Aggregation"),
            Term::Tuple(f) => {
                if let Some(_) = f.identifier {
                    name!("Function Symbol")
                } else {
                    name!("Tuple")
                }
            }
            Term::Map(_) => name!("Map"),
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
