use tower_lsp::lsp_types::SymbolKind;

use super::map::Map;
use super::named_tuple::NamedTuple;
use super::tuple::Tuple;
use super::{ast_to_ascii_tree, AstNode, List, Range, Wsoc};
use crate::io::lexer::{Span, Token};
use ascii_tree::write_tree;

#[derive(Debug, Clone, PartialEq)]
pub enum Term<'a> {
    Primitive(Primitive<'a>),
    UniversalVariable(Span<'a>),
    ExistentialVariable(Span<'a>),
    // TODO: Is whitespace needed? Figure out how unary terms look
    UnaryPrefix {
        span: Span<'a>,
        operation: Span<'a>,
        term: Box<Term<'a>>,
    },
    Binary {
        span: Span<'a>,
        lhs: Box<Term<'a>>,
        operation: Span<'a>,
        rhs: Box<Term<'a>>,
    },
    Aggregation {
        span: Span<'a>,
        operation: Span<'a>,
        open_paren: Span<'a>,
        terms: Box<List<'a, Term<'a>>>,
        close_paren: Span<'a>,
    },
    Tuple(Box<Tuple<'a>>),
    NamedTuple(Box<NamedTuple<'a>>),
    Map(Box<Map<'a>>),
    Blank(Span<'a>),
}

impl AstNode for Term<'_> {
    fn children(&self) -> Option<Vec<&dyn AstNode>> {
        match self {
            Term::Primitive(token) => Some(vec![token]),
            Term::UniversalVariable(token) => Some(vec![token]),
            Term::ExistentialVariable(token) => Some(vec![token]),
            Term::UnaryPrefix {
                operation, term, ..
            } => Some(vec![operation, &**term]),
            Term::Binary {
                lhs,
                operation,
                rhs,
                ..
            } => {
                let mut vec: Vec<&dyn AstNode> = Vec::new();
                vec.push(&**lhs);
                vec.push(operation);
                vec.push(&**rhs);
                Some(vec)
            }
            Term::Aggregation {
                operation,
                open_paren,
                terms,
                close_paren,
                ..
            } => {
                let mut vec: Vec<&dyn AstNode> = Vec::new();
                vec.push(operation);
                vec.push(open_paren);
                vec.push(&**terms);
                vec.push(close_paren);
                Some(vec)
            }
            // TODO: check whether directly the children or Some(vec![named_tuple]) should get returned (for fidelity in ast)
            Term::Tuple(tuple) => tuple.children(),
            Term::NamedTuple(named_tuple) => named_tuple.children(),
            Term::Map(map) => map.children(),
            Term::Blank(token) => Some(vec![token]),
        }
    }

    fn span(&self) -> Span {
        match self {
            Term::Primitive(p) => p.span(),
            Term::UniversalVariable(span) => *span,
            Term::ExistentialVariable(span) => *span,
            Term::UnaryPrefix { span, .. } => *span,
            Term::Binary { span, .. } => *span,
            Term::Aggregation { span, .. } => *span,
            Term::Tuple(tuple) => tuple.span(),
            Term::NamedTuple(named_tuple) => named_tuple.span(),
            Term::Map(map) => map.span(),
            Term::Blank(span) => *span,
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
            Term::UniversalVariable(_) => name!("Variable"),
            Term::ExistentialVariable(_) => name!("Existential Variable"),
            Term::UnaryPrefix { .. } => name!("Unary Term"),
            Term::Binary { .. } => name!("Binary Term"),
            Term::Aggregation { .. } => name!("Aggregation"),
            Term::Tuple(_) => name!("Tuple"),
            Term::NamedTuple(_) => name!("Function"),
            Term::Map(_) => name!("Map"),
            Term::Blank(_) => name!("Blank"),
        }
    }

    fn lsp_identifier(&self) -> Option<(String, String)> {
        match self {
            Term::UniversalVariable(t) => Some((
                format!("variable/{}", t.span().fragment()),
                "statement".to_string(),
            )),
            Term::Aggregation { operation, .. } => Some((
                format!("aggregation/{}", operation.span().fragment()),
                "file".to_string(),
            )),
            Term::NamedTuple(named_tuple) => Some((
                format!("function/{}", named_tuple.identifier.span().fragment()),
                "file".to_string(),
            )),
            _ => None,
        }
    }

    fn lsp_range_to_rename(&self) -> Option<Range> {
        match self {
            Term::Primitive(_) => None,
            Term::UniversalVariable(t) => Some(t.range()),
            Term::UnaryPrefix { .. } => None,
            Term::Blank { .. } => None,
            Term::ExistentialVariable(t) => Some(t.range()),
            Term::Binary { .. } => None,
            Term::Aggregation { operation, .. } => Some(operation.range()),
            Term::Tuple(_) => None,
            Term::NamedTuple(named_tuple) => Some(named_tuple.identifier.range()),
            Term::Map(_map) => None,
        }
    }

    fn lsp_symbol_info(&self) -> Option<(String, SymbolKind)> {
        match self {
            Term::Primitive(_) => Some((String::from("Primitive term"), SymbolKind::CONSTANT)),
            Term::UniversalVariable(t) => {
                Some((format!("Variable: {}", t.span()), SymbolKind::VARIABLE))
            }
            Term::UnaryPrefix { .. } => Some((String::from("Unary prefix"), SymbolKind::OPERATOR)),
            Term::Blank { .. } => Some((String::from("Unary prefix"), SymbolKind::VARIABLE)),
            Term::ExistentialVariable { .. } => {
                Some((String::from("Existential"), SymbolKind::VARIABLE))
            }
            Term::Binary { .. } => Some((String::from("Binary term"), SymbolKind::OPERATOR)),
            Term::Aggregation { operation, .. } => Some((
                format!("Aggregation: {}", operation.fragment()),
                SymbolKind::OPERATOR,
            )),
            Term::Tuple(_) => Some((String::from("Tuple"), SymbolKind::ARRAY)),
            Term::NamedTuple(named_tuple) => Some((
                format!("Function: {}", named_tuple.identifier.fragment()),
                SymbolKind::OPERATOR,
            )),
            Term::Map(_map) => Some((String::from("Map"), SymbolKind::ARRAY)),
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

#[derive(Debug, Clone, PartialEq)]
pub enum Primitive<'a> {
    Constant(Span<'a>),
    PrefixedConstant {
        span: Span<'a>,
        prefix: Option<Span<'a>>,
        colon: Span<'a>,
        constant: Span<'a>,
    },
    Number {
        span: Span<'a>,
        sign: Option<Span<'a>>,
        before: Option<Span<'a>>,
        dot: Option<Span<'a>>,
        after: Span<'a>,
        exponent: Option<Exponent<'a>>,
    },
    String(Span<'a>),
    Iri(Span<'a>),
    RdfLiteral {
        span: Span<'a>,
        string: Span<'a>,
        carets: Span<'a>,
        iri: Span<'a>,
    },
}

impl AstNode for Primitive<'_> {
    fn children(&self) -> Option<Vec<&dyn AstNode>> {
        match self {
            Primitive::Constant(token) => Some(vec![token]),
            Primitive::PrefixedConstant {
                prefix,
                colon,
                constant,
                ..
            } => {
                let mut vec: Vec<&dyn AstNode> = Vec::new();
                if let Some(prefix) = prefix {
                    vec.push(prefix);
                }
                vec.push(colon);
                vec.push(constant);
                Some(vec)
            }
            Primitive::Number {
                sign,
                before,
                dot,
                after,
                exponent,
                ..
            } => {
                let mut vec: Vec<&dyn AstNode> = Vec::new();
                if let Some(s) = sign {
                    vec.push(s);
                }
                if let Some(b) = before {
                    vec.push(b);
                }
                if let Some(d) = dot {
                    vec.push(d);
                }
                vec.push(after);
                if let Some(exp) = exponent {
                    if let Some(mut children) = exp.children() {
                        vec.append(&mut children);
                    }
                }
                Some(vec)
            }
            Primitive::String(token) => Some(vec![token]),
            Primitive::Iri(token) => Some(vec![token]),
            Primitive::RdfLiteral {
                string,
                carets,
                iri,
                ..
            } => Some(vec![string, carets, iri]),
        }
    }

    fn span(&self) -> Span {
        match self {
            Primitive::Constant(span) => *span,
            Primitive::PrefixedConstant { span, .. } => *span,
            Primitive::Number { span, .. } => *span,
            Primitive::String(span) => *span,
            Primitive::Iri(span) => *span,
            Primitive::RdfLiteral { span, .. } => *span,
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
            Primitive::Constant(_) => name!("Constant"),
            Primitive::PrefixedConstant { .. } => name!("Prefixed Constant"),
            Primitive::Number { .. } => name!("Number"),
            Primitive::String(_) => name!("String"),
            Primitive::Iri(_) => name!("Iri"),
            Primitive::RdfLiteral { .. } => name!("RDF Literal"),
        }
    }

    fn lsp_identifier(&self) -> Option<(String, String)> {
        None
    }

    fn lsp_range_to_rename(&self) -> Option<Range> {
        None
    }

    fn lsp_symbol_info(&self) -> Option<(String, SymbolKind)> {
        None
    }
}
impl std::fmt::Display for Primitive<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut output = String::new();
        write_tree(&mut output, &ast_to_ascii_tree(self))?;
        write!(f, "{output}")
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Exponent<'a> {
    pub(crate) e: Span<'a>,
    pub(crate) sign: Option<Span<'a>>,
    pub(crate) number: Span<'a>,
}

impl AstNode for Exponent<'_> {
    fn children(&self) -> Option<Vec<&dyn AstNode>> {
        let mut vec: Vec<&dyn AstNode> = Vec::new();
        vec.push(&self.e);
        if let Some(s) = &self.sign {
            vec.push(s);
        };
        vec.push(&self.number);
        Some(vec)
    }

    fn span(&self) -> Span {
        todo!()
    }

    fn is_token(&self) -> bool {
        todo!()
    }

    fn name(&self) -> String {
        todo!()
    }

    fn lsp_identifier(&self) -> Option<(String, String)> {
        None
    }

    fn lsp_range_to_rename(&self) -> Option<Range> {
        None
    }

    fn lsp_symbol_info(&self) -> Option<(String, SymbolKind)> {
        None
    }
}

impl std::fmt::Display for Exponent<'_> {
    fn fmt(&self, _f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}
