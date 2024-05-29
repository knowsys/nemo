use nom::Offset;
use tower_lsp::lsp_types::SymbolKind;

use crate::io::lexer::{Span, Token};
use ascii_tree::{write_tree, Tree};
use std::fmt::Display;

pub(crate) mod atom;
pub(crate) mod directive;
pub(crate) mod map;
pub mod program;
pub(crate) mod statement;
pub(crate) mod term;
pub(crate) mod tuple;

pub trait AstNode: std::fmt::Debug + Display + Sync {
    fn children(&self) -> Option<Vec<&dyn AstNode>>;
    fn span(&self) -> Span;
    fn position(&self) -> Position;
    fn is_token(&self) -> bool;

    fn name(&self) -> String;

    /// Returns an optional pair of the identfier and identifier scope.
    ///
    /// The identifier scope will scope this identifier up to any [`AstNode`]
    /// that has an identifier that has this node's identifier scope as a prefix.
    ///
    /// This can be used to restict rename operations to be local, e.g. for variable idenfiers inside of rules.
    fn lsp_identifier(&self) -> Option<(String, String)>;
    fn lsp_symbol_info(&self) -> Option<(String, SymbolKind)>;
    fn lsp_sub_node_to_rename(&self) -> Option<&dyn AstNode>;
}

#[derive(Debug, Clone, Copy, Hash)]
pub struct Position {
    pub offset: usize,
    pub line: u32,
    pub column: u32,
}
impl PartialEq for Position {
    fn eq(&self, other: &Self) -> bool {
        self.offset.eq(&other.offset)
    }
}
impl Eq for Position {}
impl PartialOrd for Position {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.offset.partial_cmp(&other.offset)
    }
}
impl Ord for Position {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.offset.cmp(&other.offset)
    }
}
impl Default for Position {
    fn default() -> Self {
        Position {
            offset: 0,
            line: 1,
            column: 1,
        }
    }
}

/// Whitespace or Comment token
#[derive(Debug, Clone, PartialEq)]
pub struct Wsoc<'a> {
    pub span: Span<'a>,
    pub token: Vec<Token<'a>>,
}
impl AstNode for Wsoc<'_> {
    fn children(&self) -> Option<Vec<&dyn AstNode>> {
        if self.token.is_empty() {
            None
        } else {
            #[allow(trivial_casts)]
            Some(self.token.iter().map(|t| t as &dyn AstNode).collect())
        }
    }

    fn span(&self) -> Span {
        self.span
    }

    fn position(&self) -> Position {
        Position {
            offset: self.span.location_offset(),
            line: self.span.location_line(),
            column: self.span.get_utf8_column() as u32,
        }
    }

    fn is_token(&self) -> bool {
        false
    }

    fn name(&self) -> String {
        format!(
            "Wsoc \x1b[34m@{}:{} \x1b[92m{:?}\x1b[0m",
            self.span.location_line(),
            self.span.get_utf8_column(),
            self.span.fragment()
        )
    }

    fn lsp_identifier(&self) -> Option<(String, String)> {
        None
    }

    fn lsp_sub_node_to_rename(&self) -> Option<&dyn AstNode> {
        None
    }

    fn lsp_symbol_info(&self) -> Option<(String, SymbolKind)> {
        None
    }
}

impl Display for Wsoc<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct List<'a, T> {
    pub span: Span<'a>,
    pub first: T,
    // ([ws]?[,][ws]?[T])*
    pub rest: Option<Vec<(Option<Wsoc<'a>>, Token<'a>, Option<Wsoc<'a>>, T)>>,
}
impl<T: Clone> List<'_, T> {
    pub fn to_vec(&self) -> Vec<T> {
        let mut vec = Vec::new();
        vec.push(self.first.clone());
        if let Some(rest) = &self.rest {
            for (_, _, _, item) in rest {
                vec.push(item.clone());
            }
        }
        vec
    }
}
impl<T> IntoIterator for List<'_, T> {
    type Item = T;

    type IntoIter = std::vec::IntoIter<T>;

    fn into_iter(self) -> Self::IntoIter {
        let mut vec = Vec::new();
        vec.push(self.first);
        if let Some(rest) = self.rest {
            for (_, _, _, item) in rest {
                vec.push(item);
            }
        }
        vec.into_iter()
    }
}
impl<T: AstNode + std::fmt::Debug> AstNode for List<'_, T> {
    fn children(&self) -> Option<Vec<&dyn AstNode>> {
        let mut vec: Vec<&dyn AstNode> = Vec::new();
        vec.push(&self.first);
        if let Some(rest) = &self.rest {
            for (ws1, delim, ws2, item) in rest {
                if let Some(ws) = ws1 {
                    vec.push(ws);
                };
                vec.push(delim);
                if let Some(ws) = ws2 {
                    vec.push(ws);
                };
                vec.push(item);
            }
        };
        Some(vec)
    }

    fn span(&self) -> Span {
        self.span
    }

    fn position(&self) -> Position {
        Position {
            offset: self.span.location_offset(),
            line: self.span.location_line(),
            column: self.span.get_utf8_column() as u32,
        }
    }

    fn is_token(&self) -> bool {
        false
    }

    fn name(&self) -> String {
        format!(
            "List \x1b[34m@{}:{} \x1b[92m{:?}\x1b[0m",
            self.span.location_line(),
            self.span.get_utf8_column(),
            self.span.fragment()
        )
    }

    fn lsp_identifier(&self) -> Option<(String, String)> {
        None
    }

    fn lsp_sub_node_to_rename(&self) -> Option<&dyn AstNode> {
        None
    }

    fn lsp_symbol_info(&self) -> Option<(String, SymbolKind)> {
        Some((String::from("List"), SymbolKind::ARRAY))
    }
}

impl<T: AstNode + std::fmt::Debug> Display for List<'_, T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut output = String::new();
        write_tree(&mut output, &ast_to_ascii_tree(self))?;
        write!(f, "{output}")
    }
}

pub(crate) fn get_all_tokens(node: &dyn AstNode) -> Vec<&dyn AstNode> {
    let mut vec = Vec::new();
    if let Some(children) = node.children() {
        for child in children {
            vec.append(&mut get_all_tokens(child));
        }
    } else {
        vec.push(node);
    };
    vec
}

pub(crate) fn ast_to_ascii_tree(node: &dyn AstNode) -> Tree {
    let mut vec = Vec::new();
    if let Some(children) = node.children() {
        for child in children {
            if child.is_token() {
                vec.push(Tree::Leaf(vec![format!("{}", child)]));
            } else {
                vec.push(ast_to_ascii_tree(child));
            }
        }
    }
    Tree::Node(node.name(), vec)
}

mod test {
    use super::*;
    use super::{
        atom::Atom,
        directive::Directive,
        program::Program,
        statement::Statement,
        term::{Primitive, Term},
        tuple::Tuple,
    };
    use crate::io::lexer::{Span, TokenKind};

    macro_rules! s {
        ($offset:literal,$line:literal,$str:literal) => {
            unsafe { Span::new_from_raw_offset($offset, $line, $str, ()) }
        };
    }

    #[test]
    fn ast_traversal() {
        let input = "\
            %! This is just a test file.\n\
            %! So the documentation of the rules is not important.\n\
            %% This is the prefix used for datatypes\n\
            @prefix xsd: <http://www.w3.org/2001/XMLSchema#>.\n\
            \n\
            % Facts\n\
            %% This is just an example predicate.\n\
            somePredicate(ConstA, ConstB).\n\
            \n\
            % Rules\n\
            %% This is just an example rule.\n\
            someHead(?VarA) :- somePredicate(?VarA, ConstB).   % all constants that are in relation with ConstB\n";
        let span = Span::new(input);
        let ast = Program {
            span,
            tl_doc_comment: Some(Token {
                kind: TokenKind::TlDocComment,
                span: s!(0, 1, "%! This is just a test file.\n%! So the documentation of the rules is not important.\n")
            }),
            statements: vec![
                Statement::Directive(Directive::Prefix {
                    span:s!(125,4,"@prefix xsd: <http://www.w3.org/2001/XMLSchema#>."),
                    doc_comment:Some(Token {
                        kind:TokenKind::DocComment,
                        span:s!(84,3,"%% This is the prefix used for datatypes\n")
                    }),
                    kw: Token{
                        kind:TokenKind::Prefix,
                        span:s!(125,4,"@prefix")
                    },
                    ws1:Some(Wsoc {span: s!(132, 4, " "), token: vec![Token{kind:TokenKind::Whitespace,span:s!(132,4," ")}] }),
                    prefix: Token {
                        kind: TokenKind::PrefixIdent,
                        span: s!(133, 4, "xsd:"),
                    },
                    ws2: Some(Wsoc {span: s!(137, 4, " "), token: vec![Token{kind:TokenKind::Whitespace,span:s!(137,4," ")}] }),
                    prefix_iri: Token {
                        kind: TokenKind::Iri,
                        span: s!(138, 4, "<http://www.w3.org/2001/XMLSchema#>"),
                    },
                    ws3: None,
                    dot: Token{
                        kind:TokenKind::Dot,
                        span:s!(173,4,".")
                    }
                }),
                Statement::Whitespace(Token {
                    kind: TokenKind::Whitespace,
                    span: s!(174, 4, "\n\n"),
                }),
                Statement::Comment(Token {
                    kind: TokenKind::Comment,
                    span: s!(176, 6, "% Facts\n"),
                }),
                Statement::Fact {
                    span:s!(222,8,"somePredicate(ConstA, ConstB)."),
                    doc_comment: Some(Token {
                        kind: TokenKind::DocComment,
                        span:s!(184,7,"%% This is just an example predicate.\n")
                    }),
                    atom: Atom::Positive(Tuple {
                        span: s!(222,8,"somePredicate(ConstA, ConstB)"),
                        identifier: Some(Token {
                            kind: TokenKind::Ident,
                            span: s!(222, 8, "somePredicate"),
                        }),
                         ws1:None ,
                        open_paren:Token{
                            kind:TokenKind::OpenParen,
                            span:s!(235,8,"(")
                        } ,
                         ws2:None ,
                        terms: Some(List {
                            span: s!(236, 8, "ConstA, ConstB"),
                            first: Term::Primitive(Primitive::Constant(Token {
                                kind: TokenKind::Ident,
                                span: s!(236, 8, "ConstA"),
                            })),
                            rest: Some(vec![(
                                None,
                                Token {
                                    kind: TokenKind::Comma,
                                    span: s!(242, 8, ","),
                                },
                                Some(Wsoc {span: s!(243, 8, " "), token: vec![Token{kind:TokenKind::Whitespace,span:s!(243,8," "),}] }),
                                Term::Primitive(Primitive::Constant(Token {
                                    kind: TokenKind::Ident,
                                    span: s!(244, 8, "ConstB"),
                                })),
                            )]),
                        }),
                        ws3: None ,
                        close_paren:Token {
                            kind: TokenKind::CloseParen,
                            span:s!(250,8,")")
                        }
                    }),
                    ws: None,
                    dot: Token {
                        kind: TokenKind::Dot,
                        span: s!(251,8,".")
                    }
                },
                Statement::Whitespace(Token {
                    kind: TokenKind::Whitespace,
                    span: s!(252, 8, "\n\n"),
                }),
                Statement::Comment(Token {
                    kind: TokenKind::Comment,
                    span: s!(254, 10, "% Rules\n"),
                }),
                Statement::Rule {
                    span: s!(295,12,"someHead(?VarA) :- somePredicate(?VarA, ConstB)."),
                    doc_comment: Some(Token { kind: TokenKind::DocComment, span: s!(262,11,"%% This is just an example rule.\n") }),
                    head: List {
                        span: s!(295, 12, "someHead(?VarA)"),
                        first: Atom::Positive(Tuple {
                            span: s!(295,12,"someHead(?VarA)"),
                            identifier: Some(Token {
                                kind: TokenKind::Ident,
                                span: s!(295, 12, "someHead"),
                            }),
                            ws1: None,
                            open_paren: Token { kind: TokenKind::OpenParen, span: s!(303,12,"(") },
                            ws2: None,
                            terms: Some(List {
                                span: s!(304, 12, "?VarA"),
                                first: Term::Variable(Token {
                                    kind: TokenKind::Variable,
                                    span: s!(304, 12, "?VarA"),
                                }),
                                rest: None,
                            }),
                            ws3: None,
                            close_paren: Token { kind: TokenKind::CloseParen, span: s!(309,12,")") },
                        }),
                        rest: None,
                    },
                    ws1: Some(Wsoc {span: s!(310, 12, " "), token: vec![Token{kind:TokenKind::Whitespace,span:s!(310,12," ")}] }),
                    arrow: Token{kind:TokenKind::Arrow, span:s!(311,12,":-")},
                    ws2: Some(Wsoc {span: s!(313, 12, " "), token: vec![Token{kind:TokenKind::Whitespace,span:s!(313,12," ")}] }),
                    body: List {
                        span: s!(314, 12, "somePredicate(?VarA, ConstB)"),
                        first: Atom::Positive(Tuple {
                            span: s!(314, 12,"somePredicate(?VarA, ConstB)"),
                            identifier: Some(Token {
                                kind: TokenKind::Ident,
                                span: s!(314, 12, "somePredicate"),
                            }),
                            ws1: None,
                            open_paren: Token { kind: TokenKind::OpenParen, span: s!(327,12,"(") },
                            ws2: None,
                            terms: Some(List {
                                span: s!(328, 12, "?Var, ConstB"),
                                first: Term::Variable(Token {
                                    kind: TokenKind::Variable,
                                    span: s!(328, 12, "?VarA"),
                                }),
                                rest: Some(vec![(
                                    None,
                                    Token {
                                        kind: TokenKind::Comma,
                                        span: s!(333, 12, ","),
                                    },
                                    Some(Wsoc {span: s!(334, 12, " "), token: vec![Token{kind:TokenKind::Whitespace,span:s!(334,12," "),}] }),
                                    Term::Primitive(Primitive::Constant(Token {
                                        kind: TokenKind::Ident,
                                        span: s!(335, 12, "ConstB"),
                                    })),
                                )]),
                            }),
                            ws3: None,
                            close_paren: Token { kind: TokenKind::CloseParen, span: s!(341, 12,")") },
                        }),
                        rest: None,
                    },
                    ws3: None,
                    dot: Token{kind:TokenKind::Dot,span:s!(342, 12,".")},
                },
                Statement::Whitespace(Token {
                    kind: TokenKind::Whitespace,
                    span: s!(343, 12, "   "),
                }),
                Statement::Comment(Token {
                    kind: TokenKind::Comment,
                    span: s!(346, 12, "% all constants that are in relation with ConstB\n"),
                }),
            ],
        };
        println!("{}", ast);
        let tokens1 = get_all_tokens(&ast);
        for token in &tokens1 {
            println!("{}", token);
        }

        assert_eq!(input, {
            let mut result = String::new();
            for token in &tokens1 {
                result.push_str(token.span().fragment());
            }
            result
        });
    }
}
