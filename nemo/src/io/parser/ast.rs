use tower_lsp::lsp_types::SymbolKind;

use crate::io::lexer::Span;
use ascii_tree::{write_tree, Tree};
use std::fmt::Display;

pub(crate) mod atom;
pub(crate) mod directive;
pub(crate) mod map;
pub mod program;
pub(crate) mod statement;
pub(crate) mod term;
pub(crate) mod tuple;
pub(crate) mod named_tuple;

pub trait AstNode: std::fmt::Debug + Display + Sync {
    fn children(&self) -> Option<Vec<&dyn AstNode>>;
    fn span(&self) -> Span;

    fn range(&self) -> Range {
        let span = self.span();

        let start_position = Position {
            offset: self.span().location_offset(),
            line: self.span().location_line(),
            column: self.span().get_utf8_column() as u32,
        };

        let end_position = Position {
            offset: start_position.offset + span.len(),
            line: start_position.line + span.fragment().lines().count() as u32 - 1,
            column: if span.fragment().lines().count() > 1 {
                1 + span.fragment().lines().last().unwrap().len() as u32 // Column is on new line
            } else {
                start_position.column + span.fragment().len() as u32 // Column is on same line
            },
        };

        Range {
            start: start_position,
            end: end_position,
        }
    }

    // FIXME: With the removal of tokens is this method still usefull and/or should be renamed?
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
    /// Range of the part of the node that should be renamed or [`None`] if the node can not be renamed
    fn lsp_range_to_rename(&self) -> Option<Range>;
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

#[derive(Debug, Clone, Copy, Hash)]
pub struct Range {
    pub start: Position,
    pub end: Position,
}

/// Whitespace or Comment token
#[derive(Debug, Clone, PartialEq)]
pub struct Wsoc<'a> {
    pub span: Span<'a>,
    pub token: Vec<Span<'a>>,
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

    fn lsp_range_to_rename(&self) -> Option<Range> {
        None
    }

    fn lsp_symbol_info(&self) -> Option<(String, SymbolKind)> {
        None
    }
}

impl Display for Wsoc<'_> {
    fn fmt(&self, _f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct List<'a, T> {
    pub span: Span<'a>,
    pub first: T,
    // (,T)*
    pub rest: Option<Vec<(Span<'a>, T)>>,
}
impl<'a, T> List<'a, T> {
    pub fn to_item_vec(&'a self) -> Vec<&'a T> {
        let mut vec = Vec::new();
        vec.push(&self.first);
        if let Some(rest) = &self.rest {
            for (_, item) in rest {
                vec.push(&item);
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
            for (_, item) in rest {
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
            for (delim, item) in rest {
                vec.push(delim);
                vec.push(item);
            }
        };
        Some(vec)
    }

    fn span(&self) -> Span {
        self.span
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

    fn lsp_range_to_rename(&self) -> Option<Range> {
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
                vec.push(Tree::Leaf(vec![format!("\x1b[93m{:?}\x1b[0m", child.name())]));
            } else {
                vec.push(ast_to_ascii_tree(child));
            }
        }
    }
    Tree::Node(node.name(), vec)
}

mod test {
    use named_tuple::NamedTuple;

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
            tl_doc_comment: Some(
                s!(0, 1, "%! This is just a test file.\n%! So the documentation of the rules is not important.\n")
            ),
            statements: vec![
                Statement::Directive(Directive::Prefix {
                    span:s!(125,4,"@prefix xsd: <http://www.w3.org/2001/XMLSchema#>."),
                    doc_comment:Some(
                        s!(84,3,"%% This is the prefix used for datatypes\n")
                    ),
                    prefix: 
                        s!(133, 4, "xsd:"),
                    prefix_iri: 
                        s!(138, 4, "<http://www.w3.org/2001/XMLSchema#>"),
                    dot: 
                        s!(173,4,".")
                }),
                Statement::Comment(
                    s!(176, 6, "% Facts\n"),
                ),
                Statement::Fact {
                    span:s!(222,8,"somePredicate(ConstA, ConstB)."),
                    doc_comment: Some(
                        s!(184,7,"%% This is just an example predicate.\n")
                    ),
                    atom: Atom::Positive(NamedTuple {
                        span: s!(222,8,"somePredicate(ConstA, ConstB)"),
                        identifier: s!(222, 8, "somePredicate"),
                        tuple: Tuple {
                            span: s!(235,8,"(ConstA, ConstB)"),
                            open_paren:
                                s!(235,8,"(")
                            ,
                            terms: Some(List {
                                span: s!(236, 8, "ConstA, ConstB"),
                                first: Term::Primitive(Primitive::Constant(                                s!(236, 8, "ConstA"),
                                )),
                                rest: Some(vec![(
                                        s!(242, 8, ","),
                                    Term::Primitive(Primitive::Constant(                                    s!(244, 8, "ConstB"),
                                    )),
                                )]),
                            }),
                            close_paren: s!(250,8,")")
                        }
                    }),
                    dot: 
                        s!(251,8,".")
                    
                },
                Statement::Comment(
                    s!(254, 10, "% Rules\n"),
                ),
                Statement::Rule {
                    span: s!(295,12,"someHead(?VarA) :- somePredicate(?VarA, ConstB)."),
                    doc_comment: Some(s!(262,11,"%% This is just an example rule.\n")),
                    head: List {
                        span: s!(295, 12, "someHead(?VarA)"),
                        first: Atom::Positive(NamedTuple {
                            span: s!(295,12,"someHead(?VarA)"),
                            identifier: s!(295, 12, "someHead"),
                            tuple: Tuple {
                                span: s!(303,12,"(?VarA)"),
                                open_paren: s!(303,12,"(") ,
                                terms: Some(List {
                                    span: s!(304, 12, "?VarA"),
                                    first: Term::UniversalVariable(                                    s!(304, 12, "?VarA"),
                                    ),
                                    rest: None,
                                }),
                                close_paren: s!(309,12,")") ,
                            }
                        }),
                        rest: None,
                    },
                    arrow: s!(311,12,":-"),
                    body: List {
                        span: s!(314, 12, "somePredicate(?VarA, ConstB)"),
                        first: Atom::Positive(NamedTuple {
                            span: s!(314, 12,"somePredicate(?VarA, ConstB)"),
                            identifier: s!(314, 12, "somePredicate"),
                            tuple: Tuple {
                                span: s!(327,12,"(?VarA, ConstB)"),
                                open_paren: s!(327,12,"("),
                                terms: Some(List {
                                    span: s!(328, 12, "?Var, ConstB"),
                                    first: Term::UniversalVariable(                                    s!(328, 12, "?VarA"),
                                    ),
                                    rest: Some(vec![(
                                            s!(333, 12, ","),
                                    
                                        Term::Primitive(Primitive::Constant(s!(335, 12, "ConstB"),
                                        )),
                                    )]),
                                }),
                                close_paren: s!(341, 12,")") ,
                            }
                        }),
                        rest: None,
                    },
                    dot: s!(342, 12,"."),
                },
                Statement::Comment(
                    s!(346, 12, "% all constants that are in relation with ConstB\n"),
                ),
            ],
        };
        println!("{}", ast);
        let tokens1 = get_all_tokens(&ast);
        for token in &tokens1 {
            println!("{}", token);
        }

        // This doesn't work anymore, because the whitespace and keywords got removed from
        // from the AST, so you can't directly recreate the input exactly.
        // assert_eq!(input, {
        //     let mut result = String::new();
        //     for token in &tokens1 {
        //         result.push_str(token.span().fragment());
        //     }
        //     result
        // });
    }
}
