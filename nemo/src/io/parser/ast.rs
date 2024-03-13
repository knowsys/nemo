use std::collections::BTreeMap;

use crate::io::lexer::Token;

struct Position {
    offset: usize,
    line: u32,
    column: u32,
}

pub(crate) type Program<'a> = Vec<Statement<'a>>;

#[derive(Debug, PartialEq)]
pub(crate) enum Statement<'a> {
    Directive(Directive<'a>),
    Fact {
        atom: Atom<'a>,
    },
    Rule {
        head: Vec<Atom<'a>>,
        body: Vec<Literal<'a>>,
    },
}

#[derive(Debug, PartialEq)]
pub(crate) enum Directive<'a> {
    Base {
        kw: Token<'a>,
        base_iri: Token<'a>,
    },
    Prefix {
        kw: Token<'a>,
        prefix: Token<'a>,
        prefix_iri: Token<'a>,
    },
    Import {
        kw: Token<'a>,
        predicate: Token<'a>,
        map: Map<'a>,
    },
    Export {
        kw: Token<'a>,
        predicate: Token<'a>,
        map: Map<'a>,
    },
    Output {
        kw: Token<'a>,
        predicates: Vec<Token<'a>>,
    },
}

#[derive(Debug, PartialEq)]
pub(crate) enum Atom<'a> {
    Atom {
        predicate: Token<'a>,
        terms: Vec<Term<'a>>,
    },
    InfixAtom {
        operation: Token<'a>,
        lhs: Term<'a>,
        rhs: Term<'a>,
    },
    Map(Map<'a>),
}

#[derive(Debug, PartialEq)]
pub(crate) enum Literal<'a> {
    Positive(Atom<'a>),
    Negative(Atom<'a>),
}

#[derive(Debug, PartialEq)]
pub(crate) enum Term<'a> {
    Primitive(Token<'a>),
    Binary {
        operation: Token<'a>,
        lhs: Box<Term<'a>>,
        rhs: Box<Term<'a>>,
    },
    Unary {
        operation: Token<'a>,
        term: Box<Term<'a>>,
    },
    Aggregation {
        operation: Token<'a>,
        terms: Vec<Term<'a>>,
    },
    Function {
        identifier: Token<'a>,
        terms: Vec<Term<'a>>,
    },
    Map(Map<'a>),
}

#[derive(Debug, PartialEq)]
struct Map<'a> {
    identifier: Option<Token<'a>>,
    pairs: BTreeMap<Term<'a>, Term<'a>>,
}

#[derive(Debug, PartialEq)]
pub(crate) enum Node<'a> {
    Statement(&'a Statement<'a>),
    Directive(&'a Directive<'a>),
    RuleHead(&'a Vec<Atom<'a>>),
    RuleBody(&'a Vec<Literal<'a>>),
    Atom(&'a Atom<'a>),
    Term(&'a Term<'a>),
    Terms(&'a Vec<Term<'a>>),
    Map(&'a Map<'a>),
    KeyWord(&'a Token<'a>),
    BaseIri(&'a Token<'a>),
    Prefix(&'a Token<'a>),
    PrefixIri(&'a Token<'a>),
    Predicate(&'a Token<'a>),
    Predicates(&'a Vec<Token<'a>>),
    Operation(&'a Token<'a>),
    Lhs(&'a Term<'a>),
    Rhs(&'a Term<'a>),
    Identifier(&'a Token<'a>),
    Pairs(&'a BTreeMap<Term<'a>, Term<'a>>),
    MapIdentifier(&'a Option<Token<'a>>),
    Primitive(&'a Token<'a>),
}

trait AstNode {
    fn children(&self) -> Vec<Node>;
    // fn position(&self) -> Position;
}

impl<'a> AstNode for Program<'a> {
    fn children(&self) -> Vec<Node> {
        let mut vec = Vec::new();
        for statement in self {
            vec.push(Node::Statement(statement))
        }
        vec
    }

    // fn position(&self) -> Position {
    //     let first = self.get(0);
    //     match first {
    //         Some(elem) => {
    //             let span;
    //             match elem {
    //                 Statement::Directive(directive) => match directive {
    //                     Directive::Base { kw, base_iri } => span = kw.span,
    //                     Directive::Prefix {
    //                         kw,
    //                         prefix,
    //                         prefix_iri,
    //                     } => span = kw.span,
    //                     Directive::Import { kw, predicate, map } => span = kw.span,
    //                     Directive::Export { kw, predicate, map } => span = kw.span,
    //                     Directive::Output { kw, predicates } => span = kw.span,
    //                 },
    //                 Statement::Fact { atom } => match atom {
    //                     Atom::Atom { predicate, terms } => todo!(),
    //                     Atom::InfixAtom { operation, lhs, rhs } => todo!(),
    //                     Atom::Map(_) => todo!(),
    //                 },
    //                 Statement::Rule { head, body } => todo!(),
    //             };
    //         }
    //         None => Position {
    //             offset: 0,
    //             line: 1,
    //             column: 0,
    //         },
    //     }
    // }
}

impl<'a> AstNode for Statement<'a> {
    fn children(&self) -> Vec<Node> {
        match self {
            Statement::Directive(directive) => directive.children(),
            Statement::Fact { atom } => vec![Node::Atom(atom)],
            Statement::Rule { head, body } => {
                vec![Node::RuleHead(head), Node::RuleBody(body)]
            }
        }
    }

    // fn position(&self) -> Position {
    //     todo!()
    // }
}

impl<'a> AstNode for Directive<'a> {
    fn children(&self) -> Vec<Node> {
        match self {
            Directive::Base { kw, base_iri } => {
                vec![Node::KeyWord(kw), Node::BaseIri(base_iri)]
            }
            Directive::Prefix {
                kw,
                prefix,
                prefix_iri,
            } => vec![
                Node::KeyWord(kw),
                Node::Prefix(prefix),
                Node::PrefixIri(prefix_iri),
            ],
            Directive::Import { kw, predicate, map } => vec![
                Node::KeyWord(kw),
                Node::Predicate(predicate),
                Node::Map(map),
            ],
            Directive::Export { kw, predicate, map } => vec![
                Node::KeyWord(kw),
                Node::Predicate(predicate),
                Node::Map(map),
            ],
            Directive::Output { kw, predicates } => {
                vec![Node::KeyWord(kw), Node::Predicates(predicates)]
            }
        }
    }

    // fn position(&self) -> Position {
    //     todo!()
    // }
}

impl<'a> AstNode for Atom<'a> {
    fn children(&self) -> Vec<Node> {
        match self {
            Atom::Atom { predicate, terms } => {
                vec![Node::KeyWord(predicate), Node::Terms(terms)]
            }
            Atom::InfixAtom {
                operation,
                lhs,
                rhs,
            } => vec![Node::Operation(operation), Node::Lhs(lhs), Node::Rhs(rhs)],
            Atom::Map(map) => map.children(),
        }
    }

    // fn position(&self) -> Position {
    //     todo!()
    // }
}

impl<'a> AstNode for Term<'a> {
    fn children(&self) -> Vec<Node> {
        match self {
            Term::Primitive(prim) => vec![Node::Primitive(prim)],
            Term::Binary {
                operation,
                lhs,
                rhs,
            } => vec![Node::Operation(operation), Node::Lhs(lhs), Node::Rhs(rhs)],
            Term::Unary { operation, term } => vec![Node::Operation(operation), Node::Term(term)],
            Term::Aggregation { operation, terms } => {
                vec![Node::Operation(operation), Node::Terms(terms)]
            }
            Term::Function { identifier, terms } => {
                vec![Node::Identifier(identifier), Node::Terms(terms)]
            }
            Term::Map(map) => map.children(),
        }
    }

    // fn position(&self) -> Position {
    //     todo!()
    // }
}

impl<'a> AstNode for Map<'a> {
    fn children(&self) -> Vec<Node> {
        vec![
            Node::MapIdentifier(&self.identifier),
            Node::Pairs(&self.pairs),
        ]
    }

    // fn position(&self) -> Position {
    //     todo!()
    // }
}
