//! The data model.

use std::{collections::HashMap, fmt::Display, path::Path};

use crate::physical::{
    datatypes::Double,
    dictionary::{Dictionary, PrefixedStringDictionary},
};

/// An identifier for, e.g., a Term or a Predicate.
#[derive(Debug, Eq, PartialEq, Hash, Copy, Clone, PartialOrd, Ord)]
pub struct Identifier(pub(crate) usize);

impl Identifier {
    pub fn format<'a, 'b>(
        &'a self,
        dictionary: &'b PrefixedStringDictionary,
    ) -> PrintableIdentifier<'b>
    where
        'a: 'b,
    {
        PrintableIdentifier {
            identifier: self,
            dictionary,
        }
    }
}

/// A pretty-printable identifier that can be resolved using the dictionary.
#[derive(Debug)]
pub struct PrintableIdentifier<'a> {
    identifier: &'a Identifier,
    dictionary: &'a PrefixedStringDictionary,
}

impl Display for PrintableIdentifier<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let ident = self.identifier.0;
        write!(
            f,
            "{}",
            self.dictionary
                .entry(ident)
                .unwrap_or_else(|| format!("<unresolved identifier {ident}>"))
        )
    }
}

#[derive(Debug, Eq, PartialEq, Copy, Clone, PartialOrd, Ord)]
pub enum Term {
    Constant(Identifier),
    Variable(Identifier),
    ExistentialVariable(Identifier),
    NumericLiteral(NumericLiteral),
    RdfLiteral(RdfLiteral),
}

impl Term {
    pub fn is_ground(&self) -> bool {
        matches!(
            self,
            Self::Constant(_) | Self::NumericLiteral(_) | Self::RdfLiteral(_)
        )
    }
}

#[derive(Debug, Eq, PartialEq, Copy, Clone, PartialOrd, Ord)]
pub enum NumericLiteral {
    Integer(i64),
    Decimal(i64, u64),
    Double(Double),
}

#[derive(Debug, Eq, PartialEq, Hash, Copy, Clone, PartialOrd, Ord)]
pub enum RdfLiteral {
    LanguageString { value: usize, tag: usize },
    DatatypeValue { value: usize, datatype: usize },
}

#[derive(Debug, Eq, PartialEq, Clone, PartialOrd, Ord)]
pub struct Atom {
    pub(crate) predicate: Identifier,
    pub(crate) terms: Vec<Term>,
}

#[derive(Debug, Eq, PartialEq, Clone, PartialOrd, Ord)]
pub enum Literal {
    Positive(Atom),
    Negative(Atom),
}

#[derive(Debug, Eq, PartialEq, Clone, PartialOrd, Ord)]
pub struct Rule {
    pub(crate) head: Vec<Atom>,
    pub(crate) body: Vec<Literal>,
}

#[derive(Debug, Eq, PartialEq, Clone, PartialOrd, Ord)]
pub struct Fact(pub(crate) Atom);

#[derive(Debug, Eq, PartialEq, Clone, PartialOrd, Ord)]
pub enum Statement {
    Fact(Fact),
    Rule(Rule),
}

#[derive(Debug)]
pub struct Program {
    pub(crate) base: Option<usize>,
    pub(crate) prefixes: HashMap<String, usize>,
    pub(crate) rules: Vec<Rule>,
    pub(crate) facts: Vec<Fact>,
}

#[derive(Debug)]
pub enum Directive {
    Import(Box<Path>),
    ImportRelative(Box<Path>),
}

#[derive(Debug)]
pub struct SparqlQuery {
    endpoint: String,
    projection: String,
    query: String,
}

#[derive(Debug)]
pub enum DataSource {
    CsvFile(Box<Path>),
    RdfFile(Box<Path>),
    SparqlQuery(Box<SparqlQuery>),
}
