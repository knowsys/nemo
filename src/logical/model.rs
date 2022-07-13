//! The data model.

use std::{collections::HashMap, fmt::Display, path::Path};

use crate::physical::dictionary::{Dictionary, PrefixedStringDictionary};

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

#[derive(Debug, Eq, PartialEq, Hash, Copy, Clone, PartialOrd, Ord)]
pub enum Term {
    Constant(Identifier),
    Variable(Identifier),
    ExistentialVariable(Identifier),
}

#[derive(Debug, Eq, PartialEq, Hash, Clone, PartialOrd, Ord)]
pub struct Atom {
    predicate: Identifier,
    terms: Vec<Term>,
}

#[derive(Debug, Eq, PartialEq, Hash, Clone, PartialOrd, Ord)]
pub enum Literal {
    Positive(Atom),
    Negative(Atom),
}

#[derive(Debug, Eq, PartialEq, Hash, Clone, PartialOrd, Ord)]
pub struct Rule {
    head: Vec<Atom>,
    body: Vec<Literal>,
}

#[derive(Debug)]
pub struct Program {
    base: Option<usize>,
    prefixes: HashMap<String, usize>,
    rules: Vec<Rule>,
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
