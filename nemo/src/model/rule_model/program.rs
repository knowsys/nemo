use std::{
    collections::{HashMap, HashSet},
    path::Path,
};

use crate::model::PrimitiveType;

use super::{Atom, DataSourceDeclaration, Identifier, QualifiedPredicateName, Rule};

/// A (ground) fact.
#[derive(Debug, Eq, PartialEq, Clone)]
pub struct Fact(pub Atom);

/// A statement that can occur in the program.
#[derive(Debug, Eq, PartialEq, Clone)]
pub enum Statement {
    /// A fact.
    Fact(Fact),
    /// A rule.
    Rule(Rule),
}

/// Which predicates to output
#[derive(Debug, Eq, PartialEq, Clone, Default)]
pub enum OutputPredicateSelection {
    /// Output every IDB predicate
    #[default]
    AllIDBPredicates,
    /// Output only selected predicates
    SelectedPredicates(Vec<QualifiedPredicateName>),
}

impl From<Vec<QualifiedPredicateName>> for OutputPredicateSelection {
    fn from(predicates: Vec<QualifiedPredicateName>) -> Self {
        if predicates.is_empty() {
            Self::AllIDBPredicates
        } else {
            Self::SelectedPredicates(predicates)
        }
    }
}

/// A directive that can occur in the program.
#[derive(Debug)]
pub enum Directive {
    /// Import another source file.
    Import(Box<Path>),
    /// Import another source file with a relative path.
    ImportRelative(Box<Path>),
}

/// A full program.
#[derive(Debug, Default, Clone)]
pub struct Program {
    base: Option<String>,
    prefixes: HashMap<String, String>,
    sources: Vec<DataSourceDeclaration>,
    rules: Vec<Rule>,
    facts: Vec<Fact>,
    parsed_predicate_declarations: HashMap<Identifier, Vec<PrimitiveType>>,
    output_predicates: OutputPredicateSelection,
}

impl From<Vec<Rule>> for Program {
    fn from(rules: Vec<Rule>) -> Self {
        Self {
            rules,
            ..Default::default()
        }
    }
}

impl From<(Vec<DataSourceDeclaration>, Vec<Rule>)> for Program {
    fn from((sources, rules): (Vec<DataSourceDeclaration>, Vec<Rule>)) -> Self {
        Self {
            sources,
            rules,
            ..Default::default()
        }
    }
}

impl Program {
    /// Construct a new program.
    pub fn new(
        base: Option<String>,
        prefixes: HashMap<String, String>,
        sources: Vec<DataSourceDeclaration>,
        rules: Vec<Rule>,
        facts: Vec<Fact>,
        parsed_predicate_declarations: HashMap<Identifier, Vec<PrimitiveType>>,
        output_predicates: OutputPredicateSelection,
    ) -> Self {
        Self {
            base,
            prefixes,
            sources,
            rules,
            facts,
            parsed_predicate_declarations,
            output_predicates,
        }
    }

    /// Get the base IRI, if set.
    #[must_use]
    pub fn base(&self) -> Option<String> {
        self.base.clone()
    }

    /// Return all rules in the program - immutable.
    #[must_use]
    pub fn rules(&self) -> &Vec<Rule> {
        &self.rules
    }

    /// Return all rules in the program - mutable.
    #[must_use]
    pub fn rules_mut(&mut self) -> &mut Vec<Rule> {
        &mut self.rules
    }

    /// Return all facts in the program.
    #[must_use]
    pub fn facts(&self) -> &Vec<Fact> {
        &self.facts
    }

    /// Return a HashSet of all predicates in the program (in rules and facts).
    #[must_use]
    pub fn predicates(&self) -> HashSet<Identifier> {
        self.rules()
            .iter()
            .flat_map(|rule| {
                rule.head()
                    .iter()
                    .map(|atom| atom.predicate())
                    .chain(rule.body().iter().map(|literal| literal.predicate()))
            })
            .chain(self.facts().iter().map(|atom| atom.0.predicate()))
            .collect()
    }

    /// Return a HashSet of all idb predicates (predicates occuring rule heads) in the program.
    #[must_use]
    pub fn idb_predicates(&self) -> HashSet<Identifier> {
        self.rules()
            .iter()
            .flat_map(|rule| rule.head())
            .map(|atom| atom.predicate())
            .collect()
    }

    /// Return a HashSet of all edb predicates (all predicates minus idb predicates) in the program.
    #[must_use]
    pub fn edb_predicates(&self) -> HashSet<Identifier> {
        self.predicates()
            .difference(&self.idb_predicates())
            .cloned()
            .collect()
    }

    /// Return an Iterator over all output predicates
    pub fn output_predicates(&self) -> impl Iterator<Item = Identifier> {
        let result: Vec<_> = match &self.output_predicates {
            OutputPredicateSelection::AllIDBPredicates => {
                self.idb_predicates().iter().cloned().collect()
            }
            OutputPredicateSelection::SelectedPredicates(predicates) => predicates
                .iter()
                .map(|QualifiedPredicateName { identifier, .. }| identifier)
                .cloned()
                .collect(),
        };

        result.into_iter()
    }

    /// Return all prefixes in the program.
    #[must_use]
    pub fn prefixes(&self) -> &HashMap<String, String> {
        &self.prefixes
    }

    /// Return all data sources in the program.
    pub fn sources(&self) -> impl Iterator<Item = &DataSourceDeclaration> {
        self.sources.iter()
    }

    /// Look up a given prefix.
    #[must_use]
    pub fn resolve_prefix(&self, tag: &str) -> Option<String> {
        self.prefixes.get(tag).cloned()
    }

    /// Return parsed predicate declarations
    #[must_use]
    pub fn parsed_predicate_declarations(&self) -> HashMap<Identifier, Vec<PrimitiveType>> {
        self.parsed_predicate_declarations.clone()
    }

    /// Force the given selection of output predicates.
    pub fn force_output_predicate_selection(
        &mut self,
        output_predicates: OutputPredicateSelection,
    ) {
        self.output_predicates = output_predicates;
    }
}
