use std::collections::{HashMap, HashSet};

use crate::{
    io::formats::types::{ExportSpec, ImportSpec},
    model::PrimitiveType,
};

use super::{Atom, Identifier, QualifiedPredicateName, Rule};

/// A (ground) fact.
#[derive(Debug, Eq, PartialEq, Clone)]
pub struct Fact(pub Atom);

impl std::fmt::Display for Fact {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

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

/// A full program.
#[derive(Debug, Default, Clone)]
pub struct Program {
    base: Option<String>,
    prefixes: HashMap<String, String>,
    imports: Vec<ImportSpec>,
    exports: Vec<ExportSpec>,
    rules: Vec<Rule>,
    facts: Vec<Fact>,
    parsed_predicate_declarations: HashMap<Identifier, Vec<PrimitiveType>>,
    output_predicates: OutputPredicateSelection,
}

/// A Builder for a program.
#[derive(Debug, Default)]
pub struct ProgramBuilder {
    program: Program,
}

impl ProgramBuilder {
    /// Construct a new builder.
    pub fn new() -> Self {
        Default::default()
    }

    /// Construct a [Program] from this builder.
    pub fn build(self) -> Program {
        self.program
    }

    /// Set the base IRI.
    pub fn base(mut self, base: String) -> Self {
        self.program.base = Some(base);
        self
    }

    /// Add a prefix.
    pub fn prefix(mut self, prefix: String, iri: String) -> Self {
        self.program.prefixes.insert(prefix, iri);
        self
    }

    /// Add prefixes.
    pub fn prefixes<T>(mut self, prefixes: T) -> Self
    where
        T: IntoIterator<Item = (String, String)>,
    {
        self.program.prefixes.extend(prefixes);
        self
    }

    /// Add an imported table.
    pub fn import(mut self, import: ImportSpec) -> Self {
        self.program.imports.push(import);
        self
    }

    /// Add imported tables.
    pub fn imports<T>(mut self, imports: T) -> Self
    where
        T: IntoIterator<Item = ImportSpec>,
    {
        self.program.imports.extend(imports);
        self
    }

    /// Add an exported table.
    pub fn export(mut self, export: ExportSpec) -> Self {
        self.program.exports.push(export);
        self
    }

    /// Add exported tables.
    pub fn exports<T>(mut self, exports: T) -> Self
    where
        T: IntoIterator<Item = ExportSpec>,
    {
        self.program.exports.extend(exports);
        self
    }

    /// Add a rule.
    pub fn rule(mut self, rule: Rule) -> Self {
        self.program.rules.push(rule);
        self
    }

    /// Add rules.
    pub fn rules<T>(mut self, rules: T) -> Self
    where
        T: IntoIterator<Item = Rule>,
    {
        self.program.rules.extend(rules);
        self
    }

    /// Add a fact.
    pub fn fact(mut self, fact: Fact) -> Self {
        self.program.facts.push(fact);
        self
    }

    /// Add facts.
    pub fn facts<T>(mut self, facts: T) -> Self
    where
        T: IntoIterator<Item = Fact>,
    {
        self.program.facts.extend(facts);
        self
    }

    /// Add a predicate declaration.
    pub fn predicate_declaration(
        mut self,
        predicate: Identifier,
        declared_type: Vec<PrimitiveType>,
    ) -> Self {
        self.program
            .parsed_predicate_declarations
            .insert(predicate, declared_type);
        self
    }

    /// Add predicate declarations.
    pub fn predicate_declarations<T>(mut self, declarations: T) -> Self
    where
        T: IntoIterator<Item = (Identifier, Vec<PrimitiveType>)>,
    {
        self.program
            .parsed_predicate_declarations
            .extend(declarations);
        self
    }

    /// Select all IDB predicates for output.
    pub fn output_all_idb_predicates(mut self) -> Self {
        self.program.output_predicates = OutputPredicateSelection::AllIDBPredicates;
        self
    }

    /// Select an IDB predicate for output.
    pub fn output_predicate(self, predicate: QualifiedPredicateName) -> Self {
        self.output_predicates([predicate])
    }

    /// Select IDB predicates for output.
    pub fn output_predicates<T>(mut self, predicates: T) -> Self
    where
        T: IntoIterator<Item = QualifiedPredicateName>,
    {
        match self.program.output_predicates {
            OutputPredicateSelection::SelectedPredicates(ref mut selected) => {
                selected.extend(predicates)
            }
            OutputPredicateSelection::AllIDBPredicates => {
                self.program.output_predicates =
                    OutputPredicateSelection::SelectedPredicates(Vec::from_iter(predicates))
            }
        }
        self
    }
}

impl Program {
    /// Return a [builder][ProgramBuilder] for the [Program].
    pub fn builder() -> ProgramBuilder {
        Default::default()
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

    pub(crate) fn output_predicate_selection(&self) -> &OutputPredicateSelection {
        &self.output_predicates
    }

    /// Return all prefixes in the program.
    #[must_use]
    pub fn prefixes(&self) -> &HashMap<String, String> {
        &self.prefixes
    }

    /// Return all imports in the program.
    pub fn imports(&self) -> impl Iterator<Item = &ImportSpec> {
        self.imports.iter()
    }

    /// Return all exports in the program.
    pub fn exports(&self) -> impl Iterator<Item = &ExportSpec> {
        self.exports.iter()
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
