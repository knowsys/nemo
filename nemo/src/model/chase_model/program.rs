//! Defines a variant of [`crate::model::Program`], suitable for computing the chase.

use std::collections::{HashMap, HashSet};

use crate::{
    error::Error,
    io::formats::types::{ExportSpec, ImportSpec},
    model::{
        DataSourceDeclaration, Identifier, OutputPredicateSelection, PrimitiveType, Program,
        QualifiedPredicateName,
    },
};

use super::{ChaseAtom, ChaseFact, ChaseRule};

/// Representation of a datalog program that is used for generating execution plans for the physical layer.
#[derive(Debug, Default, Clone)]
pub struct ChaseProgram {
    base: Option<String>,
    prefixes: HashMap<String, String>,
    sources: Vec<DataSourceDeclaration>,
    imports: Vec<ImportSpec>,
    exports: Vec<ExportSpec>,
    rules: Vec<ChaseRule>,
    facts: Vec<ChaseFact>,
    parsed_predicate_declarations: HashMap<Identifier, Vec<PrimitiveType>>,
    output_predicates: OutputPredicateSelection,
}

/// A Builder for a [ChaseProgram].
#[derive(Debug, Default)]
pub(crate) struct ChaseProgramBuilder {
    program: ChaseProgram,
}

#[allow(dead_code)]
impl ChaseProgramBuilder {
    /// Construct a new builder.
    pub fn new() -> Self {
        Default::default()
    }

    /// Construct a [Program] from this builder.
    pub fn build(self) -> ChaseProgram {
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

    /// Add a data source.
    pub fn source(mut self, source: DataSourceDeclaration) -> Self {
        self.program.sources.push(source);
        self
    }

    /// Add data sources.
    pub fn sources<T>(mut self, sources: T) -> Self
    where
        T: IntoIterator<Item = DataSourceDeclaration>,
    {
        self.program.sources.extend(sources);
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
    pub fn rule(mut self, rule: ChaseRule) -> Self {
        self.program.rules.push(rule);
        self
    }

    /// Add rules.
    pub fn rules<T>(mut self, rules: T) -> Self
    where
        T: IntoIterator<Item = ChaseRule>,
    {
        self.program.rules.extend(rules);
        self
    }

    /// Add a fact.
    pub fn fact(mut self, fact: ChaseFact) -> Self {
        self.program.facts.push(fact);
        self
    }

    /// Add facts.
    pub fn facts<T>(mut self, facts: T) -> Self
    where
        T: IntoIterator<Item = ChaseFact>,
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

impl ChaseProgram {
    /// Return a [builder][ChaseProgramBuilder] for a [ChaseProgram].
    pub(crate) fn builder() -> ChaseProgramBuilder {
        Default::default()
    }

    /// Get the base IRI, if set.
    #[must_use]
    pub fn base(&self) -> Option<String> {
        self.base.clone()
    }

    /// Return all rules in the program - immutable.
    #[must_use]
    pub fn rules(&self) -> &Vec<ChaseRule> {
        &self.rules
    }

    /// Return all rules in the program - mutable.
    #[must_use]
    pub fn rules_mut(&mut self) -> &mut Vec<ChaseRule> {
        &mut self.rules
    }

    /// Return all facts in the program.
    #[must_use]
    pub fn facts(&self) -> &Vec<ChaseFact> {
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
                    .chain(rule.all_body().map(|atom| atom.predicate()))
            })
            .chain(self.facts().iter().map(|atom| atom.predicate()))
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

    /// Return all imports in the program.
    pub fn imports(&self) -> impl Iterator<Item = &ImportSpec> {
        self.imports.iter()
    }

    /// Return all exports in the program.
    pub fn exports(&self) -> impl Iterator<Item = &ExportSpec> {
        self.exports.iter()
    }

    /// Return an Iterator over all output predicates
    pub fn output_predicates(&self) -> impl Iterator<Item = Identifier> {
        let result: Vec<_> = match &self.output_predicates {
            OutputPredicateSelection::AllIDBPredicates => {
                log::debug!("outputting all IDB predicates");
                self.idb_predicates().iter().cloned().collect()
            }
            OutputPredicateSelection::SelectedPredicates(predicates) => {
                log::debug!("outputting predicates {predicates:?}");
                predicates
                    .iter()
                    .map(|QualifiedPredicateName { identifier, .. }| identifier)
                    .cloned()
                    .collect()
            }
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
    pub fn parsed_predicate_declarations(&self) -> &HashMap<Identifier, Vec<PrimitiveType>> {
        &self.parsed_predicate_declarations
    }

    /// Force the given selection of output predicates.
    pub fn force_output_predicate_selection(
        &mut self,
        output_predicates: OutputPredicateSelection,
    ) {
        self.output_predicates = output_predicates;
    }
}

impl TryFrom<Program> for ChaseProgram {
    type Error = Error;

    fn try_from(program: Program) -> Result<Self, Error> {
        let mut builder = Self::builder()
            .prefixes(program.prefixes().clone())
            .sources(program.sources().cloned())
            .imports(program.imports().cloned())
            .exports(program.exports().cloned())
            .rules(
                program
                    .rules()
                    .iter()
                    .cloned()
                    .map(ChaseRule::try_from)
                    .collect::<Result<Vec<_>, Error>>()?,
            )
            .facts(
                program
                    .facts()
                    .iter()
                    .map(|fact| ChaseFact::from_flat_atom(&fact.0)),
            )
            .predicate_declarations(program.parsed_predicate_declarations());

        if let Some(base) = program.base() {
            builder = builder.base(base);
        }

        if let OutputPredicateSelection::SelectedPredicates(predicates) =
            program.output_predicate_selection()
        {
            log::debug!("setting output predicates: {predicates:?}");
            builder = builder.output_predicates(predicates.clone());
        }

        Ok(builder.build())
    }
}
