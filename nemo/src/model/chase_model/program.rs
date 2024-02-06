//! Defines a variant of [`crate::model::Program`], suitable for computing the chase.

use std::collections::{HashMap, HashSet};

use crate::{
    error::Error,
    io::formats::import_export::{ImportExportHandler, ImportExportHandlers},
    model::{Constant, ExportDirective, Identifier, ImportDirective, PrimitiveType, Program},
};

use super::{ChaseAtom, ChaseFact, ChaseRule};

/// Representation of a datalog program that is used for generating execution plans for the physical layer.
#[derive(Debug, Default, Clone)]
pub(crate) struct ChaseProgram {
    base: Option<String>,
    prefixes: HashMap<String, String>,
    import_handlers: Vec<(Identifier, Box<dyn ImportExportHandler>)>,
    export_handlers: Vec<(Identifier, Box<dyn ImportExportHandler>)>,
    rules: Vec<ChaseRule>,
    facts: Vec<ChaseFact>,
    parsed_predicate_declarations: HashMap<Identifier, Vec<PrimitiveType>>,
    output_predicates: Vec<Identifier>,
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

    /// Add an imported table.
    pub fn import(mut self, import: &ImportDirective) -> Result<Self, Error> {
        let handler = ImportExportHandlers::import_handler(import)?;
        self.program
            .import_handlers
            .push((import.predicate().clone(), handler));
        Ok(self)
    }

    /// Add imported tables.
    pub fn imports<T>(self, imports: T) -> Result<Self, Error>
    where
        T: IntoIterator<Item = ImportDirective>,
    {
        let mut cur_self: Self = self;
        for import in imports {
            cur_self = cur_self.import(&import)?;
        }
        Ok(cur_self)
    }

    /// Add an exported table.
    pub fn export(mut self, export: &ExportDirective) -> Result<Self, Error> {
        let handler = ImportExportHandlers::export_handler(export)?;
        self.program
            .export_handlers
            .push((export.predicate().clone(), handler));
        Ok(self)
    }

    /// Add exported tables.
    pub fn exports<T>(self, exports: T) -> Result<Self, Error>
    where
        T: IntoIterator<Item = ExportDirective>,
    {
        let mut cur_self: Self = self;
        for export in exports {
            cur_self = cur_self.export(&export)?;
        }
        Ok(cur_self)
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

    /// Select an IDB predicate for output.
    pub fn output_predicate(self, predicate: Identifier) -> Self {
        self.output_predicates([predicate])
    }

    /// Select IDB predicates for output.
    pub fn output_predicates<T>(mut self, predicates: T) -> Self
    where
        T: IntoIterator<Item = Identifier>,
    {
        self.program.output_predicates.extend(predicates);
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
    pub(crate) fn imports(
        &self,
    ) -> impl Iterator<Item = &(Identifier, Box<dyn ImportExportHandler>)> {
        self.import_handlers.iter()
    }

    /// Return all exports in the program.
    pub fn exports(&self) -> impl Iterator<Item = &(Identifier, Box<dyn ImportExportHandler>)> {
        self.export_handlers.iter()
    }

    /// Return an Iterator over all output predicates
    pub fn output_predicates(&self) -> impl Iterator<Item = &Identifier> {
        self.output_predicates.iter()
    }

    /// Return all prefixes in the program.
    #[must_use]
    pub fn prefixes(&self) -> &HashMap<String, String> {
        &self.prefixes
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

    /// Return the constants appearing in the rules of the program.
    pub fn all_constants(&self) -> impl Iterator<Item = &Constant> {
        self.rules.iter().flat_map(|rule| rule.all_constants())
    }
}

impl TryFrom<Program> for ChaseProgram {
    type Error = Error;

    fn try_from(program: Program) -> Result<Self, Error> {
        let mut builder = Self::builder()
            .prefixes(program.prefixes().clone())
            .imports(program.imports().cloned())?
            .exports(program.exports().cloned())?
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
            );

        if let Some(base) = program.base() {
            builder = builder.base(base);
        }

        builder = builder.output_predicates(program.output_predicates().cloned());

        Ok(builder.build())
    }
}
