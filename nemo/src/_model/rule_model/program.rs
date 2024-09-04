// use std::collections::{HashMap, HashSet};

// use crate::model::{ExportDirective, ImportDirective};

// use super::{Atom, Identifier, Rule};

// /// A (ground) fact.
// #[derive(Debug, Eq, PartialEq, Clone)]
// pub struct Fact(pub Atom);

// impl std::fmt::Display for Fact {
//     fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
//         self.0.fmt(f)
//     }
// }

// /// A statement that can occur in the program.
// #[derive(Debug, Eq, PartialEq, Clone)]
// pub enum Statement {
//     /// A fact.
//     Fact(Fact),
//     /// A rule.
//     Rule(Rule),
// }

// /// A complete program.
// #[derive(Debug, Default, Clone)]
// pub struct Program {
//     base: Option<String>,
//     prefixes: HashMap<String, String>,
//     rules: Vec<Rule>,
//     facts: Vec<Fact>,
//     imports: Vec<ImportDirective>,
//     exports: Vec<ExportDirective>,
//     output_predicates: Vec<Identifier>,
// }

// /// A Builder for a program.
// #[derive(Debug, Default)]
// pub struct ProgramBuilder {
//     program: Program,
// }

// impl ProgramBuilder {
//     /// Construct a new builder.
//     pub fn new() -> Self {
//         Default::default()
//     }

//     /// Construct a [Program] from this builder.
//     pub fn build(self) -> Program {
//         self.program
//     }

//     /// Set the base IRI.
//     pub fn base(mut self, base: String) -> Self {
//         self.program.base = Some(base);
//         self
//     }

//     /// Add a prefix.
//     pub fn prefix(mut self, prefix: String, iri: String) -> Self {
//         self.program.prefixes.insert(prefix, iri);
//         self
//     }

//     /// Add prefixes.
//     pub fn prefixes<T>(mut self, prefixes: T) -> Self
//     where
//         T: IntoIterator<Item = (String, String)>,
//     {
//         self.program.prefixes.extend(prefixes);
//         self
//     }

//     /// Add an imported table.
//     pub fn import(mut self, import: ImportDirective) -> Self {
//         self.program.imports.push(import);
//         self
//     }

//     /// Add imported tables.
//     pub fn imports<T>(mut self, imports: T) -> Self
//     where
//         T: IntoIterator<Item = ImportDirective>,
//     {
//         self.program.imports.extend(imports);
//         self
//     }

//     /// Add an exported table.
//     pub fn export(mut self, export: ExportDirective) -> Self {
//         self.program.exports.push(export);
//         self
//     }

//     /// Add exported tables.
//     pub fn exports<T>(mut self, exports: T) -> Self
//     where
//         T: IntoIterator<Item = ExportDirective>,
//     {
//         self.program.exports.extend(exports);
//         self
//     }

//     /// Add a rule.
//     pub fn rule(mut self, rule: Rule) -> Self {
//         self.program.rules.push(rule);
//         self
//     }

//     /// Add rules.
//     pub fn rules<T>(mut self, rules: T) -> Self
//     where
//         T: IntoIterator<Item = Rule>,
//     {
//         self.program.rules.extend(rules);
//         self
//     }

//     /// Add a fact.
//     pub fn fact(mut self, fact: Fact) -> Self {
//         self.program.facts.push(fact);
//         self
//     }

//     /// Add facts.
//     pub fn facts<T>(mut self, facts: T) -> Self
//     where
//         T: IntoIterator<Item = Fact>,
//     {
//         self.program.facts.extend(facts);
//         self
//     }

//     /// Mark predicate as output predicate.
//     pub fn output_predicate(self, predicate: Identifier) -> Self {
//         self.output_predicates([predicate])
//     }

//     /// Mark predicates as output predicates.
//     pub fn output_predicates<T>(mut self, predicates: T) -> Self
//     where
//         T: IntoIterator<Item = Identifier>,
//     {
//         self.program.output_predicates.extend(predicates);
//         self
//     }
// }

// impl Program {
//     /// Return a [builder][ProgramBuilder] for the [Program].
//     pub fn builder() -> ProgramBuilder {
//         Default::default()
//     }

//     /// Get the base IRI, if set.
//     #[must_use]
//     pub fn base(&self) -> Option<String> {
//         self.base.clone()
//     }

//     /// Return all rules in the program - immutable.
//     #[must_use]
//     pub fn rules(&self) -> &Vec<Rule> {
//         &self.rules
//     }

//     /// Return all facts in the program.
//     #[must_use]
//     pub fn facts(&self) -> &Vec<Fact> {
//         &self.facts
//     }

//     /// Return a HashSet of all predicates in the program (in rules and facts).
//     #[must_use]
//     pub fn predicates(&self) -> HashSet<Identifier> {
//         self.rules()
//             .iter()
//             .flat_map(|rule| {
//                 rule.head()
//                     .iter()
//                     .map(|atom| atom.predicate())
//                     .chain(rule.body().iter().map(|literal| literal.predicate()))
//             })
//             .chain(self.facts().iter().map(|atom| atom.0.predicate()))
//             .collect()
//     }

//     /// Return a HashSet of all idb predicates (predicates occuring rule heads) in the program.
//     #[must_use]
//     pub fn idb_predicates(&self) -> HashSet<Identifier> {
//         self.rules()
//             .iter()
//             .flat_map(|rule| rule.head())
//             .map(|atom| atom.predicate())
//             .collect()
//     }

//     /// Return a HashSet of all edb predicates (all predicates minus idb predicates) in the program.
//     #[must_use]
//     pub fn edb_predicates(&self) -> HashSet<Identifier> {
//         self.predicates()
//             .difference(&self.idb_predicates())
//             .cloned()
//             .collect()
//     }

//     /// Return an Iterator over all output predicates that
//     /// were explicitly marked in output directives.
//     pub fn output_predicates(&self) -> impl Iterator<Item = &Identifier> {
//         self.output_predicates.iter()
//     }

//     /// Add output predicates to the program.
//     pub fn add_output_predicates<T>(&mut self, predicates: T)
//     where
//         T: IntoIterator<Item = Identifier>,
//     {
//         self.output_predicates.extend(predicates);
//     }

//     /// Remove all output predicates of the program.
//     pub fn clear_output_predicates(&mut self) {
//         self.output_predicates.clear();
//     }

//     /// Return all prefixes in the program.
//     #[must_use]
//     pub fn prefixes(&self) -> &HashMap<String, String> {
//         &self.prefixes
//     }

//     /// Return all [ImportDirective]s of the program.
//     pub fn imports(&self) -> impl Iterator<Item = &ImportDirective> {
//         self.imports.iter()
//     }

//     /// Add [ImportDirective]s to the program.
//     pub fn add_imports<T>(&mut self, imports: T)
//     where
//         T: IntoIterator<Item = ImportDirective>,
//     {
//         self.imports.extend(imports);
//     }

//     /// Return all [ExportDirective]s of the program.
//     pub fn exports(&self) -> impl Iterator<Item = &ExportDirective> {
//         self.exports.iter()
//     }

//     /// Add [ExportDirective]s to the program.
//     pub fn add_exports<T>(&mut self, exports: T)
//     where
//         T: IntoIterator<Item = ExportDirective>,
//     {
//         self.exports.extend(exports);
//     }

//     /// Remove all [ExportDirective]s of the program.
//     pub fn clear_exports(&mut self) {
//         self.exports.clear();
//     }

//     /// Look up a given prefix.
//     #[must_use]
//     pub fn resolve_prefix(&self, tag: &str) -> Option<String> {
//         self.prefixes.get(tag).cloned()
//     }
// }
