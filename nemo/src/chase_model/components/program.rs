//! This module defines [ChaseProgram].

use std::collections::HashSet;

use nemo_physical::datavalues::AnyDataValue;

use crate::rule_model::components::{tag::Tag, term::primitive::Primitive, IterablePrimitives};

use super::{
    atom::{ground_atom::GroundAtom, ChaseAtom},
    export::ChaseExport,
    import::ChaseImport,
    rule::ChaseRule,
};

#[derive(Debug, Default, Clone)]
pub(crate) struct ChaseProgram {
    /// Imports
    imports: Vec<ChaseImport>,
    /// Exports
    exports: Vec<ChaseExport>,
    /// Rules
    rules: Vec<ChaseRule>,
    /// Facts
    facts: Vec<GroundAtom>,
    /// Predicates marked as output
    output_predicates: Vec<Tag>,
}

impl ChaseProgram {
    /// Add a new rule to the program.
    pub(crate) fn add_rule(&mut self, rule: ChaseRule) {
        self.rules.push(rule)
    }

    /// Add a new fact to the program.
    pub(crate) fn add_fact(&mut self, fact: GroundAtom) {
        self.facts.push(fact)
    }

    /// Add a new output predicate to the program.
    pub(crate) fn add_output_predicate(&mut self, predicate: Tag) {
        self.output_predicates.push(predicate)
    }

    /// Add a new import to the program.
    pub(crate) fn add_import(&mut self, import: ChaseImport) {
        self.imports.push(import);
    }

    /// Add a new export to the program.
    pub(crate) fn add_export(&mut self, export: ChaseExport) {
        self.exports.push(export);
    }
}

impl ChaseProgram {
    /// Return a list of rules contained in this program.
    pub(crate) fn rules(&self) -> &Vec<ChaseRule> {
        &self.rules
    }

    /// Return a list of imports contained in this program.
    pub(crate) fn imports(&self) -> &Vec<ChaseImport> {
        &self.imports
    }

    /// Return a list of exports contained in this program.
    pub(crate) fn exports(&self) -> &Vec<ChaseExport> {
        &self.exports
    }

    /// Return a list of facts contained in this program.
    pub(crate) fn facts(&self) -> &Vec<GroundAtom> {
        &self.facts
    }

    /// Return a list of output predicates contained in this program.
    pub(crate) fn _output_predicates(&self) -> &Vec<Tag> {
        &self.output_predicates
    }
}

impl ChaseProgram {
    /// Return a HashSet of all idb predicates (predicates occurring rule heads) in the program.
    pub fn idb_predicates(&self) -> HashSet<Tag> {
        self.rules()
            .iter()
            .flat_map(|rule| rule.head())
            .map(|atom| atom.predicate())
            .collect()
    }

    /// Return an iterator over all [AnyDataValue]s contained in this program.
    pub fn datavalues(&self) -> impl Iterator<Item = AnyDataValue> + '_ {
        let datavalues_rules = self
            .rules
            .iter()
            .flat_map(|rule| rule.primitive_terms())
            .filter_map(|primitive| match primitive {
                Primitive::Variable(_) => None,
                Primitive::Ground(ground) => Some(ground.value()),
            });
        let datavalues_facts = self.facts.iter().flat_map(|fact| fact.datavalues());

        datavalues_facts.chain(datavalues_rules)
    }
}
