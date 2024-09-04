//! This module defines [ChaseProgram].

use crate::rule_model::components::tag::Tag;

use super::{atom::ground_atom::GroundAtom, rule::ChaseRule};

#[derive(Debug, Default)]
pub(crate) struct ChaseProgram {
    // import_handlers: Vec<(Identifier, Box<dyn ImportExportHandler>)>,
    // export_handlers: Vec<(Identifier, Box<dyn ImportExportHandler>)>,
    /// Rules
    rules: Vec<ChaseRule>,
    /// Facts
    facts: Vec<GroundAtom>,
    /// Predicates marked as output
    output_predicates: Vec<Tag>,
}

impl ChaseProgram {
    /// Create a new [ChaseProgram].
    pub(crate) fn new(
        rules: Vec<ChaseRule>,
        facts: Vec<GroundAtom>,
        output_predicates: Vec<Tag>,
    ) -> Self {
        Self {
            rules,
            facts,
            output_predicates,
        }
    }

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
}
