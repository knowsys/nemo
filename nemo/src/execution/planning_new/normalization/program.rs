//! This module defines [NormalizedProgram].

use std::collections::{HashMap, HashSet};

use nemo_physical::datavalues::AnyDataValue;

use crate::{
    execution::planning_new::normalization::{
        atom::ground::GroundAtom, export::ExportInstruction, import::ImportInstruction,
        rule::NormalizedRule,
    },
    io::format_builder::ImportExportBuilder,
    rule_model::{components::tag::Tag, programs::ProgramRead},
};

/// Represents a normalized nemo program
#[derive(Debug, Default, Clone)]
pub struct NormalizedProgram {
    /// Imports
    imports: Vec<ImportInstruction>,
    /// Exports
    exports: Vec<ExportInstruction>,
    /// Rules
    rules: Vec<NormalizedRule>,
    /// Facts
    facts: Vec<GroundAtom>,
    /// Output predicates
    output_predicates: Vec<Tag>,

    /// Predicate arities
    predicate_arities: HashMap<Tag, usize>,
    /// Set of "derived" predicates
    /// A predicate is considered derived if it appears in the head of a rule
    derived_predicates: HashSet<Tag>,
    /// Map from a predicate to all the rules where that predicate appears in its body
    predicate_to_rule_body: HashMap<Tag, HashSet<usize>>,
    /// Map from a predicate to all the rules where that predicate appears in its head
    predicate_to_rule_head: HashMap<Tag, HashSet<usize>>,
    /// Set of datavalues used in this program
    datavalues: HashSet<AnyDataValue>,
}

impl NormalizedProgram {
    /// Return a list of rules contained in this program.
    pub fn rules(&self) -> &Vec<NormalizedRule> {
        &self.rules
    }

    /// Return a list of imports contained in this program.
    pub fn imports(&self) -> &Vec<ImportInstruction> {
        &self.imports
    }

    /// Return a list of exports contained in this program.
    pub fn exports(&self) -> &Vec<ExportInstruction> {
        &self.exports
    }

    /// Return a list of facts contained in this program.
    pub fn facts(&self) -> &Vec<GroundAtom> {
        &self.facts
    }

    /// Return a list of output predicates contained in this program.
    pub fn output_predicates(&self) -> &Vec<Tag> {
        &self.output_predicates
    }

    /// Return the arity of a predicate.
    ///
    /// # Panics
    /// Panics if the requested predicate doesn't exist.
    pub fn predicate_arity(&self, predicate: &Tag) -> usize {
        *self
            .predicate_arities
            .get(predicate)
            .expect("arity of this predicate is unknown")
    }

    /// Return the set of derived predicates.
    ///
    /// A predicate is considered "derived" if it appears in the head of a rule.
    pub fn derived_predicates(&self) -> &HashSet<Tag> {
        &self.derived_predicates
    }

    /// Return the set of rule indices that contain the given predicate
    /// in the rule's positive body.
    ///
    /// # Panics
    /// Panics if the requested predicate doesn't exist.
    pub fn rules_with_body_predicate(&self, predicate: &Tag) -> &HashSet<usize> {
        self.predicate_to_rule_body
            .get(predicate)
            .expect("predicate is unknown")
    }

    /// Return the set of rule indices that contain the given predicate
    /// in the rule's head.
    ///
    /// # Panics
    /// Panics if the requested predicate doesn't exist.
    pub fn rules_with_head_predicate(&self, predicate: &Tag) -> &HashSet<usize> {
        self.predicate_to_rule_head
            .get(predicate)
            .expect("predicate is unknown")
    }

    /// Return the set of [AnyDataValue]s that appear in this program.
    pub fn datavalues(&self) -> &HashSet<AnyDataValue> {
        &self.datavalues
    }

    /// Associate an arity with a given predicate.
    ///
    /// # Panics
    /// Panics if the arity contradicts the previous arity.
    fn add_predicate_arity(&mut self, predicate: Tag, arity: usize) {
        if let Some(previous_arity) = self.predicate_arities.insert(predicate.clone(), arity) {
            if previous_arity != arity {
                panic!("predicate {predicate} appears with multiple arities");
            }
        }
    }

    /// Mark a predicate as "derived".
    fn add_derived_predicate(&mut self, predicate: Tag) {
        self.derived_predicates.insert(predicate);
    }

    /// Add a [AnyDataValue].
    fn add_datavalue(&mut self, value: AnyDataValue) {
        self.datavalues.insert(value);
    }

    /// Associate a predicate with a rule index,
    /// where the predicate appears in the positive body of that rule.
    fn add_body_predicate_rule(&mut self, predicate: Tag, rule: usize) {
        self.predicate_to_rule_body
            .entry(predicate)
            .or_default()
            .insert(rule);
    }

    /// Associate a predicate with a rule index,
    /// where the predicate appears in the head of that rule.
    fn add_head_predicate_rule(&mut self, predicate: Tag, rule: usize) {
        self.predicate_to_rule_head
            .entry(predicate)
            .or_default()
            .insert(rule);
    }
}

impl NormalizedProgram {
    /// Normalize a [crate::rule_model::programs::program::Program]
    /// into a [NormalizedProgram].
    ///
    /// # Panics
    /// Panics if the program is ill-formed.
    pub fn normalize_program(
        import_export_builder: &ImportExportBuilder,
        program: &crate::rule_model::programs::program::Program,
    ) -> Self {
        let mut result = Self::default();

        // Handle facts
        for fact in program.facts() {
            let Some(ground_atom) = GroundAtom::normalize_fact(fact) else {
                continue;
            };

            result.add_predicate_arity(ground_atom.predicate(), ground_atom.arity());
            result.facts.push(ground_atom);
        }

        // Handle rules
        for (rule_index, rule) in program.rules().enumerate() {
            let normalized_rule = NormalizedRule::normalize_rule(import_export_builder, rule);

            for (predicate, arity) in normalized_rule.predicates() {
                result.add_predicate_arity(predicate, arity);
            }

            for body in normalized_rule.positive() {
                result.add_body_predicate_rule(body.predicate(), rule_index);
            }

            for head in normalized_rule.head() {
                result.add_head_predicate_rule(head.predicate(), rule_index);
                result.add_derived_predicate(head.predicate());
            }

            result.rules.push(normalized_rule);
        }

        // Handle imports
        for import in program.imports() {
            let arity = result.predicate_arities.get(import.predicate()).cloned();
            let normalized_import =
                ImportInstruction::normalize_import(import_export_builder, import, arity);

            result.add_predicate_arity(normalized_import.predicate(), normalized_import.arity());
            result.imports.push(normalized_import);
        }

        // Handle exports
        for export in program.exports() {
            let arity = result.predicate_arities.get(export.predicate()).cloned();
            let normalized_exports =
                ExportInstruction::normalize_import(import_export_builder, export, arity);

            result.add_predicate_arity(normalized_exports.predicate(), normalized_exports.arity());
            result.exports.push(normalized_exports);
        }

        // Calculate variable order
        // TODO

        result
    }
}
