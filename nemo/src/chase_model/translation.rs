//! This module defines [ProgramChaseTranslation].

pub(crate) mod aggregate;
pub(crate) mod fact;
pub(crate) mod filter;
pub(crate) mod import;
pub(crate) mod operation;
pub(crate) mod rule;

use std::collections::HashMap;

use crate::rule_model::{components::tag::Tag, program::Program};

use super::components::program::ChaseProgram;

/// Object for translating a [Program] into a [ChaseProgram]
#[derive(Debug)]
pub(crate) struct ProgramChaseTranslation {
    /// Counter for generating ids for fresh variables
    fresh_variable_counter: usize,
    /// Map associating each predicate with its arity
    predicate_arity: HashMap<Tag, usize>,
}

impl ProgramChaseTranslation {
    /// Initialize a new [ProgramChaseTranslation].
    pub fn new() -> Self {
        Self {
            fresh_variable_counter: 0,
            predicate_arity: HashMap::default(),
        }
    }

    /// Translate a [Program] into a [ChaseProgram].
    pub(crate) fn translate(&mut self, mut program: Program) -> ChaseProgram {
        let mut result = ChaseProgram::default();

        for fact in program.facts() {
            result.add_fact(self.build_fact(fact));
        }

        for rule in program.rules_mut() {
            result.add_rule(self.build_rule(rule));
        }

        for output in program.outputs() {
            result.add_output_predicate(output.predicate().clone());
        }

        for import in program.imports() {
            if let Some(arity) = import.expected_arity() {
                self.predicate_arity
                    .insert(import.predicate().clone(), arity);
            }
        }

        for export in program.exports() {
            if let Some(arity) = export.expected_arity() {
                self.predicate_arity
                    .insert(export.predicate().clone(), arity);
            }
        }

        for import in program.imports() {
            result.add_import(self.build_import(import));
        }

        for export in program.exports() {
            result.add_export(self.build_export(export));
        }

        result
    }

    /// Create a fresh variable name
    fn create_fresh_variable(&mut self) -> String {
        self.fresh_variable_counter += 1;
        format!("__VARIABLE_{}", self.fresh_variable_counter)
    }
}
