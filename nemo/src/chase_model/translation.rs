//! This module defines [ProgramChaseTranslation].

pub(crate) mod aggregate;
pub(crate) mod fact;
pub(crate) mod filter;
pub(crate) mod import_export;
pub(crate) mod operation;
pub(crate) mod rule;

use std::collections::HashMap;

use nemo_physical::util::hook::FilterHook;

use crate::rule_model::{
    components::{tag::Tag, term::primitive::variable::Variable},
    program::Program,
};

use super::components::program::ChaseProgram;

#[derive(Debug, Default, Clone, Copy)]
struct FreshVariableGenerator {
    /// Counter for generating ids for fresh variables
    fresh_variable_counter: usize,
}

impl FreshVariableGenerator {
    /// Create a fresh universal variable.
    pub fn create_fresh_variable(&mut self) -> Variable {
        self.fresh_variable_counter += 1;
        Variable::universal(&format!("__VARIABLE_{}", self.fresh_variable_counter))
    }
}

/// Object for translating a [Program] into a [ChaseProgram]
#[derive(Debug, Default)]
pub(crate) struct ProgramChaseTranslation {
    /// Generator for fresh variables
    fresh_variable_generator: FreshVariableGenerator,
    /// Map associating each predicate with its arity
    predicate_arity: HashMap<Tag, usize>,
    /// Optional hook with external code supplied to each rule
    hook: Option<FilterHook>,
}

impl ProgramChaseTranslation {
    /// Initialize a new [ProgramChaseTranslation].
    pub fn new() -> Self {
        Self {
            fresh_variable_generator: FreshVariableGenerator::default(),
            predicate_arity: HashMap::default(),
            hook: None,
        }
    }

    /// Translate a [Program] into a [ChaseProgram].
    pub(crate) fn translate(&mut self, mut program: Program) -> ChaseProgram {
        self.hook = program.hook();

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
    fn create_fresh_variable(&mut self) -> Variable {
        self.fresh_variable_generator.create_fresh_variable()
    }
}
