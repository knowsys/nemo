//! This module defines [ProgramChaseTranslation].

pub(crate) mod aggregate;
pub(crate) mod fact;
pub(crate) mod filter;
pub(crate) mod import_export;
pub(crate) mod operation;
pub(crate) mod rule;

use std::collections::HashMap;

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
}

impl ProgramChaseTranslation {
    /// Initialize a new [ProgramChaseTranslation].
    pub fn new() -> Self {
        Self {
            fresh_variable_generator: FreshVariableGenerator::default(),
            predicate_arity: HashMap::default(),
        }
    }

    /// Translate a [Program] into a [ChaseProgram].
    pub(crate) fn translate(&mut self, mut program: Program) -> ChaseProgram {
        let mut result = ChaseProgram::default();

        // TODO:

        // self.predicate_arity = program
        //     .arities()
        //     .iter()
        //     .map(|(tag, (arity, _))| (tag.clone(), *arity))
        //     .collect();

        for fact in program.facts() {
            if let Some(fact) = self.build_fact(fact) {
                result.add_fact(fact);
            }
        }

        for rule in program.rules_mut() {
            result.add_rule(self.build_rule(rule));
        }

        for output in program.outputs() {
            result.add_output_predicate(output.predicate().clone());
        }

        // TODO:

        // for import_directive in program.imports() {
        //     let import_builder = import_directive
        //         .validate(&mut ValidationErrorBuilder::default())
        //         .expect("validation already happened");
        //     result.add_import(self.build_import(import_directive, &import_builder));
        // }

        // for export_directive in program.exports() {
        //     let export_builder = export_directive
        //         .validate(&mut ValidationErrorBuilder::default())
        //         .expect("validation already happened");
        //     result.add_export(self.build_export(export_directive, &export_builder));
        // }

        result
    }

    /// Create a fresh variable name
    fn create_fresh_variable(&mut self) -> Variable {
        self.fresh_variable_generator.create_fresh_variable()
    }
}
