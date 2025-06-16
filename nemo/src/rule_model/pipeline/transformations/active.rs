//! This module defines [TransformationActive].

use std::collections::HashSet;

use crate::rule_model::{
    components::{statement::Statement, tag::Tag, ComponentIdentity},
    error::ValidationReport,
    pipeline::id::ProgramComponentId,
    programs::{handle::ProgramHandle, ProgramRead},
};

use super::ProgramTransformation;

/// Program transformation
///
/// Removes any rule that is not needed in the output.
#[derive(Debug, Default, Clone, Copy)]
pub struct TransformationActive {}

impl TransformationActive {
    /// Compute the required predicates and rules
    fn required(program: &ProgramHandle) -> (HashSet<Tag>, HashSet<ProgramComponentId>) {
        let mut predicates = HashSet::<Tag>::new();
        let mut rules = HashSet::new();

        for output in program.outputs() {
            predicates.insert(output.predicate().clone());
        }

        for export in program.exports() {
            predicates.insert(export.predicate().clone());
        }

        let mut required_count: usize = 0;
        while required_count != predicates.len() {
            required_count = predicates.len();

            for rule in program.rules() {
                if rules.contains(&rule.id()) {
                    continue;
                }

                if rule
                    .head()
                    .iter()
                    .any(|atom| predicates.contains(&atom.predicate()))
                {
                    for predicate in rule.body().iter().filter_map(|literal| literal.predicate()) {
                        predicates.insert(predicate);
                    }

                    rules.insert(rule.id());
                }
            }
        }

        (predicates, rules)
    }
}

impl ProgramTransformation for TransformationActive {
    fn apply(self, program: &ProgramHandle) -> Result<ProgramHandle, ValidationReport> {
        let mut commit = program.fork();

        let (predicates, rules) = Self::required(&program);

        for statement in program.statements() {
            match statement {
                Statement::Rule(rule) => {
                    if rules.contains(&rule.id()) {
                        commit.keep(rule);
                    }
                }
                Statement::Fact(fact) => {
                    if predicates.contains(fact.predicate()) {
                        commit.keep(fact);
                    }
                }
                Statement::Import(import) => {
                    if predicates.contains(import.predicate()) {
                        commit.keep(import);
                    }
                }
                Statement::Export(_) | Statement::Output(_) | Statement::Parameter(_) => {
                    commit.keep(statement)
                }
            }
        }

        commit.submit()
    }
}
