//! This module defines [TransformationFilterImports].

use std::collections::{HashMap, HashSet};

use crate::rule_model::{
    components::{
        ComponentIdentity, rule::Rule, statement::Statement, tag::Tag,
    },
    error::ValidationReport,
    pipeline::id::ProgramComponentId,
    programs::{ProgramRead, ProgramWrite, handle::ProgramHandle},
};

use super::ProgramTransformation;

/// Program transformation
///
/// Pushes conditions of simple rule into the import statement
#[derive(Debug, Clone, Copy, Default)]
pub struct TransformationFilterImports {}

impl TransformationFilterImports {
    /// Create a new [TransformationFilterImports].
    pub fn new() -> Self {
        Self {}
    }

    /// Check if rule can be internalized into an import.
    ///
    /// If this is the case, it returns the predicate that is imported.
    fn check_rule(rule: &Rule) -> Option<Tag> {
        if rule.body_negative().next().is_some() {
            return None;
        }

        if rule.body_positive().count() != 1 {
            return None;
        }

        let Some(import_atom) = &rule.body_positive().next() else {
            return None;
        };

        let head_predicates = rule
            .head()
            .iter()
            .map(|atom| atom.predicate())
            .collect::<HashSet<_>>();

        if head_predicates.len() != 1 {
            return None;
        }

        if rule.head().iter().any(|atom| {
            atom.terms()
                .any(|term| term.is_aggregate() || term.is_existential_variable())
        }) {
            return None;
        }

        Some(import_atom.predicate())
    }

    /// Check if there is only a single predicate used in all heads of all given rules.
    fn same_head_predicate(rules: &Vec<&Rule>) -> bool {
        let predicates = rules
            .iter()
            .flat_map(|rule| rule.head().iter().map(|atom| atom.predicate()))
            .collect::<HashSet<_>>();

        predicates.len() == 1
    }

    /// Return a set of predicates that need to be imported fully
    fn forbidden_predicates(program: &ProgramHandle) -> HashSet<Tag> {
        let mut forbidden = HashSet::<Tag>::default();

        for statement in program.statements() {
            match statement {
                Statement::Rule(rule) => {
                    if Self::check_rule(rule).is_none() {
                        for head in rule.head() {
                            forbidden.insert(head.predicate());
                        }
                        for atom in rule.body_positive().chain(rule.body_negative()) {
                            forbidden.insert(atom.predicate());
                        }
                    }
                }
                Statement::Fact(fact) => {
                    forbidden.insert(fact.predicate().clone());
                }
                Statement::Export(export) => {
                    forbidden.insert(export.predicate().clone());
                }
                Statement::Output(output) => {
                    forbidden.insert(output.predicate().clone());
                }
                Statement::Import(_) => {}
                Statement::Parameter(_) => {}
            }
        }

        forbidden
    }

    /// Return a set of internalized rules and
    /// the imports associated with the corresponding rules.
    fn import_rules(
        program: &ProgramHandle,
    ) -> (
        HashMap<ProgramComponentId, Vec<&Rule>>,
        HashSet<ProgramComponentId>,
    ) {
        let mut import_predicates = HashSet::<Tag>::default();
        let mut import_map = HashMap::<ProgramComponentId, Vec<&Rule>>::default();
        let mut predicate_map = HashMap::<Tag, Vec<ProgramComponentId>>::default();

        let forbidden = Self::forbidden_predicates(program);

        for import in program.imports() {
            if forbidden.contains(import.predicate()) {
                continue;
            }

            import_predicates.insert(import.predicate().clone());
            predicate_map
                .entry(import.predicate().clone())
                .or_insert_with(Vec::default)
                .push(import.id());
        }

        for rule in program.rules() {
            if let Some(import_predicate) = Self::check_rule(rule)
                && let Some(imports) = predicate_map.get(&import_predicate)
            {
                for &import in imports {
                    import_map
                        .entry(import)
                        .or_insert_with(Vec::default)
                        .push(rule);
                }
            }
        }

        import_map.retain(|_, rules| Self::same_head_predicate(rules));

        let internalized_rules = import_map
            .iter()
            .flat_map(|(_, rules)| rules.iter().map(|rule| rule.id()))
            .collect::<HashSet<_>>();

        (import_map, internalized_rules)
    }
}

impl ProgramTransformation for TransformationFilterImports {
    fn apply(self, program: &ProgramHandle) -> Result<ProgramHandle, ValidationReport> {
        let mut commit = program.fork();

        let (import_map, internalized_rules) = Self::import_rules(program);

        for statement in program.statements() {
            match statement {
                Statement::Rule(rule) => {
                    if !internalized_rules.contains(&rule.id()) {
                        commit.keep(statement);
                    }
                }
                Statement::Import(import) => {
                    if let Some(rules) = import_map.get(&import.id()) {
                        let mut new_import = import.clone();

                        // All head predicates of all rules are the same
                        let new_predicate = rules[0].head()[0].predicate();
                        new_import.set_predicate(new_predicate);

                        for &rule in rules.iter() {
                            new_import.add_filter_rule(rule.clone());
                        }

                        commit.add_import(new_import);
                    } else {
                        commit.keep(statement);
                    }
                }
                _ => {
                    commit.keep(statement);
                }
            }
        }

        commit.submit()
    }
}
