//! This module defines [TransformationFilterImports].

use std::collections::{HashMap, HashSet};

use crate::rule_model::{
    components::{literal::Literal, rule::Rule, statement::Statement, tag::Tag, ComponentIdentity},
    error::ValidationReport,
    pipeline::id::ProgramComponentId,
    programs::{handle::ProgramHandle, ProgramRead, ProgramWrite},
};

use super::ProgramTransformation;

/// Program transformation
///
/// Pushes conditions of simple rule into the import statement
#[derive(Debug, Clone, Copy)]
pub struct TransformationFilterImports {}

impl TransformationFilterImports {
    /// Create a new [TransformationFilterImports].
    pub fn new() -> Self {
        Self {}
    }

    /// Check if rule can be internalized into an import.
    fn check_import_rule(rule: &Rule) -> bool {
        if rule.body().len() != 1 {
            return false;
        }

        let Literal::Positive(_) = &rule.body()[0] else {
            return false;
        };

        let head_predicates = rule
            .head()
            .iter()
            .map(|atom| atom.predicate())
            .collect::<HashSet<_>>();

        head_predicates.len() == 1
    }

    /// Return a set of predicates such that if imported cannot be internalized
    fn forbidden_predicates(program: &ProgramHandle) -> HashSet<Tag> {
        let mut forbidden = HashSet::<Tag>::default();

        for statement in program.statements() {
            match statement {
                Statement::Rule(rule) => {
                    if !Self::check_import_rule(rule) {
                        for atom in rule.head() {
                            forbidden.insert(atom.predicate());
                        }
                    }
                }
                Statement::Fact(fact) => {
                    forbidden.insert(fact.predicate().clone());
                }
                Statement::Import(_)
                | Statement::Export(_)
                | Statement::Output(_)
                | Statement::Parameter(_) => {}
            }
        }

        forbidden
    }

    /// Check if rule can be internalized into an import.
    ///
    /// Returns the ids of the import statement if this is the case.
    fn internalize_rule(
        rule: &Rule,
        forbidden: &HashSet<Tag>,
        map: &HashMap<Tag, Vec<ProgramComponentId>>,
    ) -> Option<Vec<ProgramComponentId>> {
        if !Self::check_import_rule(rule) {
            return None;
        }

        let Some(head) = rule.head().first() else {
            return None;
        };

        if forbidden.contains(&head.predicate()) {
            return None;
        }

        map.get(&head.predicate()).cloned()
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

        for import in program.imports() {
            import_predicates.insert(import.predicate().clone());
            import_map.insert(import.id(), Vec::default());
            predicate_map
                .entry(import.predicate().clone())
                .or_insert_with(Vec::default)
                .push(import.id());
        }

        let forbidden = Self::forbidden_predicates(program);
        let mut internalized_rules = HashSet::<ProgramComponentId>::new();

        for rule in program.rules() {
            if let Some(imports) = Self::internalize_rule(rule, &forbidden, &predicate_map) {
                internalized_rules.insert(rule.id());

                for import in imports {
                    import_map
                        .entry(import)
                        .or_insert_with(Vec::default)
                        .push(rule);
                }
            }
        }

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
                    if internalized_rules.contains(&rule.id()) {
                        let mut copy_rule = rule.clone();
                        copy_rule.body_mut().retain(|literal| match literal {
                            Literal::Positive(_) | Literal::Negative(_) => true,
                            Literal::Operation(_) => false,
                        });

                        commit.add_rule(copy_rule);
                    } else {
                        commit.keep(statement);
                    }
                }
                Statement::Import(import) => {
                    if let Some(rules) = import_map.get(&import.id()) {
                        let mut new_import = import.clone();

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
