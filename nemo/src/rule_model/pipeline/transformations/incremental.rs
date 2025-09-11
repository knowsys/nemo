//! This module defines [TransformationIncremental].

use std::collections::{HashMap, HashSet};

use crate::{
    io::{format_builder::SupportedFormatTag, formats::sparql::SparqlTag},
    rule_model::{
        components::{
            import_export::{clause::ImportClause, ImportDirective},
            literal::Literal,
            rule::Rule,
            statement::Statement,
            tag::Tag,
            IterableVariables,
        },
        error::ValidationReport,
        programs::{handle::ProgramHandle, ProgramRead, ProgramWrite},
    },
};

use super::ProgramTransformation;

/// Program transformation
///
/// Inlines certain import statements into rules.
#[derive(Debug, Clone, Copy)]
pub struct TransformationIncremental {}

impl TransformationIncremental {
    /// Create a new [TransformationIncremental].
    pub fn new() -> Self {
        Self {}
    }

    /// Compute the predicates that will be inlined in this transformation.
    ///
    /// Returns a hash map containing those predicates together with the
    /// associated import statement from which they originated.
    fn incremental_predicates(program: &ProgramHandle) -> HashMap<Tag, &ImportDirective> {
        let mut incremental_predicates = HashMap::<Tag, &ImportDirective>::default();
        let mut normal_predicates = HashSet::<Tag>::default();

        for statement in program.statements() {
            match statement {
                Statement::Import(import) => {
                    if let Some(builder) = import.builder() {
                        if let SupportedFormatTag::Sparql(SparqlTag::Sparql) = builder.format() {
                            if incremental_predicates
                                .insert(import.predicate().clone(), import)
                                .is_some()
                            {
                                normal_predicates.insert(import.predicate().clone());
                            }
                        } else {
                            normal_predicates.insert(import.predicate().clone());
                        }
                    }
                }
                Statement::Fact(fact) => {
                    normal_predicates.insert(fact.predicate().clone());
                }
                Statement::Rule(rule) => {
                    for head in rule.head() {
                        normal_predicates.insert(head.predicate());
                    }
                    for atom in rule.body_negative() {
                        normal_predicates.insert(atom.predicate().clone());
                    }
                }
                Statement::Export(export) => {
                    normal_predicates.insert(export.predicate().clone());
                }
                Statement::Output(output) => {
                    normal_predicates.insert(output.predicate().clone());
                }
                Statement::Parameter(_) => {}
            }
        }

        incremental_predicates.retain(|predicate, _| !normal_predicates.contains(predicate));

        incremental_predicates
    }

    /// Given a rule with inlined import predicates,
    /// compute a rule that includes the corresponding import statements.
    fn incremental_rule(
        rule: &Rule,
        incremental_predicates: &HashMap<Tag, &ImportDirective>,
    ) -> Rule {
        let mut result = rule.clone();

        let mut import_clauses = Vec::<ImportClause>::default();

        result.body_mut().retain(|literal| {
            if let Literal::Positive(atom) = literal {
                if let Some(&import) = incremental_predicates.get(&atom.predicate()) {
                    let variables = atom.variables().cloned().collect::<Vec<_>>();
                    let clause = ImportClause::new(import.clone(), variables);

                    import_clauses.push(clause);
                }
            }

            true
        });

        for import in import_clauses {
            result.add_import(import);
        }

        result
    }
}

impl ProgramTransformation for TransformationIncremental {
    fn apply(self, program: &ProgramHandle) -> Result<ProgramHandle, ValidationReport> {
        let mut commit = program.fork();

        let incremental_predicates = Self::incremental_predicates(program);

        for statement in program.statements() {
            match statement {
                Statement::Rule(rule) => {
                    if rule
                        .body_positive()
                        .any(|atom| incremental_predicates.contains_key(&atom.predicate()))
                    {
                        let new_rule = Self::incremental_rule(rule, &incremental_predicates);
                        commit.add_rule(new_rule);
                    } else {
                        commit.keep(statement);
                    }
                }
                Statement::Import(import) => {
                    if !incremental_predicates.contains_key(import.predicate()) {
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
