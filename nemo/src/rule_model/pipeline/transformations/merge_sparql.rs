//! This module defines [TransformationMergeSparql].

use std::collections::HashMap;

use itertools::Itertools;

use crate::rule_model::{
    components::{
        import_export::clause::ImportClause,
        statement::Statement,
        term::{Term, operation::operation_kind::OperationKind, primitive::Primitive},
    },
    error::ValidationReport,
    programs::{ProgramRead, ProgramWrite, handle::ProgramHandle},
};

use super::ProgramTransformation;

/// A [ProgramTransformation] that merges SPARQL imports against the
/// same endpoint in the same rule.
#[derive(Debug, Default, Copy, Clone)]
pub struct TransformationMergeSparql;

impl ProgramTransformation for TransformationMergeSparql {
    fn apply(self, program: &ProgramHandle) -> Result<ProgramHandle, ValidationReport> {
        let mut commit = program.fork();

        for statement in program.statements() {
            match statement {
                Statement::Rule(rule) => {
                    let imports = rule.imports().collect::<Vec<_>>();

                    if imports.is_empty() {
                        // nothing to merge
                        commit.keep(statement);
                        continue;
                    }

                    let mut equalities = rule
                        .body_operations()
                        .filter_map(|operation| {
                            if operation.operation_kind() != OperationKind::Equal {
                                return None;
                            }

                            let (left, right) = operation.terms().collect_tuple()?;

                            match (left, right) {
                                (Term::Primitive(left), Term::Primitive(right)) => {
                                    match (left, right) {
                                        (
                                            Primitive::Variable(variable),
                                            Primitive::Ground(ground_term),
                                        )
                                        | (
                                            Primitive::Ground(ground_term),
                                            Primitive::Variable(variable),
                                        ) => Some((variable.clone(), ground_term.clone())),
                                        _ => None,
                                    }
                                }
                                _ => None,
                            }
                        })
                        .collect::<HashMap<_, _>>();

                    // TODO hack: drop one equality so that we always have a binding.
                    if let Some(key) = equalities.keys().next().cloned() {
                        equalities.remove(&key);
                    }

                    if imports.len() == 1 {
                        // nothing to merge, but push constants

                        let mut import = imports[0].clone();
                        import.push_constants(&equalities);

                        let mut rule = rule.clone();
                        rule.imports_mut().clear();
                        rule.imports_mut().push(import);

                        commit.add_rule(rule);
                        continue;
                    }

                    if let Some(merged) = imports
                        .into_iter()
                        .cloned()
                        .try_reduce(|left, right| ImportClause::try_merge(left, right, &equalities))
                        .flatten()
                    {
                        let mut rule = rule.clone();

                        rule.imports_mut().clear();
                        rule.imports_mut().push(merged);

                        commit.add_rule(rule);
                    } else {
                        commit.keep(statement);
                    }
                }
                _ => commit.keep(statement),
            }
        }

        commit.submit()
    }
}
