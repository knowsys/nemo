//! This module defines [TransformationMergeSparql].

use crate::rule_model::{
    components::{
        import_export::{ImportDirective, clause::ImportClause},
        statement::Statement,
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

                    if !rule.body().is_empty() || imports.len() <= 1 {
                        // not a pure import, or nothing to merge
                        commit.keep(statement);
                        continue;
                    }

                    if let Some(merged) = imports
                        .into_iter()
                        .cloned()
                        .try_reduce(ImportClause::try_merge)
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
