//! This module defines [TransformationSkolemize].

use std::collections::HashSet;

use crate::rule_model::{
    components::{
        literal::Literal,
        term::{
            function::FunctionTerm,
            primitive::{variable::Variable, Primitive},
            Term,
        },
        ComponentIdentity, IterableVariables,
    },
    error::ValidationReport,
    pipeline::{
        commit::ProgramCommit,
        state::{ExtendStatementKind, ExtendStatementValidity},
        ProgramPipeline,
    },
    program::ProgramRead,
};

use super::ProgramTransformation;

/// Program transformation
///
/// Replaces each existential variable with skolem terms.
#[derive(Debug, Copy, Clone)]
pub struct TransformationSkolemize {
    /// Skolem function counter
    skolem_count: usize,
}

impl ProgramTransformation for TransformationSkolemize {
    fn keep(&self) -> ExtendStatementValidity {
        ExtendStatementValidity::Keep(ExtendStatementKind::All)
    }

    fn apply(
        mut self,
        commit: &mut ProgramCommit,
        pipeline: &ProgramPipeline,
    ) -> Result<(), ValidationReport> {
        for rule in pipeline.rules() {
            let head_variables = || rule.head().iter().flat_map(|atom| atom.variables());
            if !head_variables().any(|variable| variable.is_existential()) {
                continue;
            }

            let head_variables = head_variables().collect::<HashSet<_>>();

            let mut frontier_variables = HashSet::new();
            for literal in rule.body() {
                let variables = match literal {
                    Literal::Positive(atom) => atom.variables(),
                    Literal::Operation(operation) => operation.variables(),
                    Literal::Negative(_) => continue,
                };

                for variable in variables {
                    if head_variables.contains(variable) {
                        frontier_variables.insert(variable.clone());
                    }
                }
            }
            let frontier_variables = frontier_variables
                .into_iter()
                .map(Term::from)
                .collect::<Vec<_>>();

            let mut new_rule = rule.clone();
            for head_atom in new_rule.head_mut() {
                for term in head_atom.terms_mut() {
                    if let Term::Primitive(Primitive::Variable(Variable::Existential(_))) = term {
                        self.skolem_count += 1;
                        let name = format!("_SKOLEM_{}", self.skolem_count);
                        *term = Term::from(FunctionTerm::new(&name, frontier_variables.clone()));
                    }
                }
            }

            commit.delete(rule.id());
        }

        Ok(())
    }
}
