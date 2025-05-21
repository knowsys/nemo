//! This module defines [TransformationGlobal].

use std::collections::{HashMap, HashSet};

use crate::rule_model::{
    components::{
        term::{
            primitive::{ground::GroundTerm, variable::global::GlobalVariable},
            Term,
        },
        ComponentIdentity, IterablePrimitives, IterableVariables, ProgramComponent,
    },
    error::ValidationReport,
    pipeline::{
        commit::ProgramCommit,
        state::{ExtendStatementKind, ExtendStatementValidity},
        ProgramPipeline,
    },
    program::ProgramRead,
    substitution::Substitution,
};

use super::ProgramTransformation;

/// Program transformation
///
/// Replaces each occurrence of a global variable
/// with the term it evaluates to.
#[derive(Debug)]
pub struct TransformationGlobal {
    /// Externally set global variables
    external: HashMap<GlobalVariable, GroundTerm>,
}

impl TransformationGlobal {
    /// Create a new [TransformationGlobal].
    pub fn new(external: HashMap<GlobalVariable, GroundTerm>) -> Self {
        Self { external }
    }

    /// Compute a [Substitution] based on external parameters
    /// and paramater declarations within the current program.
    fn subsitution(
        external: HashMap<GlobalVariable, GroundTerm>,
        pipeline: &ProgramPipeline,
    ) -> Substitution {
        let mut ground_set = external.keys().cloned().collect::<HashSet<_>>();
        let mut substitution = Substitution::new(external.into_iter());

        let mut ground_count: usize = ground_set.len();
        loop {
            for parameter in pipeline.parameters() {
                if ground_set.contains(parameter.variable()) {
                    continue;
                }

                let Some(mut expression) = parameter.expression().cloned() else {
                    continue;
                };

                substitution.apply(&mut expression);

                if expression.is_ground() {
                    ground_set.insert(parameter.variable().clone());
                    substitution.insert(parameter.variable().clone(), expression);
                }
            }

            if ground_count == ground_set.len() {
                return substitution;
            }

            ground_count = ground_set.len();
        }
    }

    /// Takes an iterator over [ProgramComponent]s
    /// checking whether they contain a global variable
    /// and if so applying
    fn apply_subsitution<'a, Iter, Component>(
        iterator: Iter,
        substitution: &'a Substitution,
    ) -> impl Iterator<Item = (&'a Component, Component)> + 'a
    where
        Component:
            ProgramComponent + IterablePrimitives<TermType = Term> + IterableVariables + Clone + 'a,
        Iter: Iterator<Item = &'a Component> + 'a,
    {
        iterator.filter_map(|old_component| {
            if old_component
                .variables()
                .any(|variable| variable.is_global())
            {
                let mut new_component = old_component.clone();
                substitution.apply(&mut new_component);

                Some((old_component, new_component))
            } else {
                None
            }
        })
    }
}

impl ProgramTransformation for TransformationGlobal {
    fn keep(&self) -> ExtendStatementValidity {
        ExtendStatementValidity::Keep(ExtendStatementKind::All)
    }

    fn apply(
        self,
        commit: &mut ProgramCommit,
        pipeline: &ProgramPipeline,
    ) -> Result<(), ValidationReport> {
        let substitution = Self::subsitution(self.external, pipeline);

        for (old_import, new_import) in Self::apply_subsitution(pipeline.imports(), &substitution) {
            commit.delete(old_import.id());
            commit.add_import(new_import);
        }

        for (old_rule, new_rule) in Self::apply_subsitution(pipeline.rules(), &substitution) {
            commit.delete(old_rule.id());
            commit.add_rule(new_rule);
        }

        for (old_fact, new_fact) in Self::apply_subsitution(pipeline.facts(), &substitution) {
            commit.delete(old_fact.id());
            commit.add_fact(new_fact);
        }

        Ok(())
    }
}
