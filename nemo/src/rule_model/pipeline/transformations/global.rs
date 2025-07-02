//! This module defines [TransformationGlobal].

use std::collections::{HashMap, HashSet};

use crate::rule_model::{
    components::{
        term::primitive::{ground::GroundTerm, variable::global::GlobalVariable},
        ComponentSource, IterableVariables,
    },
    error::ValidationReport,
    origin::Origin,
    programs::{handle::ProgramHandle, ProgramRead, ProgramWrite},
    substitution::Substitution,
};

use super::ProgramTransformation;

/// Program transformation
///
/// Replaces each occurrence of a global variable
/// with the term it evaluates to.
#[derive(Debug)]
pub struct TransformationGlobal<'a> {
    /// Externally set global variables
    external: &'a HashMap<GlobalVariable, GroundTerm>,
}

impl<'a> TransformationGlobal<'a> {
    /// Create a new [TransformationGlobal].
    pub fn new(external: &'a HashMap<GlobalVariable, GroundTerm>) -> Self {
        Self { external }
    }

    /// Compute a [Substitution] based on external parameters
    /// and paramater declarations within the current program.
    fn subsitution(
        external: &HashMap<GlobalVariable, GroundTerm>,
        pipeline: &ProgramHandle,
    ) -> Substitution {
        let mut ground_set = external.keys().cloned().collect::<HashSet<_>>();
        let external = external.iter().map(|(variable, term)| {
            let mut term = term.clone();
            term.set_origin(Origin::Extern);

            (variable.clone(), term)
        });

        let mut substitution = Substitution::new(external);

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
}

impl<'a> ProgramTransformation for TransformationGlobal<'a> {
    fn apply(self, program: &ProgramHandle) -> Result<ProgramHandle, ValidationReport> {
        let mut commit = program.fork();

        let substitution = Self::subsitution(self.external, program);

        for statement in program.statements() {
            if !statement.is_parameter()
                && statement.variables().any(|variable| variable.is_global())
            {
                let mut new_statement = statement.clone();
                substitution.apply(&mut new_statement);

                commit.add_statement(new_statement);
            } else {
                commit.keep(statement);
            }
        }

        commit.submit()
    }
}
