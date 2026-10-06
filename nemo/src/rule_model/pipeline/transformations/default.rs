//! This module defines [TransformationDefault].

use std::collections::HashSet;

use crate::{
    execution::execution_parameters::ExecutionParameters,
    rule_model::{
        error::ValidationReport,
        pipeline::transformations::{
            active::TransformationActive, incremental::TransformationIncremental,
            set_default_outputs::TransformationSetDefaultOutputs, validate::TransformationValidate,
        },
        programs::{ProgramRead, handle::ProgramHandle},
    },
};

use super::{
    ProgramTransformation, exports::TransformationExports,
    filter_imports::TransformationFilterImports, global::TransformationGlobal,
    merge_sparql::TransformationMergeSparql, normalize::TransformationNormalize,
};

/// Default transformation
///
/// This transformation will be applied to every nemo program
/// before executing
#[derive(Debug)]
pub struct TransformationDefault<'a> {
    /// Execution Parameters
    parameters: &'a ExecutionParameters,
}

impl<'a> TransformationDefault<'a> {
    /// Create a new [TransformationDefault].
    pub fn new(parameters: &'a ExecutionParameters) -> Self {
        Self { parameters }
    }
}

impl<'a> ProgramTransformation for TransformationDefault<'a> {
    fn apply(self, program: &ProgramHandle) -> Result<ProgramHandle, ValidationReport> {
        let mut commit = program.fork_full();

        program.validate_parameters(
            commit.report_mut(),
            self.parameters
                .global_variables
                .keys()
                .collect::<HashSet<_>>(),
        );

        commit
            .submit()?
            .transform(TransformationNormalize::default())?
            .transform(TransformationGlobal::new(&self.parameters.global_variables))?
            .transform(TransformationExports::new(
                self.parameters.export_parameters,
            ))?
            .transform(TransformationSetDefaultOutputs::default())?
            .transform(TransformationValidate::default())?
            .transform(TransformationActive::default())?
            .transform(TransformationIncremental::new())?
            .transform(TransformationMergeSparql)?
            .transform(TransformationFilterImports::new())
    }
}

#[cfg(test)]
mod test {
    use crate::{
        api::load_program_handle,
        execution::{
            planning::normalization::program::NormalizedProgram,
            tracing::rule_translation::RuleIdTranslation,
        },
        rule_model::programs::ProgramRead,
    };

    #[test]
    #[cfg_attr(miri, ignore)]
    fn rule_translation_default_transformations() {
        let program = r#"@parameter $t = 1 .
            @import ra :- sparql {endpoint = "http://example.org", query = "SELECT ?a ?b WHERE {?a ?b ?z.}"} .
            @import rb :- sparql {endpoint = "http://example.org", query = "SELECT ?a ?c WHERE {?a ?c ?z.}"} .
            seed(1) .
            fact(2, 5) .
            global_rule(?x) :- seed(?x), ?x = $t .
            normalized_rule(?x) :- fact(?x, 5) .
            dropped_rule(?x) :- seed(?x) .
            merged_rule(?a, ?b, ?c) :- seed(?a), ra(?a, ?b), rb(?a, ?c) .
            @output global_rule .
            @output normalized_rule .
            @output merged_rule ."#;
        let handle = load_program_handle(program.to_string(), String::default())
            .expect("program parses and validates");
        let normalized = NormalizedProgram::normalize_program(&handle);

        let translation = RuleIdTranslation::new(&handle, &normalized);

        assert!(handle.original_revision().rules().count() > normalized.rules().len());

        for (normalized_index, normalized_rule) in normalized.rules().iter().enumerate() {
            let original_index = translation.original_index(normalized_index);

            assert_eq!(
                translation.normalized_index(original_index),
                normalized_index
            );
            assert_eq!(
                translation.original_rule_unchecked(normalized_index).head()[0].predicate(),
                normalized_rule.head()[0].predicate()
            );
        }
    }
}
