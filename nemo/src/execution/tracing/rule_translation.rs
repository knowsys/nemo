//! This module defines [RuleIdTranslation], which maps rule identifiers
//! between the normalized program and the original program.

use std::collections::HashMap;

use crate::{
    execution::planning::normalization::program::NormalizedProgram,
    rule_model::{
        components::{ComponentIdentity, rule::Rule},
        programs::{ProgramRead, handle::ProgramHandle},
    },
};

use super::resolve_origin::tracing_resolve_origin_id;

/// Translates rule identifiers between the normalized program (used internally
/// during execution) and the original program (as seen by a frontend or library user).
///
/// On the frontend, a rule id ([super::shared::RuleId]) is an index into the original
/// program's list of rules. Internally, rules are addressed by their index in
/// [NormalizedProgram::rules].
///
/// This relies on the transformations between the original and the normalized
/// program being one-to-one on rules and recording their provenance (each
/// derived rule points back, via its [crate::rule_model::origin::Origin], to the
/// rule it was created from).
///
/// TODO: handling transformations that change the structure of the derivation trees
/// (https://github.com/knowsys/nemo/issues/774)
#[derive(Debug, Clone)]
pub(crate) struct RuleIdTranslation {
    /// The original program (the first revision of the pipeline), used to look up
    /// the [Rule] a normalized rule should be displayed as, by its index.
    original: ProgramHandle,

    /// For each normalized rule index, the corresponding original rule index.
    norm_to_orig: Vec<usize>,
    /// Inverse of [Self::norm_to_orig].
    orig_to_norm: HashMap<usize, usize>,
}

impl RuleIdTranslation {
    /// Build the translation for a given (transformed) [ProgramHandle]
    /// and the [NormalizedProgram] derived from it.
    pub(crate) fn new(handle: &ProgramHandle, normalized: &NormalizedProgram) -> Self {
        let original = handle.original_revision();

        let orig_index_by_id = original
            .rules()
            .enumerate()
            .map(|(index, rule)| (rule.id(), index))
            .collect::<HashMap<_, _>>();

        let mut norm_to_orig = Vec::with_capacity(normalized.rules().len());
        let mut orig_to_norm = HashMap::with_capacity(normalized.rules().len());

        for (normalized_index, rule) in normalized.rules().iter().enumerate() {
            let origin_id = tracing_resolve_origin_id(handle, rule.id());
            let original_index = *orig_index_by_id
                .get(&origin_id)
                .expect("each normalized rule has a corresponding original rule");

            norm_to_orig.push(original_index);

            let previous = orig_to_norm.insert(original_index, normalized_index);
            debug_assert!(
                previous.is_none(),
                "each original rule maps to at most one normalized rule"
            );
        }

        Self {
            original,
            norm_to_orig,
            orig_to_norm,
        }
    }

    /// Return the original [Rule] that the normalized rule with the given index
    /// should be displayed as, i.e. the rule the user wrote that this normalized
    /// rule was (transitively) derived from.
    ///
    /// # Panics
    /// Panics if `normalized` is not a valid normalized rule index.
    pub(crate) fn original_rule_unchecked(&self, normalized: usize) -> Rule {
        self.original
            .rule_by_index(self.norm_to_orig[normalized])
            .expect("normalized rule maps to an existing original rule")
            .clone()
    }

    /// Translate a normalized rule index into the original rule index used on the frontend.
    ///
    /// # Panics
    /// Panics if `normalized` is not a valid normalized rule index.
    pub(crate) fn original_index(&self, normalized: usize) -> usize {
        self.norm_to_orig[normalized]
    }

    /// Translate an original (frontend) rule index into the normalized rule index used internally.
    ///
    /// # Panics
    /// Panics if `original` has no corresponding normalized rule.
    pub(crate) fn normalized_index(&self, original: usize) -> usize {
        self.orig_to_norm
            .get(&original)
            .copied()
            .expect("original rule index has a corresponding normalized rule")
    }
}

#[cfg(test)]
mod test {
    use crate::{
        execution::planning::normalization::program::NormalizedProgram,
        rule_model::{
            components::{ComponentIdentity, ComponentSource, rule::Rule},
            error::ValidationReport,
            origin::Origin,
            pipeline::{ProgramPipeline, commit::ProgramCommit},
            programs::{ProgramRead, ProgramWrite, handle::ProgramHandle},
            translation::TranslationComponent,
        },
    };

    use super::RuleIdTranslation;

    /// Program handle from single rule
    fn program_with_rule(rule: Rule) -> ProgramHandle {
        let mut commit = ProgramCommit::empty(ProgramPipeline::new(), ValidationReport::default());
        commit.add_rule(rule);
        commit.submit().unwrap()
    }

    #[test]
    fn translation_resolves_to_original_rule() {
        // The user's (original) program.
        let original_rule = Rule::parse("P(?x, ?y) :- Q(?y, ?x) .").unwrap();
        let original_string = original_rule.to_string();

        let original = program_with_rule(original_rule);
        let original_id = original.rules().next().unwrap().id();

        // A transformation rewrites the rule, recording its origin.
        let mut rewritten = Rule::parse("P(?x, ?y) :- Q(?y, ?x), R(?y) .").unwrap();
        rewritten.set_origin(Origin::Normalization(original_id));
        let rewritten_string = rewritten.to_string();
        assert_ne!(original_string, rewritten_string);

        let mut commit = original.fork();
        commit.add_rule(rewritten);
        let transformed = commit.submit().unwrap();

        let normalized = NormalizedProgram::normalize_program(&transformed);
        let translation = RuleIdTranslation::new(&transformed, &normalized);

        assert_eq!(normalized.rules().len(), 1);

        // Index translation round-trips between the single normalized and original rule.
        assert_eq!(translation.original_index(0), 0);
        assert_eq!(translation.normalized_index(0), 0);

        // The displayed rule is the original one, not the rewritten (normalized) one.
        assert_eq!(
            translation.original_rule_unchecked(0).to_string(),
            original_string
        );
        assert_ne!(
            translation.original_rule_unchecked(0).to_string(),
            rewritten_string
        );
    }
}
