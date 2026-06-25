//! This module defines [RuleIdTranslation], which maps rule identifiers
//! between the normalized program and the original program.

use std::collections::HashMap;

use crate::{
    execution::planning::normalization::program::NormalizedProgram,
    rule_model::{
        components::{ComponentIdentity, rule::Rule},
        pipeline::id::ProgramComponentId,
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
/// This is the single entry point for resolving normalized rules back to the
/// original rules they should be displayed as (both their index and their content);
/// the tracing modules should not resolve rule origins by other means.
///
/// If no original revision is recorded for the program, the index translation behaves
/// as the identity, while [RuleIdTranslation::original_rule] still resolves the rule
/// content through the chain of transformations.
///
/// TODO: the current implementation assumes that transformations are essentially
/// one-to-one on rules. Handling rules that are split into (or merged from) several
/// rules is left for when proper tree rewriting is introduced.
#[derive(Debug, Clone)]
pub(crate) struct RuleIdTranslation {
    /// The (transformed) program handle the normalized program was derived from,
    /// used to resolve the original rule that should be displayed.
    handle: ProgramHandle,

    /// For each normalized rule index, the [ProgramComponentId] of the original
    /// rule it should be displayed as.
    norm_to_orig_id: Vec<ProgramComponentId>,
    /// For each normalized rule index, the corresponding original rule index (if any).
    norm_to_orig: Vec<Option<usize>>,
    /// For each original rule index, the corresponding normalized rule indices.
    orig_to_norm: HashMap<usize, Vec<usize>>,
    /// For each original rule index, the [ProgramComponentId] of that rule.
    orig_rule_ids: Vec<ProgramComponentId>,
}

impl RuleIdTranslation {
    /// Build the translation for a given (transformed) [ProgramHandle]
    /// and the [NormalizedProgram] derived from it.
    pub(crate) fn new(handle: &ProgramHandle, normalized: &NormalizedProgram) -> Self {
        // Resolve, for each normalized rule, the original rule it should be displayed as.
        let norm_to_orig_id = normalized
            .rules()
            .iter()
            .map(|rule| tracing_resolve_origin_id(handle, rule.id()))
            .collect::<Vec<_>>();

        // If an original revision is recorded, index its rules to map between
        // a rule's [ProgramComponentId] and its position in the original program.
        let mut orig_rule_ids = Vec::new();
        let mut orig_index_by_id = HashMap::new();
        if let Some(original) = handle.original_revision() {
            for (index, rule) in original.rules().enumerate() {
                orig_index_by_id.insert(rule.id(), index);
                orig_rule_ids.push(rule.id());
            }
        }

        let mut norm_to_orig = Vec::with_capacity(norm_to_orig_id.len());
        let mut orig_to_norm: HashMap<usize, Vec<usize>> = HashMap::new();

        for (norm_index, origin_id) in norm_to_orig_id.iter().enumerate() {
            let orig_index = orig_index_by_id.get(origin_id).copied();

            norm_to_orig.push(orig_index);

            if let Some(orig_index) = orig_index {
                orig_to_norm.entry(orig_index).or_default().push(norm_index);
            }
        }

        Self {
            handle: handle.clone(),
            norm_to_orig_id,
            norm_to_orig,
            orig_to_norm,
            orig_rule_ids,
        }
    }

    /// Return the original [Rule] that the normalized rule with the given index
    /// should be displayed as.
    ///
    /// This walks the chain of transformations back to the rule the user wrote,
    /// as far as it can be resolved.
    ///
    /// # Panics
    /// Panics if `normalized` is not a valid normalized rule index.
    pub(crate) fn original_rule(&self, normalized: usize) -> Rule {
        let origin_id = self.norm_to_orig_id[normalized];

        self.handle
            .rule_by_id(origin_id)
            .expect("resolved id must point to a rule")
            .clone()
    }

    /// Translate a normalized rule index into the original rule index used on the frontend.
    ///
    /// Falls back to the identity if no mapping is available.
    pub(crate) fn to_original(&self, normalized: usize) -> usize {
        self.norm_to_orig
            .get(normalized)
            .copied()
            .flatten()
            .unwrap_or(normalized)
    }

    /// Translate an original (frontend) rule index into the normalized rule index used internally.
    ///
    /// Under the assumption that transformations are essentially one-to-one,
    /// the first matching normalized rule is returned. Falls back to the identity
    /// if no mapping is available.
    pub(crate) fn to_normalized(&self, original: usize) -> usize {
        self.orig_to_norm
            .get(&original)
            .and_then(|indices| indices.first().copied())
            .unwrap_or(original)
    }

    /// Return the [ProgramComponentId] of the original rule with the given (frontend) index,
    /// if it exists.
    pub(crate) fn original_rule_id(&self, original: usize) -> Option<ProgramComponentId> {
        self.orig_rule_ids.get(original).copied()
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

    /// Submit a single rule as a fresh program and return the resulting handle.
    fn program_with_rule(rule: Rule) -> ProgramHandle {
        let mut commit = ProgramCommit::empty(ProgramPipeline::new(), ValidationReport::default());
        commit.add_rule(rule);
        commit.submit().unwrap()
    }

    #[test]
    fn mark_as_original_round_trip() {
        let original = program_with_rule(Rule::parse("P(?x, ?y) :- Q(?y, ?x) .").unwrap());

        // Without marking, there is no original revision.
        assert!(original.original_revision().is_none());

        original.mark_as_original();

        // A revision later derived from the same pipeline can recover the original.
        let derived = original.fork().submit().unwrap();
        let recovered = derived
            .original_revision()
            .expect("original revision was marked");

        assert_eq!(
            recovered.rules().next().unwrap().id(),
            original.rules().next().unwrap().id()
        );
    }

    #[test]
    fn translation_resolves_to_original_rule() {
        // The user's (original) program.
        let original_rule = Rule::parse("P(?x, ?y) :- Q(?y, ?x) .").unwrap();
        let original_string = original_rule.to_string();

        let original = program_with_rule(original_rule);
        original.mark_as_original();
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
        assert_eq!(translation.to_original(0), 0);
        assert_eq!(translation.to_normalized(0), 0);
        assert_eq!(translation.original_rule_id(0), Some(original_id));

        // The displayed rule is the original one, not the rewritten (normalized) one.
        assert_eq!(translation.original_rule(0).to_string(), original_string);
        assert_ne!(translation.original_rule(0).to_string(), rewritten_string);
    }
}
