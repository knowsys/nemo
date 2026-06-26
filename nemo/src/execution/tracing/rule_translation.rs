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
/// This relies on the transformations between the original and the normalized
/// program being one-to-one on rules and recording their provenance (each
/// derived rule points back, via its [crate::rule_model::origin::Origin], to the
/// rule it was created from).
///
/// TODO: handling transformations that change the structure of the derivation trees
/// (https://github.com/knowsys/nemo/issues/774)
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

        // Index the rules of the original program to map a rule's
        // [ProgramComponentId] to its position in the original program.
        let mut orig_index_by_id = HashMap::new();
        for (index, rule) in handle.original_revision().rules().enumerate() {
            orig_index_by_id.insert(rule.id(), index);
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

        // The translation assumes that the transformations between the original
        // and the normalized program are one-to-one on rules and record their
        // provenance
        debug_assert!(
            norm_to_orig.iter().all(Option::is_some),
            "tracing rule translation: a normalized rule has no corresponding \
             original rule; a transformation created a rule without recording \
             its origin"
        );
        debug_assert!(
            orig_to_norm.values().all(|indices| indices.len() == 1),
            "tracing rule translation: several normalized rules map to the same \
             original rule; a transformation is not one-to-one on rules"
        );

        Self {
            handle: handle.clone(),
            norm_to_orig_id,
            norm_to_orig,
            orig_to_norm,
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
    /// # Panics
    /// Panics if `normalized` is not a valid normalized rule index, or if it has
    /// no corresponding original rule.
    pub(crate) fn to_original(&self, normalized: usize) -> usize {
        self.norm_to_orig[normalized]
            .expect("every normalized rule maps to an original rule (checked in `new`)")
    }

    /// Translate an original (frontend) rule index into the normalized rule index used internally.
    ///
    /// # Panics
    /// Panics if `original` has no corresponding normalized rule.
    pub(crate) fn to_normalized(&self, original: usize) -> usize {
        self.orig_to_norm
            .get(&original)
            .and_then(|indices| indices.first().copied())
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

    /// Build a [RuleIdTranslation] for a program string
    fn translation_for(program: &str) -> RuleIdTranslation {
        let handle = crate::api::load_program_handle(program.to_string(), String::default())
            .expect("program should parse and validate");
        let normalized = NormalizedProgram::normalize_program(&handle);

        RuleIdTranslation::new(&handle, &normalized)
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
        assert_eq!(translation.to_original(0), 0);
        assert_eq!(translation.to_normalized(0), 0);

        // The displayed rule is the original one, not the rewritten (normalized) one.
        assert_eq!(translation.original_rule(0).to_string(), original_string);
        assert_ne!(translation.original_rule(0).to_string(), rewritten_string);
    }

    #[test]
    #[cfg_attr(miri, ignore)]
    fn translation_global_variable_rule() {
        // The global variable `$t` is substituted by TransformationGlobal.
        let translation = translation_for(
            "@parameter $t = 5 .\ndata(5) .\nresult(?x) :- data(?x), ?x = $t .\n@output result .",
        );

        assert_eq!(translation.to_original(0), 0);
        assert_eq!(translation.to_normalized(0), 0);
        // Resolution recovers the original rule, which still contains `$t`.
        assert!(translation.original_rule(0).to_string().contains("$t"));
    }

    #[test]
    #[cfg_attr(miri, ignore)]
    fn translation_normalized_rule() {
        // The constant `2` in a positive body atom triggers TransformationNormalize.
        let translation =
            translation_for("data(1, 2) .\nresult(?x) :- data(?x, 2) .\n@output result .");

        assert_eq!(translation.to_original(0), 0);
        assert_eq!(translation.to_normalized(0), 0);
    }

    #[test]
    #[cfg_attr(miri, ignore)]
    fn translation_incremental_sparql_rule() {
        // A SPARQL import bound by another body atom is inlined into the rule by
        // TransformationIncremental.
        let translation = translation_for(
            r#"@import remote :- sparql {endpoint = "http://example.org", query = "SELECT ?a ?b WHERE {?a ?b ?c.}"} .
            seed(1) .
            result(?a, ?b) :- seed(?a), remote(?a, ?b) .
            @output result ."#,
        );

        assert_eq!(translation.to_original(0), 0);
        assert_eq!(translation.to_normalized(0), 0);
    }

    #[test]
    #[cfg_attr(miri, ignore)]
    fn translation_merge_sparql_rule() {
        // Two SPARQL imports against the same endpoint in one rule are merged by
        // TransformationMergeSparql (after being inlined by TransformationIncremental).
        let translation = translation_for(
            r#"@import ra :- sparql {endpoint = "http://example.org", query = "SELECT ?a ?b WHERE {?a ?b ?z.}"} .
            @import rb :- sparql {endpoint = "http://example.org", query = "SELECT ?a ?c WHERE {?a ?c ?z.}"} .
            seed(1) .
            result(?a, ?b, ?c) :- seed(?a), ra(?a, ?b), rb(?a, ?c) .
            @output result ."#,
        );

        assert_eq!(translation.to_original(0), 0);
        assert_eq!(translation.to_normalized(0), 0);
    }
}
