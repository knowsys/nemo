//! This module defines [tracing_resolve_origin].

use crate::rule_model::{
    components::{ComponentSource, rule::Rule},
    origin::Origin,
    pipeline::id::ProgramComponentId,
    programs::handle::ProgramHandle,
};

/// Given a [ProgramComponentId] associated with a program pipeline (of the given [ProgramHandle])
/// return the [Rule] that should be displayed.
///
/// If rules have been created by transformations from other rules,
/// this function will try to find, as best as possible,
/// the original rule that should be displayed.
pub fn tracing_resolve_origin(handle: &ProgramHandle, id: ProgramComponentId) -> Rule {
    let rule = handle.rule_by_id(id).expect("id must point to a rule");

    match rule.origin() {
        Origin::Created
        | Origin::File { .. }
        | Origin::Reference(_)
        | Origin::Component(_)
        | Origin::Extern
        | Origin::Substitution { .. } => rule.clone(),
        Origin::Normalization(id) => tracing_resolve_origin(handle, id),
    }
}
