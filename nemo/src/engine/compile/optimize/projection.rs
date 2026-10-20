//! Projection pushdown: the import columns no rule reads, dropped at
//! load.

use std::collections::HashMap;

use nemo_physical::diagnostics::Diagnostic;

use crate::{
    engine::{
        compile::import_plan::ImportPlan,
        compile::optimize::{
            ImportPass, ImportSurvey,
            survey::{counts_matches, occurrences},
        },
        compile::translation::Translator,
    },
    model::{Term, TermKind, VariableKind},
};

/// Finds the import columns no rule reads.
///
/// The pass applies to a predicate that only rules read
/// (not a fact, rule head, export, or `@input`),
/// and finds the positions holding, in every body occurrence,
/// an anonymous term or a universal variable the rule mentions nowhere else.
/// A rule with `#count()` reads every position, anonymous ones included,
/// since it counts the body matches.
/// Rows differing only in such columns collapse into one stored row, which no rule can observe,
/// so the loader skips such columns before they are parsed, interned, or stored.
/// A predicate whose rules read none of its columns is stored whole,
/// so that its atoms can still test whether a row exists.
#[derive(Debug)]
pub(crate) struct ProjectionPushdown;

impl ImportPass for ProjectionPushdown {
    fn name(&self) -> &'static str {
        "projection"
    }

    fn run(
        &self,
        survey: &ImportSurvey,
        translator: &Translator<'_>,
        plan: &mut ImportPlan,
    ) -> Result<(), Diagnostic> {
        // Per candidate, whether each position was read anywhere. A
        // width disagreement disqualifies the predicate (the arity
        // diagnostics report it).
        let mut read: HashMap<String, Option<Vec<bool>>> = HashMap::new();
        for rule in &survey.rules {
            let counts = occurrences(rule);
            let counted = counts_matches(rule);
            for literal in &rule.body {
                let Some(atom) = literal.atom() else {
                    continue;
                };
                let name = translator.resolve(&atom.predicate, atom.span)?;
                if !survey.optimizable(&name) {
                    continue;
                }
                let entry = read
                    .entry(name)
                    .or_insert_with(|| Some(vec![false; atom.terms.len()]));
                let Some(flags) = entry else { continue };
                if flags.len() != atom.terms.len() {
                    *entry = None;
                    continue;
                }
                for (flag, term) in flags.iter_mut().zip(&atom.terms) {
                    *flag |= !unread(term, &counts, counted);
                }
            }
        }
        // For an import with a `where` clause, the columns the clause's
        // atoms bind are the keys its fetch sends, and the columns its
        // conditions test are read too, whatever the rules read.
        for (name, bound) in &survey.bound {
            if let Some(Some(flags)) = read.get_mut(name)
                && flags.len() == bound.len()
            {
                for (flag, &bound) in flags.iter_mut().zip(bound) {
                    *flag |= bound;
                }
            }
        }
        for (name, flags) in read {
            let Some(flags) = flags else { continue };
            // An import whose conditions the survey could not place among
            // the kept columns has its predicate stored whole.
            if survey.conditioned.contains(&name) {
                continue;
            }
            // Rules that read no column still ask whether a row exists,
            // and a join reads columns, so such an import is stored whole.
            if !flags.contains(&true) {
                continue;
            }
            let pruned: Vec<bool> = flags.iter().map(|&read| !read).collect();
            if pruned.contains(&true) {
                plan.prune(name, pruned);
            }
        }
        Ok(())
    }
}

/// Returns whether a term at an atom position reads nothing: in a rule without
/// `#count()`, anonymous, or a universal variable occurring nowhere
/// else in the rule.
fn unread(term: &Term, counts: &HashMap<String, usize>, counted: bool) -> bool {
    !counted
        && match &term.kind {
            TermKind::Variable(variable) => match variable.kind {
                VariableKind::Anonymous => true,
                VariableKind::Universal => counts.get(&variable.name) == Some(&1),
                _ => false,
            },
            _ => false,
        }
}
