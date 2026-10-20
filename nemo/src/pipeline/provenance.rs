//! Provenance edges: how statement versions came to be.

use crate::{
    model::{Rule, Span, Variable},
    pipeline::{StatementId, matching::Correspondence},
};

/// One provenance edge: the statement versions a [Commit](crate::pipeline::Commit) method
/// produced, the versions they derive from, and the stage that did it.
///
/// Edges have any number of versions on either side:
/// a split shares one edge across its results, and a merge records every source.
/// Removals are edges with no targets, so deletion is never silent.
#[derive(Debug)]
pub(crate) struct ProvenanceEdge {
    /// The stage whose commit recorded the edge.
    #[expect(dead_code, reason = "no code displays provenance yet")]
    pub(crate) stage: &'static str,
    /// The statement versions the change derives from.
    pub(crate) sources: Vec<StatementId>,
    /// The statement versions the change produced.
    pub(crate) targets: Vec<StatementId>,
    /// The stage's rewrite declaration about the change.
    pub(crate) declaration: Option<RewriteDeclaration>,
}

/// The rewrite declaration on a provenance edge:
/// what a stage declared about the statements it created.
///
/// Proofs and results are mapped back onto the input program through it,
/// and diagnostics are labeled at the definitions of substituted values.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum RewriteDeclaration {
    /// The created statement replaces the edge's first source, an input rule.
    /// It is a rewritten rule, or an import whose view is the input rule.
    Replaces {
        /// The import's view, or `None` for a rule.
        view: Option<Rule>,
        /// How the input rule reappears in the created statement.
        correspondence: Correspondence,
        /// The occurrences a substitution replaced, if the stage substituted.
        substituted: Vec<SubstitutionRecord>,
    },
    /// The created statement defines a new predicate:
    /// a rule, which is the predicate's definition, or an import, whose view is.
    Defines {
        /// The import's view, or `None` for a rule.
        view: Option<Rule>,
    },
    /// The created rule defines a filter: a new predicate that other statements
    /// use only to restrict their matches, and that proofs never expand.
    DefinesFilter,
}

/// One substituted occurrence: which variable, where it occurred, and where
/// its value was defined.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SubstitutionRecord {
    /// The substituted variable.
    pub(crate) variable: Variable,
    /// The span of the replaced occurrence.
    pub(crate) occurrence: Option<Span>,
    /// The span of the value's definition.
    pub(crate) definition: Option<Span>,
}
