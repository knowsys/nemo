//! Provenance edges: how statement versions came to be.

use crate::{
    model::{Rule, Span, Variable},
    pipeline::{StatementId, matching::Correspondence},
};

/// One provenance edge, from the statement versions that a change read
/// to the versions that it produced.
///
/// A [Commit](crate::pipeline::Commit) method records one edge for each change.
/// An edge has any number of versions on either side.
/// For example, a split shares one edge among its results, and a merge records every source.
/// A removal is an edge without targets, so that no statement disappears without a record.
#[derive(Debug)]
pub(crate) struct ProvenanceEdge {
    /// The stage whose commit recorded the edge.
    #[expect(dead_code, reason = "no code displays provenance yet")]
    pub(crate) stage: &'static str,
    /// The statement versions that the change derives from.
    pub(crate) sources: Vec<StatementId>,
    /// The statement versions that the change produced.
    pub(crate) targets: Vec<StatementId>,
    /// The stage's rewrite declaration about the targets, if it made one.
    pub(crate) declaration: Option<RewriteDeclaration>,
}

/// What a stage declared about the statement that a provenance edge created.
///
/// Through the declarations, proofs and results are mapped back onto the input program,
/// and diagnostics are labeled at the definitions of substituted values.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum RewriteDeclaration {
    /// The created statement replaces the edge's first source, an input rule.
    ///
    /// It is a rewritten rule, or an import whose view is the input rule.
    Replaces {
        /// The view of the created import, or `None` if the created statement is a rule.
        view: Option<Rule>,
        /// How the input rule reappears in the created statement.
        correspondence: Correspondence,
        /// The occurrences that a substitution replaced, if the stage substituted.
        substituted: Vec<SubstitutionRecord>,
    },
    /// The created statement defines a new predicate.
    ///
    /// It is a rule, which is the predicate's definition,
    /// or an import, whose view is the definition.
    Defines {
        /// The view of the created import, or `None` if the created statement is a rule.
        view: Option<Rule>,
    },
    /// The created rule defines a filter.
    ///
    /// A filter is a new predicate that other statements use only to restrict their matches,
    /// and that proofs never expand.
    DefinesFilter,
}

/// One occurrence of a variable that a substitution replaced.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SubstitutionRecord {
    /// The substituted variable.
    pub(crate) variable: Variable,
    /// The span of the replaced occurrence.
    pub(crate) occurrence: Option<Span>,
    /// The span of the value that replaced the variable, which is where the value is defined.
    pub(crate) definition: Option<Span>,
}
