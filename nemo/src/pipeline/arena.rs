//! The arena of a pipeline: its statement versions, its provenance edges and its revisions.

use crate::{
    model::Statement,
    pipeline::{provenance::ProvenanceEdge, revision::ProgramRevision},
};

/// The statement versions, provenance edges and revisions of a pipeline.
///
/// Each of the three lists only grows, so an id into one of them stays valid.
/// A statement version and a revision refer only to entries that were added before them.
#[derive(Debug, Default)]
pub(super) struct Arena {
    /// Every statement version, by id, including the replaced and removed ones.
    pub(super) statements: Vec<Statement>,
    /// For each statement version, the index of the edge that created it,
    /// or `None` for a loaded statement.
    pub(super) created_by: Vec<Option<usize>>,
    /// The provenance edges, by index.
    pub(super) edges: Vec<ProvenanceEdge>,
    /// The revisions, by id, in the order they were created.
    pub(super) revisions: Vec<ProgramRevision>,
}

/// The sizes of a pipeline's arena at one moment.
///
/// [Pipeline::rewind](crate::pipeline::Pipeline::rewind) removes what was added since.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ArenaMark {
    /// The number of statement versions.
    statements: usize,
    /// The number of provenance edges.
    edges: usize,
    /// The number of revisions.
    revisions: usize,
}

impl Arena {
    /// Returns the arena's sizes now.
    pub(super) fn mark(&self) -> ArenaMark {
        ArenaMark {
            statements: self.statements.len(),
            edges: self.edges.len(),
            revisions: self.revisions.len(),
        }
    }

    /// Removes every statement version, edge and revision added since `mark` was taken.
    ///
    /// The ids given out since then are invalid afterwards.
    pub(super) fn rewind(&mut self, mark: ArenaMark) {
        self.statements.truncate(mark.statements);
        self.created_by.truncate(mark.statements);
        self.edges.truncate(mark.edges);
        self.revisions.truncate(mark.revisions);
    }
}
