//! The arena of a pipeline: its statement versions, its provenance edges and its revisions.

use crate::{
    model::Statement,
    pipeline::{RevisionId, StatementId, provenance::ProvenanceEdge, revision::ProgramRevision},
};

/// The statement versions, provenance edges and revisions of a pipeline.
///
/// Every entry has an id, which is its number among all the entries of its kind that the arena
/// ever got. An id is given once.
/// A statement version and a revision refer only to entries that were added before them.
///
/// The arena loses entries at its ends only:
/// [rewind](Self::rewind) removes the newest, and [forget_before](Self::forget_before) the
/// oldest. The ids of the entries that stay are as they were.
#[derive(Debug, Default)]
pub(super) struct Arena {
    /// The ids of the oldest entries that the arena still has.
    first: ArenaMark,
    /// The statement versions, including the replaced and removed ones.
    statements: Vec<Statement>,
    /// For each statement version, the id of the edge that created it,
    /// or `None` for a loaded statement.
    created_by: Vec<Option<usize>>,
    /// The provenance edges.
    edges: Vec<ProvenanceEdge>,
    /// The revisions, in the order they were created.
    revisions: Vec<ProgramRevision>,
}

/// A position in a pipeline's arena: the ids that its next entries get.
///
/// The entries with smaller ids were added before the mark was taken,
/// and the others after it.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ArenaMark {
    /// The id of the next statement version.
    statements: usize,
    /// The id of the next provenance edge.
    edges: usize,
    /// The id of the next revision.
    revisions: usize,
}

impl Arena {
    /// Returns the statement version with the given id.
    pub(super) fn statement(&self, id: StatementId) -> &Statement {
        &self.statements[id.index() - self.first.statements]
    }

    /// Returns the id of the edge that created a statement version,
    /// or `None` for a loaded statement.
    pub(super) fn created_by(&self, id: StatementId) -> Option<usize> {
        self.created_by[id.index() - self.first.statements]
    }

    /// Returns the provenance edge with the given id.
    pub(super) fn edge(&self, id: usize) -> &ProvenanceEdge {
        &self.edges[id - self.first.edges]
    }

    /// Returns the provenance edge with the given id mutably.
    pub(super) fn edge_mut(&mut self, id: usize) -> &mut ProvenanceEdge {
        &mut self.edges[id - self.first.edges]
    }

    /// Returns the revision with the given id.
    pub(super) fn revision(&self, id: RevisionId) -> &ProgramRevision {
        &self.revisions[id.index() - self.first.revisions]
    }

    /// Adds a statement version, and returns its id.
    ///
    /// `edge` is the id of the edge that created it, or `None` for a loaded statement.
    pub(super) fn intern(&mut self, statement: Statement, edge: Option<usize>) -> StatementId {
        let id = self.first.statements + self.statements.len();
        self.statements.push(statement);
        self.created_by.push(edge);

        StatementId(u32::try_from(id).expect("statement counts stay below u32"))
    }

    /// Adds a provenance edge, and returns its id.
    pub(super) fn record_edge(&mut self, edge: ProvenanceEdge) -> usize {
        self.edges.push(edge);
        self.first.edges + self.edges.len() - 1
    }

    /// Adds a revision, and returns its id.
    pub(super) fn push_revision(&mut self, revision: ProgramRevision) -> RevisionId {
        let id = self.first.revisions + self.revisions.len();
        self.revisions.push(revision);

        RevisionId::new(u32::try_from(id).expect("revision counts stay below u32"))
    }

    /// Returns every statement version that the arena has.
    pub(super) fn statements(&self) -> impl Iterator<Item = &Statement> {
        self.statements.iter()
    }

    /// Returns the number of statement versions that the arena has.
    #[cfg(test)]
    pub(super) fn statement_count(&self) -> usize {
        self.statements.len()
    }

    /// Returns the arena's position now.
    pub(super) fn mark(&self) -> ArenaMark {
        ArenaMark {
            statements: self.first.statements + self.statements.len(),
            edges: self.first.edges + self.edges.len(),
            revisions: self.first.revisions + self.revisions.len(),
        }
    }

    /// Removes every statement version, edge and revision added since `mark` was taken.
    ///
    /// Their ids are given again to the entries added next.
    pub(super) fn rewind(&mut self, mark: ArenaMark) {
        self.statements
            .truncate(mark.statements - self.first.statements);
        self.created_by
            .truncate(mark.statements - self.first.statements);
        self.edges.truncate(mark.edges - self.first.edges);
        self.revisions
            .truncate(mark.revisions - self.first.revisions);
    }

    /// Removes every statement version, edge and revision added before `mark` was taken.
    ///
    /// The entries that stay keep their ids, and must not refer to a removed one.
    pub(super) fn forget_before(&mut self, mark: ArenaMark) {
        self.statements
            .drain(..mark.statements - self.first.statements);
        self.created_by
            .drain(..mark.statements - self.first.statements);
        self.edges.drain(..mark.edges - self.first.edges);
        self.revisions
            .drain(..mark.revisions - self.first.revisions);
        self.first = mark;
    }
}
