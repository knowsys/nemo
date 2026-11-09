//! Revisions: versions of the program, each a list of live statements.

use crate::pipeline::StatementId;

/// The id of one revision of a pipeline's program.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct RevisionId(u32);

impl RevisionId {
    /// Creates an id from its index.
    pub(crate) fn new(index: u32) -> Self {
        Self(index)
    }

    /// Returns the id's index into the revision list.
    pub(crate) fn index(self) -> usize {
        self.0 as usize
    }
}

/// One revision: the program as a list of live statement versions.
#[derive(Debug)]
pub(crate) struct ProgramRevision {
    /// The revision that this one was derived from, or `None` for a loaded revision.
    ///
    /// A merged revision is derived from its base.
    pub(crate) parent: Option<RevisionId>,
    /// The stage that produced it, or `None` for a loaded or merged revision.
    pub(crate) stage: Option<&'static str>,
    /// The live statements, in program order.
    pub(crate) statements: Vec<StatementId>,
}
