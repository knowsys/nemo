//! The program pipeline: statement versions, revisions, and provenance.
//!
//! A [Pipeline] keeps every *statement version* in an append-only arena.
//! A *revision* of the program is a list of statement versions, which are *live* in it.
//! A *pipeline stage* ([Transformation]) rewrites one revision into the next through a [Commit].
//! Keeping a statement is the default,
//! so a statement leaves a revision only if a commit removes or replaces it.
//! Rules, facts and imports change only through *declared rewrites*,
//! which say how each new statement relates to the revision it came from.
//!
//! A commit records every change as a *provenance edge*,
//! which leads from the statement versions that the change read to the versions it created.
//! Through the edges, a statement version without a span finds the span of its nearest
//! ancestor, and a diagnostic can [point](Pipeline::refine) at the definitions of the values
//! that a stage substituted.
//!
//! An *extension* is a list of statements added to a loaded program
//! with [Engine::extend](crate::engine::Engine::extend).
//! The stages run over it without rewriting the statements of the loaded program,
//! which are *frozen*.
//!
//! The pipeline also registers the source texts that spans point into.
//! A module instance is registered as a source of its own,
//! so that a span says which instance a statement belongs to.

mod commit;
mod construct;
mod matching;
mod provenance;
mod reports;
mod revision;
mod scope;
mod sources;
mod stage;

pub use self::{
    commit::Commit,
    matching::LiteralPath,
    revision::RevisionId,
    stage::{Stages, Transformation},
};

pub(crate) use self::{
    construct::UnlinkedConstruct,
    matching::{Correspondence, grouped_paths},
    provenance::RewriteDeclaration,
    scope::NameScope,
};

use nemo_physical::diagnostics::Diagnostic;

use crate::{
    model::{Program, Span, Statement},
    pipeline::{provenance::ProvenanceEdge, revision::ProgramRevision, sources::SourceFile},
};

/// The id of one statement version in a pipeline's arena.
///
/// An id stays valid for the pipeline's lifetime.
/// Replaced and removed versions stay in the arena, so that provenance edges can point at them.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct StatementId(u32);

impl StatementId {
    /// Returns the id's index into the arena.
    fn index(self) -> usize {
        self.0 as usize
    }
}

/// The program pipeline.
///
/// It keeps the arena of statement versions, the revisions, the provenance edges,
/// the registered source texts, and the reports.
#[derive(Debug, Default)]
pub struct Pipeline {
    /// The registered source texts, by file id.
    files: Vec<SourceFile>,
    /// Every statement version, by id, including the replaced and removed ones.
    statements: Vec<Statement>,
    /// For each statement version, the index of the edge that created it,
    /// or `None` for a loaded statement.
    created_by: Vec<Option<usize>>,
    /// The provenance edges, by index.
    edges: Vec<ProvenanceEdge>,
    /// The revisions, by id, in the order they were created.
    revisions: Vec<ProgramRevision>,
    /// The reports collected so far: warnings and non-fatal errors.
    reports: Vec<Diagnostic>,
}

impl Pipeline {
    /// Creates an empty pipeline.
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds a program as a new revision without a parent.
    ///
    /// Its statements have no provenance edge, so they are the roots of provenance.
    pub fn load(&mut self, program: Program) -> RevisionId {
        let statements = program
            .statements
            .into_iter()
            .map(|statement| self.intern(statement, None))
            .collect();
        self.push_revision(ProgramRevision {
            parent: None,
            stage: None,
            statements,
        })
    }

    /// Returns the statement version with the given id, even a replaced or removed one.
    pub fn statement(&self, id: StatementId) -> &Statement {
        &self.statements[id.index()]
    }

    /// Returns the live statements of a revision, in program order.
    pub fn statements(
        &self,
        revision: RevisionId,
    ) -> impl Iterator<Item = (StatementId, &Statement)> {
        self.revisions[revision.index()]
            .statements
            .iter()
            .map(|&id| (id, self.statement(id)))
    }

    /// Returns a copy of the program of a revision.
    pub fn program(&self, revision: RevisionId) -> Program {
        Program::new(
            self.statements(revision)
                .map(|(_, statement)| statement.clone())
                .collect(),
        )
    }

    /// Returns a new revision with the live statements of `base`, followed by those of
    /// `extension`.
    ///
    /// The new revision's parent is `base`,
    /// so the stages that `base` has been through count as run for it.
    /// The statements of both revisions keep their provenance.
    pub fn merge(&mut self, base: RevisionId, extension: RevisionId) -> RevisionId {
        let mut statements = self.revisions[base.index()].statements.clone();
        statements.extend_from_slice(&self.revisions[extension.index()].statements);
        self.push_revision(ProgramRevision {
            parent: Some(base),
            stage: None,
            statements,
        })
    }

    /// Starts a commit that rewrites the revision `base` under the stage name `stage`.
    pub fn commit(&mut self, base: RevisionId, stage: &'static str) -> Commit<'_> {
        Commit::new(self, base, stage, 0)
    }

    /// Applies the stages of a list that a revision has not been through yet,
    /// and returns the resulting revision.
    ///
    /// The stages run in list order.
    /// A stage that returns an error aborts the application,
    /// and its unfinished commit creates no revision.
    /// The [reports](Self::reports) collected so far remain.
    ///
    /// # Errors
    ///
    /// Returns the diagnostic of the stage that aborted.
    ///
    /// # Panics
    ///
    /// Panics if a stage runs before a stage that it requires,
    /// or if a stage breaks a rule of [Commit],
    /// such as by writing a construct that a loaded program cannot contain.
    pub fn apply(&mut self, stages: &Stages, base: RevisionId) -> Result<RevisionId, Diagnostic> {
        let mut current = base;
        for stage in stages.entries() {
            if self.ran(current, stage.name()) {
                continue;
            }
            current = self.run(stage.as_ref(), current, 0)?;
        }
        Ok(current)
    }

    /// Applies every stage of a list to a revision whose first `frozen` statements
    /// have been through the stages already,
    /// such as a loaded program merged with an extension.
    ///
    /// Every stage runs, and none may rewrite the frozen statements.
    /// A stage that returns an error aborts the application, as in [apply](Self::apply).
    pub(crate) fn extend(
        &mut self,
        stages: &Stages,
        base: RevisionId,
        frozen: usize,
    ) -> Result<RevisionId, Diagnostic> {
        let mut current = base;
        for stage in stages.entries() {
            current = self.run(stage.as_ref(), current, frozen)?;
        }
        Ok(current)
    }

    /// Runs one stage over a revision, after checking that the stages it requires have run.
    ///
    /// The first `frozen` statements of `base` are frozen in the stage's commit.
    fn run(
        &mut self,
        stage: &dyn Transformation,
        base: RevisionId,
        frozen: usize,
    ) -> Result<RevisionId, Diagnostic> {
        for required in stage.requires() {
            assert!(
                self.ran(base, required),
                "the `{}` stage requires the `{required}` stage before it",
                stage.name()
            );
        }
        let mut commit = Commit::new(self, base, stage.name(), frozen);
        stage.apply(&mut commit)?;
        Ok(commit.finish())
    }

    /// Returns the chain of revisions that leads to a revision,
    /// from its loaded root to the revision itself.
    pub(crate) fn lineage(&self, revision: RevisionId) -> Vec<RevisionId> {
        let mut chain = Vec::new();
        let mut current = Some(revision);
        while let Some(revision) = current {
            chain.push(revision);
            current = self.revisions[revision.index()].parent;
        }
        chain.reverse();
        chain
    }

    /// Returns the stage that produced a revision, or `None` for a loaded or merged revision.
    pub fn stage(&self, revision: RevisionId) -> Option<&'static str> {
        self.revisions[revision.index()].stage
    }

    /// Returns whether a revision or one of its ancestors was produced by the named stage.
    pub(crate) fn ran(&self, revision: RevisionId, stage: &str) -> bool {
        let mut current = Some(revision);
        while let Some(revision) = current {
            let entry = &self.revisions[revision.index()];
            if entry.stage == Some(stage) {
                return true;
            }
            current = entry.parent;
        }
        false
    }

    /// Returns the loaded statements that a statement version derives from,
    /// in depth-first order.
    ///
    /// A loaded statement derives from itself.
    #[cfg(test)]
    pub(crate) fn origins(&self, statement: StatementId) -> Vec<StatementId> {
        let mut pending = vec![statement];
        let mut seen = Vec::new();
        let mut roots = Vec::new();
        while let Some(id) = pending.pop() {
            if seen.contains(&id) {
                continue;
            }
            seen.push(id);
            match self.created_by[id.index()] {
                None => roots.push(id),
                Some(edge) => pending.extend(self.edges[edge].sources.iter().rev()),
            }
        }
        roots
    }

    /// Returns the span of a statement version, or if it has none,
    /// the first span found among the versions it derives from.
    ///
    /// The versions it derives from are searched depth first.
    pub(crate) fn source_span(&self, statement: StatementId) -> Option<Span> {
        let mut pending = vec![statement];
        let mut seen = Vec::new();
        while let Some(id) = pending.pop() {
            if seen.contains(&id) {
                continue;
            }
            seen.push(id);
            if let Some(span) = self.statement(id).span() {
                return Some(span);
            }
            if let Some(edge) = self.created_by[id.index()] {
                pending.extend(self.edges[edge].sources.iter().rev());
            }
        }
        None
    }

    /// Adds a statement version to the arena, and returns its id.
    ///
    /// `edge` is the index of the edge that created it, or `None` for a loaded statement.
    pub(crate) fn intern(&mut self, statement: Statement, edge: Option<usize>) -> StatementId {
        let id = StatementId(
            u32::try_from(self.statements.len()).expect("statement counts stay below u32"),
        );
        self.statements.push(statement);
        self.created_by.push(edge);
        id
    }

    /// Records a provenance edge, and returns its index.
    pub(crate) fn record_edge(&mut self, edge: ProvenanceEdge) -> usize {
        self.edges.push(edge);
        self.edges.len() - 1
    }

    /// Returns the rewrite declaration made about a statement version when it was created,
    /// with the versions it derives from.
    ///
    /// Returns `None` for a loaded statement,
    /// and for a version created without a rewrite declaration.
    pub(crate) fn declaration(
        &self,
        statement: StatementId,
    ) -> Option<(&RewriteDeclaration, &[StatementId])> {
        let edge = &self.edges[self.created_by[statement.index()]?];
        Some((edge.declaration.as_ref()?, &edge.sources))
    }

    /// Returns the index of the edge that created a statement version,
    /// or `None` for a loaded statement.
    pub(crate) fn creating_edge(&self, statement: StatementId) -> Option<usize> {
        self.created_by[statement.index()]
    }

    /// Returns the edge with the given index mutably,
    /// so that its targets and rewrite declaration can be filled in.
    pub(crate) fn edge_mut(&mut self, edge: usize) -> &mut ProvenanceEdge {
        &mut self.edges[edge]
    }

    /// Returns the ids of the live statements of a revision, in program order.
    pub(crate) fn revision_statements(&self, revision: RevisionId) -> &[StatementId] {
        &self.revisions[revision.index()].statements
    }

    /// Appends a revision, and returns its id.
    pub(crate) fn push_revision(&mut self, revision: ProgramRevision) -> RevisionId {
        let id = RevisionId::new(
            u32::try_from(self.revisions.len()).expect("revision counts stay below u32"),
        );
        self.revisions.push(revision);
        id
    }
}

#[cfg(test)]
mod tests;
