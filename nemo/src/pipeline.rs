//! The program pipeline: statement versions, revisions, and provenance.
//!
//! A [Pipeline] keeps the *statement versions* of its programs in an arena.
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

mod arena;
mod commit;
mod construct;
mod matching;
mod provenance;
mod reports;
mod revision;
mod rewrites;
mod scope;
mod sources;
mod stage;

pub use self::{
    commit::Commit,
    matching::LiteralPath,
    revision::{RevisionId, RevisionText},
    sources::RegisteredSource,
    stage::{Stages, Transformation},
};

pub(crate) use self::{
    arena::ArenaMark,
    construct::UnlinkedConstruct,
    matching::{Correspondence, grouped_paths},
    provenance::RewriteDeclaration,
    reports::ReportMark,
    rewrites::{Definition, DefinitionKind, Rewrites},
    scope::NameScope,
};

use std::collections::HashSet;

use nemo_physical::diagnostics::Diagnostic;

use crate::{
    model::{Program, Span, Statement},
    pipeline::{
        arena::Arena, provenance::ProvenanceEdge, revision::ProgramRevision, sources::Sources,
    },
};

/// The id of one statement version in a pipeline's arena.
///
/// Replaced and removed versions stay in the arena, so that provenance edges can point at them.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct StatementId(u32);

impl StatementId {
    /// Returns the id as a number.
    fn index(self) -> usize {
        self.0 as usize
    }
}

/// The program pipeline.
///
/// It has three parts: the registered source texts,
/// the arena of statement versions, provenance edges and revisions, and the reports.
#[derive(Debug, Default)]
pub struct Pipeline {
    /// The registered source texts.
    sources: Sources,
    /// The statement versions, the provenance edges and the revisions.
    arena: Arena,
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
    /// The file ids of their spans are reserved,
    /// so that [add_source](Self::add_source) gives none of them to a source.
    pub fn load(&mut self, program: Program) -> RevisionId {
        self.reserve_files(&program);
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
        self.arena.statement(id)
    }

    /// Returns the live statements of a revision, in program order.
    pub fn statements(
        &self,
        revision: RevisionId,
    ) -> impl Iterator<Item = (StatementId, &Statement)> {
        self.arena
            .revision(revision)
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
        let mut statements = self.arena.revision(base).statements.clone();
        statements.extend_from_slice(&self.arena.revision(extension).statements);

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
        self.apply_imported(stages, base, &[])
    }

    /// Applies stages as [apply](Self::apply) does,
    /// telling them which predicates have rows given through
    /// [import_rows](crate::engine::Engine::import_rows) (see [Commit::with_imported]).
    pub(crate) fn apply_imported(
        &mut self,
        stages: &Stages,
        base: RevisionId,
        imported: &[String],
    ) -> Result<RevisionId, Diagnostic> {
        let mut current = base;
        for stage in stages.entries() {
            if self.ran(current, stage.name()) {
                continue;
            }

            current = self.run(stage.as_ref(), current, 0, imported)?;
        }

        Ok(current)
    }

    /// Applies every stage of a list to a revision whose first `frozen` statements
    /// have been through the stages already,
    /// such as a loaded program merged with an extension.
    ///
    /// Every stage runs, and none may rewrite the frozen statements.
    /// A stage that returns an error aborts the application, as in [apply](Self::apply).
    /// `imported` is as for [apply_imported](Self::apply_imported).
    pub(crate) fn extend(
        &mut self,
        stages: &Stages,
        base: RevisionId,
        frozen: usize,
        imported: &[String],
    ) -> Result<RevisionId, Diagnostic> {
        let mut current = base;
        for stage in stages.entries() {
            current = self.run(stage.as_ref(), current, frozen, imported)?;
        }

        Ok(current)
    }

    /// Runs one stage over a revision, after checking that the stages it requires have run.
    ///
    /// The first `frozen` statements of `base` are frozen in the stage's commit,
    /// and `imported` names the predicates with rows given through
    /// [import_rows](crate::engine::Engine::import_rows).
    fn run(
        &mut self,
        stage: &dyn Transformation,
        base: RevisionId,
        frozen: usize,
        imported: &[String],
    ) -> Result<RevisionId, Diagnostic> {
        for required in stage.requires() {
            assert!(
                self.ran(base, required),
                "the `{}` stage requires the `{required}` stage before it",
                stage.name()
            );
        }

        let mut commit = Commit::new(self, base, stage.name(), frozen).with_imported(imported);
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
            current = self.arena.revision(revision).parent;
        }

        chain.reverse();
        chain
    }

    /// Returns the stage that produced a revision, or `None` for a loaded or merged revision.
    pub fn stage(&self, revision: RevisionId) -> Option<&'static str> {
        self.arena.revision(revision).stage
    }

    /// Returns whether a revision or one of its ancestors was produced by the named stage.
    pub(crate) fn ran(&self, revision: RevisionId, stage: &str) -> bool {
        let mut current = Some(revision);
        while let Some(revision) = current {
            let entry = self.arena.revision(revision);
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
        self.ancestry(statement)
            .filter(|&id| self.arena.created_by(id).is_none())
            .collect()
    }

    /// Returns the span of a statement version, or if it has none,
    /// the first span found among the versions it derives from.
    ///
    /// The versions it derives from are searched depth first.
    pub(crate) fn source_span(&self, statement: StatementId) -> Option<Span> {
        self.ancestry(statement)
            .find_map(|id| self.statement(id).span())
    }

    /// Returns a walk over a statement version and the versions that it derives from,
    /// depth first, each of them once.
    pub(crate) fn ancestry(&self, statement: StatementId) -> Ancestry<'_> {
        Ancestry {
            pipeline: self,
            pending: vec![statement],
            seen: HashSet::new(),
        }
    }

    /// Returns the number of statement versions that the arena has.
    #[cfg(test)]
    pub(crate) fn statement_count(&self) -> usize {
        self.arena.statement_count()
    }

    /// Returns the position of the arena now:
    /// what is added from now on comes after it, and everything else before it.
    pub(crate) fn mark(&self) -> ArenaMark {
        self.arena.mark()
    }

    /// Removes every statement version, provenance edge and revision added since `mark` was
    /// taken, so that an abandoned program leaves nothing behind.
    ///
    /// The statement and revision ids given out since then are invalid afterwards.
    pub(crate) fn rewind(&mut self, mark: ArenaMark) {
        self.arena.rewind(mark);
    }

    /// Removes every statement version, provenance edge and revision added before `mark` was
    /// taken, so that a replaced program leaves nothing behind.
    ///
    /// The ids given out before then are invalid afterwards, and the later ones stay valid.
    /// What was added since must not derive from what is removed,
    /// which holds for a program loaded since and what the stages made of it.
    pub(crate) fn forget_before(&mut self, mark: ArenaMark) {
        self.arena.forget_before(mark);
    }

    /// Adds a statement version to the arena, and returns its id.
    ///
    /// `edge` is the index of the edge that created it, or `None` for a loaded statement.
    pub(crate) fn intern(&mut self, statement: Statement, edge: Option<usize>) -> StatementId {
        self.arena.intern(statement, edge)
    }

    /// Records a provenance edge, and returns its index.
    pub(crate) fn record_edge(&mut self, edge: ProvenanceEdge) -> usize {
        self.arena.record_edge(edge)
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
        let edge = self.arena.edge(self.arena.created_by(statement)?);
        Some((edge.declaration.as_ref()?, &edge.sources))
    }

    /// Returns the index of the edge that created a statement version,
    /// or `None` for a loaded statement.
    pub(crate) fn creating_edge(&self, statement: StatementId) -> Option<usize> {
        self.arena.created_by(statement)
    }

    /// Returns the edge with the given index mutably,
    /// so that its targets and rewrite declaration can be filled in.
    pub(crate) fn edge_mut(&mut self, edge: usize) -> &mut ProvenanceEdge {
        self.arena.edge_mut(edge)
    }

    /// Returns the ids of the live statements of a revision, in program order.
    pub(crate) fn revision_statements(&self, revision: RevisionId) -> &[StatementId] {
        &self.arena.revision(revision).statements
    }

    /// Appends a revision, and returns its id.
    pub(crate) fn push_revision(&mut self, revision: ProgramRevision) -> RevisionId {
        self.arena.push_revision(revision)
    }
}

/// A walk over a statement version and the versions that it derives from,
/// along the provenance edges.
#[derive(Debug)]
pub(crate) struct Ancestry<'a> {
    /// The pipeline whose edges are followed.
    pipeline: &'a Pipeline,
    /// The versions still to visit, the next one last.
    pending: Vec<StatementId>,
    /// The versions visited so far.
    seen: HashSet<StatementId>,
}

impl Iterator for Ancestry<'_> {
    type Item = StatementId;

    fn next(&mut self) -> Option<StatementId> {
        while let Some(id) = self.pending.pop() {
            if !self.seen.insert(id) {
                continue;
            }

            if let Some(edge) = self.pipeline.arena.created_by(id) {
                self.pending
                    .extend(self.pipeline.arena.edge(edge).sources.iter().rev());
            }

            return Some(id);
        }

        None
    }
}

#[cfg(test)]
mod tests;
