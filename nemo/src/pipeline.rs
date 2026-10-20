//! The program pipeline: statement versions, revisions, and provenance.
//!
//! A [Pipeline] owns an append-only arena of statement versions,
//! and a sequence of [revisions](RevisionId), each a list of live statements.
//! Transformations rewrite one revision into the next by calling the methods of a [Commit].
//! *Keeping a statement is the default*:
//! statements leave a revision only by explicit removal,
//! so forgetting one cannot silently delete it.
//! Rules, facts and imports change only through declared rewrites,
//! which say how each new statement relates to the revision it came from,
//! and which the commit records.
//!
//! The [Commit] methods record provenance as they run.
//! Every statement version knows the provenance edge that created it.
//! An edge leads from the versions a change read to the versions it created,
//! any number on either side, and carries the stage's name and what the stage declared.
//! Diagnostics use the edges to find a statement's nearest source span,
//! and [refine](Pipeline::refine) uses them to point at the definitions of substituted values.
//! A new statement without a span takes the span of its first source.
//!
//! Transformations are [named stages](Transformation),
//! applied in the order of a [stage list](Stages),
//! which also takes stages added with [insert_before](Stages::insert_before).
//! [Applying](Pipeline::apply) the list skips the stages a revision has already been through.
//! A stage lists the stages that must run before it.
//! No stage may write a `@use` directive or a module-qualified name,
//! since linking resolves those before the pipeline runs.
//!
//! An *extension* is a list of statements added to a loaded program
//! with [Engine::extend](crate::engine::Engine::extend).
//! The stages run over it without rewriting the statements of the loaded program.
//!
//! The pipeline also registers the source texts spans point into.
//! A module instance is registered as a source of its own, recording the `@use` that made it,
//! so a span says which instance a statement belongs to.

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

/// Identifies one statement version in a pipeline's arena.
///
/// Ids are stable forever: superseded and removed versions stay
/// addressable, which is what lets provenance point at them.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct StatementId(u32);

impl StatementId {
    /// Returns the id's index into the arena.
    fn index(self) -> usize {
        self.0 as usize
    }
}

/// The program pipeline: the arena of statement versions, the revisions listing
/// them, the provenance connecting them, the source texts, and the report sink.
#[derive(Debug, Default)]
pub struct Pipeline {
    /// The source texts spans point into.
    files: Vec<SourceFile>,
    /// Every statement version ever committed.
    statements: Vec<Statement>,
    /// Per statement version, the edge that created it,
    /// and `None` for the loaded originals, the roots of provenance.
    created_by: Vec<Option<usize>>,
    /// The provenance edges.
    edges: Vec<ProvenanceEdge>,
    /// The revisions, in creation order.
    revisions: Vec<ProgramRevision>,
    /// The accumulated reports (warnings and non-fatal errors).
    reports: Vec<Diagnostic>,
}

impl Pipeline {
    /// Creates an empty pipeline.
    pub fn new() -> Self {
        Self::default()
    }

    /// Loads a program as a new revision, whose statements become provenance roots.
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

    /// Returns the statement version behind an id, superseded and removed ones included.
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

    /// Returns the program of a revision, cloned out of the arena.
    pub fn program(&self, revision: RevisionId) -> Program {
        Program::new(
            self.statements(revision)
                .map(|(_, statement)| statement.clone())
                .collect(),
        )
    }

    /// Appends the live statements of one revision to another's, as a new revision of the base.
    ///
    /// This is how an extension joins a loaded program.
    /// Both sides keep their provenance,
    /// and the stages the base has been through count as run for the result.
    pub fn merge(&mut self, base: RevisionId, extension: RevisionId) -> RevisionId {
        let mut statements = self.revisions[base.index()].statements.clone();
        statements.extend_from_slice(&self.revisions[extension.index()].statements);
        self.push_revision(ProgramRevision {
            parent: Some(base),
            stage: None,
            statements,
        })
    }

    /// Starts a commit rewriting a revision under the given stage name.
    pub fn commit(&mut self, base: RevisionId, stage: &'static str) -> Commit<'_> {
        Commit::new(self, base, stage, 0)
    }

    /// Applies every stage of the list that the revision has not been through yet, in list order,
    /// returning the resulting revision.
    ///
    /// A stage returning an error aborts the run, and its partial commit creates no revision.
    /// The accumulated [reports](Self::reports) remain.
    ///
    /// # Errors
    ///
    /// Returns the diagnostic of the stage that aborted.
    ///
    /// # Panics
    ///
    /// Panics if a stage runs before a stage it requires,
    /// or writes a construct a loaded program cannot hold.
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

    /// Applies every stage of the list over a revision whose leading
    /// `frozen` statements went through them already,
    /// such as an extension appended to a loaded program.
    ///
    /// Every stage runs, and none may rewrite the frozen statements.
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

    /// Runs one stage over a revision, checking the stages it requires
    /// first.
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

    /// Returns the revisions leading to one: its loaded root first, the
    /// revision itself last.
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

    /// Returns the stage that produced a revision, or `None` for a loaded one.
    pub fn stage(&self, revision: RevisionId) -> Option<&'static str> {
        self.revisions[revision.index()].stage
    }

    /// Returns whether a revision (or one of its ancestors) was produced by the
    /// named stage.
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

    /// Returns the provenance roots of a statement version: the loaded originals
    /// it derives from (itself, if it is one), in first-seen order.
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

    /// Returns the nearest source span of a statement version: its own, or the
    /// closest one on its provenance ancestry.
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

    /// Interns a statement version into the arena.
    pub(crate) fn intern(&mut self, statement: Statement, edge: Option<usize>) -> StatementId {
        let id = StatementId(
            u32::try_from(self.statements.len()).expect("statement counts stay below u32"),
        );
        self.statements.push(statement);
        self.created_by.push(edge);
        id
    }

    /// Records a provenance edge, returning its index.
    pub(crate) fn record_edge(&mut self, edge: ProvenanceEdge) -> usize {
        self.edges.push(edge);
        self.edges.len() - 1
    }

    /// Returns the rewrite declaration the stage that created a statement version made about it,
    /// with the versions it derives from.
    ///
    /// Returns `None` for a loaded original, and for a version made without a rewrite declaration.
    pub(crate) fn declaration(
        &self,
        statement: StatementId,
    ) -> Option<(&RewriteDeclaration, &[StatementId])> {
        let edge = &self.edges[self.created_by[statement.index()]?];
        Some((edge.declaration.as_ref()?, &edge.sources))
    }

    /// Returns the index of the edge that created a statement version,
    /// or `None` for a loaded original.
    pub(crate) fn creating_edge(&self, statement: StatementId) -> Option<usize> {
        self.created_by[statement.index()]
    }

    /// Returns the edge behind an index, mutably (to fill in targets and
    /// rewrite declarations).
    pub(crate) fn edge_mut(&mut self, edge: usize) -> &mut ProvenanceEdge {
        &mut self.edges[edge]
    }

    /// Returns the live statement list of a revision.
    pub(crate) fn revision_statements(&self, revision: RevisionId) -> &[StatementId] {
        &self.revisions[revision.index()].statements
    }

    /// Appends a revision, returning its id.
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
