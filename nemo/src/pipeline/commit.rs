//! Commits: rewriting one revision into the next.
//!
//! A commit rewrites its *input*, the base revision it starts from, under the name of one stage.
//! Statements that produce no facts, such as `@declare` directives, outputs and exports,
//! change freely.
//! Rules, facts and imports change only through *declared rewrites*.
//! Each statement that a declared rewrite creates comes with a *rewrite declaration*,
//! which says how the statement relates to the input.
//! The statement does one of the following:
//!
//! - It replaces an input rule.
//! - It defines a new predicate.
//! - It defines a *filter*: a new predicate that other statements use
//!   only to restrict their matches, and that proofs never expand.
//!
//! A rule that replaces an input rule comes with the [path](crate::pipeline::LiteralPath)
//! of each literal of the input rule in it.
//! When the commit [finishes](Commit::finish),
//! it computes the *correspondence* of the two rules from these paths.
//! The shorthands, such as [fold](Commit::fold), make the rewrite declarations
//! of common rewrites, paths included.

mod declare;
mod shorthand;
#[cfg(test)]
mod tests;

use std::{cell::OnceCell, collections::HashSet};

use nemo_physical::diagnostics::Diagnostic;

use crate::{
    model::{Directive, Identifier, IdentifierRole, Span, Statement},
    pipeline::{
        NameScope, Pipeline, RevisionId, StatementId, UnlinkedConstruct,
        provenance::ProvenanceEdge, revision::ProgramRevision,
    },
};

use self::declare::PendingDeclaration;

/// A commit in progress, which rewrites a base revision under the name of one stage.
///
/// Keeping a statement is the default.
/// The working list starts as the list of the base revision,
/// and a statement leaves it only through a method that replaces or [removes](Self::remove) it.
/// Every method records its provenance edge as it runs.
/// A new statement without a span gets the span of its first source.
/// Dropping a commit without calling [finish](Self::finish) creates no revision.
/// The statement versions that its methods created then stay in the arena, unreachable.
///
/// A commit over an extension freezes the statements of the program that the extension is
/// appended to.
/// Those statements were rewritten when that program was loaded,
/// so the stage may read them but not rewrite them.
///
/// # Example
///
/// A stage that splits long rule bodies, added to an engine:
///
/// ```
/// use nemo::{
///     engine::Engine,
///     model::{FileId, Literal, Program, Statement},
///     pipeline::{Commit, Transformation},
/// };
/// use nemo_physical::diagnostics::Diagnostic;
///
/// /// Splits long rule bodies: the first two atoms become a rule of their own.
/// struct SplitBodies;
///
/// impl Transformation for SplitBodies {
///     fn name(&self) -> &'static str {
///         "split-bodies"
///     }
///
///     fn apply(&self, commit: &mut Commit<'_>) -> Result<(), Diagnostic> {
///         for id in commit.ids() {
///             let atoms: Vec<usize> = match commit.statement(id) {
///                 Statement::Rule(rule) if rule.body.len() > 2 => rule
///                     .body
///                     .iter()
///                     .enumerate()
///                     .filter(|(_, literal)| matches!(literal, Literal::Positive(_)))
///                     .map(|(position, _)| position)
///                     .take(2)
///                     .collect(),
///                 _ => continue,
///             };
///             if atoms.len() == 2 {
///                 let name = commit.fresh("part");
///                 commit.fold(id, &atoms, name);
///             }
///         }
///         Ok(())
///     }
/// }
///
/// let mut engine = Engine::new();
/// engine.stages_mut().push(SplitBodies);
/// let (program, _) = Program::parse(
///     "e(1, 2) . e(2, 3) . f(3) . p(?x) :- e(?x, ?y), e(?y, ?z), f(?z) .",
///     FileId::new(0),
/// );
/// engine.load(program).expect("the program loads");
/// engine.reason().expect("reasoning reaches the fixpoint");
/// assert_eq!(engine.rows("p").expect("p is a predicate").len(), 1);
/// ```
#[derive(Debug)]
pub struct Commit<'a> {
    /// The pipeline that the commit writes into.
    pipeline: &'a mut Pipeline,
    /// The revision being rewritten.
    base: RevisionId,
    /// The name of the stage that makes the commit.
    stage: &'static str,
    /// The working list of live statements.
    statements: Vec<StatementId>,
    /// The statements that the stage may read but not rewrite.
    frozen: HashSet<StatementId>,
    /// The statement versions that this commit created.
    created: Vec<StatementId>,
    /// The rewrite declarations made so far,
    /// which are recorded on the provenance edges when the commit finishes.
    pending: Vec<PendingDeclaration>,
    /// The name scope of the base revision, built on first use.
    scope: OnceCell<NameScope>,
    /// The resolved names of the predicates that the input uses,
    /// with the predicates of [imported](Self::imported), collected on first use.
    input_names: OnceCell<HashSet<String>>,
    /// The resolved names of the predicates in use,
    /// with the predicates of [imported](Self::imported)
    /// and those drawn by [fresh](Self::fresh), collected on first use.
    taken: Option<HashSet<String>>,
    /// The imports removed with [remove_import](Self::remove_import),
    /// each with the resolved name of its predicate.
    ///
    /// When the commit finishes, no statement other than a `@declare` directive may name these
    /// predicates.
    removed_imports: Vec<(StatementId, String)>,
    /// The resolved names of the predicates with rows given through
    /// [import_rows](crate::engine::Engine::import_rows), which no statement shows.
    imported: &'a [String],
}

impl<'a> Commit<'a> {
    /// Starts a commit over a base revision whose first `frozen` statements
    /// the stage may not rewrite.
    pub(crate) fn new(
        pipeline: &'a mut Pipeline,
        base: RevisionId,
        stage: &'static str,
        frozen: usize,
    ) -> Self {
        let statements = pipeline.revision_statements(base).to_vec();
        let frozen = statements.iter().take(frozen).copied().collect();
        Self {
            pipeline,
            base,
            stage,
            statements,
            frozen,
            created: Vec::new(),
            pending: Vec::new(),
            scope: OnceCell::new(),
            input_names: OnceCell::new(),
            taken: None,
            removed_imports: Vec::new(),
            imported: &[],
        }
    }

    /// Returns the commit, telling the stage which predicates have rows given through
    /// [import_rows](crate::engine::Engine::import_rows), by resolved name.
    pub(crate) fn with_imported(mut self, imported: &'a [String]) -> Self {
        self.imported = imported;
        self
    }

    /// Returns the resolved names of the predicates with rows given through
    /// [import_rows](crate::engine::Engine::import_rows).
    ///
    /// No statement shows these rows, so the engine's own stages leave their predicates alone.
    #[cfg(feature = "sparql")]
    pub(crate) fn imported_predicates(&self) -> &[String] {
        self.imported
    }

    /// Returns the live statements that the stage may rewrite, in program order.
    pub fn ids(&self) -> Vec<StatementId> {
        self.statements
            .iter()
            .copied()
            .filter(|id| !self.frozen.contains(id))
            .collect()
    }

    /// Returns every live statement, in program order, including the frozen ones.
    pub fn all_ids(&self) -> Vec<StatementId> {
        self.statements.clone()
    }

    /// Returns whether a statement is frozen, so that the stage may read it but not rewrite it.
    pub fn is_frozen(&self, id: StatementId) -> bool {
        self.frozen.contains(&id)
    }

    /// Returns the statement version with the given id.
    pub fn statement(&self, id: StatementId) -> &Statement {
        self.pipeline.statement(id)
    }

    /// Returns the absolute name that a predicate or a constant stands for
    /// in the scope of the base revision.
    ///
    /// # Errors
    ///
    /// Returns a diagnostic if the name uses a prefix that the base revision does not declare,
    /// or if it is a module-qualified name.
    pub fn resolve(&self, identifier: &Identifier) -> Result<String, Diagnostic> {
        self.scope().resolve(identifier, None)
    }

    /// Returns a bare predicate name that neither a statement nor the rows given through
    /// [import_rows](crate::engine::Engine::import_rows) use yet.
    ///
    /// The name is `stem` followed by the first number from 1 that makes it unused.
    /// A name that this method returns counts as used from then on.
    pub fn fresh(&mut self, stem: &str) -> Identifier {
        if self.taken.is_none() {
            let mut taken = self.names_of(&self.statements);
            taken.extend(self.imported.iter().cloned());
            self.taken = Some(taken);
        }
        for index in 1.. {
            let identifier = Identifier::bare(format!("{stem}{index}"));
            let name = self
                .scope()
                .resolve(&identifier, None)
                .expect("bare names resolve in any scope");
            if self
                .taken
                .as_mut()
                .expect("the names in use were collected")
                .insert(name)
            {
                return identifier;
            }
        }
        unreachable!("the naturals run out before the names do")
    }

    /// Adds a diagnostic to the pipeline's [reports](Pipeline::reports),
    /// without aborting the stage.
    pub fn report(&mut self, diagnostic: Diagnostic) {
        self.pipeline.report(diagnostic);
    }

    /// Adds a statement that produces no facts, such as a `@declare` directive, an output or an
    /// export, derived from `sources`.
    ///
    /// The statement is inserted before the first of `sources` that is live and not frozen,
    /// or at the end of the program if there is none.
    ///
    /// # Panics
    ///
    /// Panics if the statement is a rule, a fact or an import,
    /// since those change only through declared rewrites.
    pub fn add(&mut self, statement: Statement, sources: &[StatementId]) -> StatementId {
        self.expect_no_facts(&statement);
        self.insert(sources, statement)
    }

    /// Replaces a statement that produces no facts by another, in place.
    ///
    /// # Panics
    ///
    /// Panics if either is a rule, a fact or an import, or if the
    /// replaced statement is frozen or not live.
    pub fn replace(&mut self, old: StatementId, new: Statement) -> StatementId {
        self.expect_no_facts(self.pipeline.statement(old));
        self.expect_no_facts(&new);
        let position = self.position(old);
        self.statements.remove(position);
        self.derive_at(&[old], vec![new], position)[0]
    }

    /// Removes a statement that produces no facts,
    /// and records the removal as a provenance edge without targets.
    ///
    /// The version stays in the arena.
    ///
    /// # Panics
    ///
    /// Panics if the statement is frozen or not live,
    /// or if it is a rule, a fact or an import,
    /// since those change only through declared rewrites.
    pub fn remove(&mut self, old: StatementId) {
        self.position(old);
        self.expect_no_facts(self.pipeline.statement(old));
        self.take_out(old);
    }

    /// Removes a live statement from the working list,
    /// and records the removal as a provenance edge without targets.
    fn take_out(&mut self, old: StatementId) {
        self.pipeline.record_edge(ProvenanceEdge {
            stage: self.stage,
            sources: vec![old],
            targets: Vec::new(),
            declaration: None,
        });
        self.statements.retain(|&id| id != old);
    }

    /// Finishes the commit, creating the revision.
    ///
    /// # Panics
    ///
    /// Panics if the commit broke a rule of its methods:
    /// - a predicate is defined twice;
    /// - an input rule does not match the rule that replaces it along the given literal paths;
    /// - a statement other than a `@declare` directive names the predicate of an import removed
    ///   with [remove_import](Self::remove_import);
    /// - the commit wrote a construct that a loaded program cannot contain.
    pub fn finish(mut self) -> RevisionId {
        self.record_declarations();
        self.expect_unread();
        for &id in &self.created {
            if !self.statements.contains(&id) {
                continue;
            }
            let statement = self.pipeline.statement(id);
            for construct in UnlinkedConstruct::ALL {
                assert!(
                    !construct.occurs_in(statement),
                    "the `{}` stage wrote {construct}, which a loaded program cannot hold: \
                     `{statement}`",
                    self.stage,
                );
            }
        }
        self.pipeline.push_revision(ProgramRevision {
            parent: Some(self.base),
            stage: Some(self.stage),
            statements: self.statements,
        })
    }

    /// Replaces a statement by several of any kind, in place and in order,
    /// under one provenance edge and without a rewrite declaration.
    ///
    /// It lets tests build provenance edges without rewrite declarations.
    #[cfg(test)]
    pub(crate) fn split(&mut self, old: StatementId, new: Vec<Statement>) -> Vec<StatementId> {
        let position = self.position(old);
        self.statements.remove(position);
        self.derive_at(&[old], new, position)
    }

    /// Adds a statement of any kind at the end of the program,
    /// derived from `sources` and without a rewrite declaration.
    ///
    /// The sources stay live.
    #[cfg_attr(
        not(any(feature = "sparql", test)),
        expect(dead_code, reason = "only the SPARQL rewrite takes imports out")
    )]
    pub(crate) fn derive(&mut self, sources: &[StatementId], new: Statement) -> StatementId {
        self.derive_at(sources, vec![new], self.statements.len())[0]
    }

    /// Adds a statement derived from `sources`,
    /// before the first of them that is live and not frozen, or at the end if there is none.
    fn insert(&mut self, sources: &[StatementId], statement: Statement) -> StatementId {
        let position = sources
            .iter()
            .filter(|id| !self.frozen.contains(id))
            .find_map(|id| self.statements.iter().position(|candidate| candidate == id))
            .unwrap_or(self.statements.len());
        self.derive_at(sources, vec![statement], position)[0]
    }

    /// Records one provenance edge from `sources` to the statements `new`,
    /// and inserts them into the working list at `position`.
    ///
    /// A new statement without a span gets the span of the first source.
    fn derive_at(
        &mut self,
        sources: &[StatementId],
        new: Vec<Statement>,
        position: usize,
    ) -> Vec<StatementId> {
        let inherited = sources
            .first()
            .and_then(|&source| self.pipeline.statement(source).span());
        let edge = self.pipeline.record_edge(ProvenanceEdge {
            stage: self.stage,
            sources: sources.to_vec(),
            targets: Vec::new(),
            declaration: None,
        });

        let mut ids = Vec::with_capacity(new.len());
        for (offset, mut statement) in new.into_iter().enumerate() {
            if statement.span().is_none() {
                statement.set_span(inherited);
            }
            let id = self.pipeline.intern(statement, Some(edge));
            self.pipeline.edge_mut(edge).targets.push(id);
            self.statements.insert(position + offset, id);
            self.created.push(id);
            ids.push(id);
        }
        ids
    }

    /// Returns the position of a statement in the working list.
    ///
    /// # Panics
    ///
    /// Panics if the statement is frozen or not live.
    fn position(&self, id: StatementId) -> usize {
        assert!(
            !self.frozen.contains(&id),
            "the `{}` stage rewrote a frozen statement: `{}`",
            self.stage,
            self.pipeline.statement(id)
        );
        self.statements
            .iter()
            .position(|&candidate| candidate == id)
            .expect("rewritten statements are live in the commit")
    }

    /// Panics, naming the stage, if a statement produces facts,
    /// as a rule, a fact or an import does.
    fn expect_no_facts(&self, statement: &Statement) {
        assert!(
            !matches!(
                statement,
                Statement::Rule(_)
                    | Statement::Fact(_)
                    | Statement::Directive(Directive::Import(_))
            ),
            "the `{}` stage changed `{statement}` without declaring it; rules, facts and \
             imports change through declared rewrites",
            self.stage
        );
    }

    /// Returns the name scope of the base revision.
    fn scope(&self) -> &NameScope {
        self.scope.get_or_init(|| {
            let statements: Vec<Statement> = self
                .pipeline
                .statements(self.base)
                .map(|(_, statement)| statement.clone())
                .collect();
            NameScope::of(&statements)
        })
    }

    /// Returns the resolved names of the predicates that the statements `ids` name.
    ///
    /// A name that does not resolve is left out.
    fn names_of(&self, ids: &[StatementId]) -> HashSet<String> {
        let scope = self.scope();
        let mut names = HashSet::new();
        for &id in ids {
            // A fact names only its predicate, so it is not cloned for the walk below.
            if let Statement::Fact(fact) = self.pipeline.statement(id) {
                if let Ok(name) = scope.resolve(&fact.predicate, None) {
                    names.insert(name);
                }
                continue;
            }
            let mut note = |identifier: &Identifier,
                            role: IdentifierRole,
                            _: Option<Span>|
             -> Result<Option<Identifier>, Diagnostic> {
                if role == IdentifierRole::Predicate
                    && let Ok(name) = scope.resolve(identifier, None)
                {
                    names.insert(name);
                }
                Ok(None)
            };
            self.pipeline
                .statement(id)
                .clone()
                .rename_identifiers(&mut note)
                .expect("noting names fails nowhere");
        }
        names
    }
}
