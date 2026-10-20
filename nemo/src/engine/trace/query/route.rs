//! Routing written literals forward, into the compiled program.
//!
//! A query names written statements, and the children of its nodes are at written literals.
//! A pipeline stage that rewrote a statement moved its literals:
//! into the rule that replaces it, into the rule defining a predicate the stage introduced,
//! or into the view of an import.
//! A *route* follows a literal through every stage's recorded correspondence
//! to the literal it became in the final revision.
//! That is a literal of a statement of the compiled program, reached through *hops*:
//! literals that read a predicate a stage introduced, each leading into its definition.
//! For a literal over a predicate an import evaluates at its endpoint,
//! the route ends at the literal reading the import's predicate.

use std::collections::{HashMap, HashSet};

use crate::{
    engine::compile::{compiled::CompiledProgram, translation::TranslationState},
    model::{Directive, Literal, Rule, Statement, Term, TermKind},
    pipeline::{Pipeline, RevisionId, RewriteDeclaration, StatementId},
};

/// The literal a written literal became in the compiled program, and the way there.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct Route {
    /// The literals on the way, each reading a predicate a stage introduced
    /// and the engine evaluates.
    ///
    /// The next literal is in that predicate's definition.
    pub(super) hops: Vec<(StatementId, usize)>,
    /// The literal the route ends at.
    pub(super) last: (StatementId, usize),
    /// Whether the last literal reads a predicate an import evaluates at its endpoint,
    /// whose rows the written literal is matched against.
    pub(super) remote: bool,
}

/// How a written statement runs.
#[derive(Debug, Clone, Copy)]
pub(super) enum Realization {
    /// A compiled rule derives its facts.
    Rule(usize),
    /// An import that replaces the statement delivers them.
    Import(StatementId),
}

/// The routing over one engine's program.
pub(super) struct Router<'a> {
    /// The program pipeline.
    pipeline: &'a Pipeline,
    /// The translation state, for resolving names.
    state: &'a TranslationState,
    /// Per statement a later stage rewrote, the statement that replaces it.
    later: HashMap<StatementId, StatementId>,
    /// Per predicate a stage defined, by resolved name, the defining
    /// statement and whether an import evaluates it.
    definitions: HashMap<String, (StatementId, bool)>,
    /// Per statement of the compiled program, its compiled rule.
    compiled: HashMap<StatementId, usize>,
}

impl<'a> Router<'a> {
    /// Prepares the routing from the revision of the compiled program back to its written
    /// program.
    pub(super) fn new(
        pipeline: &'a Pipeline,
        state: &'a TranslationState,
        program: &'a CompiledProgram,
        revision: RevisionId,
    ) -> Self {
        let mut router = Self {
            pipeline,
            state,
            later: HashMap::new(),
            definitions: HashMap::new(),
            compiled: program
                .statements
                .iter()
                .enumerate()
                .map(|(index, &statement)| (statement, index))
                .collect(),
        };
        router.collect(pipeline.statements(revision).map(|(id, _)| id).collect());
        router
    }

    /// Collects the later versions and the definitions,
    /// following the provenance back from the given statements.
    fn collect(&mut self, mut pending: Vec<StatementId>) {
        let mut seen = HashSet::new();
        while let Some(id) = pending.pop() {
            if !seen.insert(id) {
                continue;
            }
            match self.pipeline.declaration(id) {
                Some((RewriteDeclaration::Replaces { .. }, sources)) => {
                    self.later.insert(sources[0], id);
                    pending.push(sources[0]);
                }
                Some((RewriteDeclaration::Defines { view }, _)) => {
                    let predicate = match self.pipeline.statement(id) {
                        Statement::Rule(rule) => &rule.head[0].predicate,
                        Statement::Directive(Directive::Import(import)) => &import.predicate,
                        Statement::Fact(_) | Statement::Directive(_) => continue,
                    };
                    if let Some(name) = self.state.resolve(predicate) {
                        self.definitions.insert(name, (id, view.is_some()));
                    }
                }
                Some((RewriteDeclaration::DefinesFilter, _)) | None => {}
            }
        }
    }

    /// Returns how a written statement runs,
    /// or `None` if its final version is not part of the compiled program.
    pub(super) fn realize(&self, statement: StatementId) -> Option<Realization> {
        let current = self.current(statement);
        if let Statement::Directive(Directive::Import(_)) = self.pipeline.statement(current)
            && let Some((RewriteDeclaration::Replaces { .. }, _)) =
                self.pipeline.declaration(current)
        {
            return Some(Realization::Import(current));
        }
        self.compiled.get(&current).copied().map(Realization::Rule)
    }

    /// Returns the compiled rule of a statement of the compiled program.
    pub(super) fn compiled(&self, statement: StatementId) -> usize {
        self.compiled[&statement]
    }

    /// Returns the route to the literal a literal of a statement became in the final revision.
    ///
    /// The literal is given by its path in the statement's rule: a literal, then,
    /// inside each definition unfolded on the way, a literal of that definition.
    pub(super) fn route(&self, statement: StatementId, path: &[usize]) -> Route {
        let (&position, rest) = path.split_first().expect("paths are not empty");
        let base = match self.later.get(&statement) {
            Some(&next) => {
                let Some((RewriteDeclaration::Replaces { correspondence, .. }, _)) =
                    self.pipeline.declaration(next)
                else {
                    unreachable!("a later version replaces its input")
                };
                self.route(next, &correspondence.literals[position])
            }
            None => Route {
                hops: Vec::new(),
                last: (statement, position),
                remote: false,
            },
        };
        if rest.is_empty() || base.remote {
            return base;
        }
        let Some((definition, remote)) = self.definition_at(statement, position) else {
            return base;
        };
        if remote {
            return Route {
                remote: true,
                ..base
            };
        }
        let inner = self.route(definition, rest);
        let mut hops = base.hops;
        hops.push(base.last);
        hops.extend(inner.hops);
        Route {
            hops,
            last: inner.last,
            remote: inner.remote,
        }
    }

    /// Returns the definition of the predicate a literal of a statement
    /// reads, and whether an import evaluates it.
    fn definition_at(
        &self,
        statement: StatementId,
        position: usize,
    ) -> Option<(StatementId, bool)> {
        let atom = self.rule(statement)?.body[position].atom()?;
        let name = self.state.resolve(&atom.predicate)?;
        self.definitions.get(&name).copied()
    }

    /// Returns the view of the import defining the predicate
    /// that a remote route's last literal reads.
    pub(super) fn remote_view(&self, route: &Route) -> Option<&'a Rule> {
        let (definition, _) = self.definition_at(route.last.0, route.last.1)?;
        match self.pipeline.declaration(definition) {
            Some((RewriteDeclaration::Defines { view: Some(view) }, _)) => Some(view),
            _ => None,
        }
    }

    /// Returns the view of an import that replaces a written statement.
    pub(super) fn replacing_view(&self, import: StatementId) -> Option<&'a Rule> {
        match self.pipeline.declaration(import) {
            Some((
                RewriteDeclaration::Replaces {
                    view: Some(view), ..
                },
                _,
            )) => Some(view),
            _ => None,
        }
    }

    /// Returns the final version of a statement.
    fn current(&self, mut statement: StatementId) -> StatementId {
        while let Some(&next) = self.later.get(&statement) {
            statement = next;
        }
        statement
    }

    /// Returns the rule of a statement, or `None` if the statement is not a rule.
    pub(super) fn rule(&self, statement: StatementId) -> Option<&'a Rule> {
        match self.pipeline.statement(statement) {
            Statement::Rule(rule) => Some(rule),
            _ => None,
        }
    }

    /// Returns the compiled body atom a positive literal of a statement's
    /// rule compiled into: its position among the positive literals.
    pub(super) fn atom_of(&self, statement: StatementId, literal: usize) -> usize {
        let rule = self.rule(statement).expect("routes end at rule literals");
        rule.body[..literal]
            .iter()
            .filter(|literal| matches!(literal, Literal::Positive(_)))
            .count()
    }

    /// Returns the resolved name of the predicate a literal of a
    /// statement reads.
    pub(super) fn predicate_at(&self, statement: StatementId, literal: usize) -> Option<String> {
        let atom = self.rule(statement)?.body[literal].atom()?;
        self.state.resolve(&atom.predicate)
    }
}

/// Returns, per written column, the column of a view's rows that holds it,
/// or the written term if none does.
///
/// Columns are matched by the variable's name, since the stages copy literals as written.
pub(super) fn view_columns(view: &Rule, written: &[Term]) -> Vec<Result<usize, Term>> {
    let head = &view.head[0].terms;
    written
        .iter()
        .map(|term| {
            let TermKind::Variable(variable) = &term.kind else {
                return Err(term.clone());
            };
            head.iter()
                .position(|column| {
                    matches!(&column.kind, TermKind::Variable(column) if column == variable)
                })
                .ok_or_else(|| term.clone())
        })
        .collect()
}
