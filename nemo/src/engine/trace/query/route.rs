//! Routing written literals forward, into the compiled program.
//!
//! A query names written statements, and the children of its nodes are at written literals.
//! A pipeline stage that rewrote a statement moved its literals,
//! into the rule that replaces it, into the definition of a predicate that the stage
//! introduced, or into the view of an import.
//! A *route* follows a literal through the correspondence that each stage recorded,
//! to the literal it became in the final revision, which is a literal of the compiled program.
//! On the way, the route passes *hops*,
//! which are literals that read a predicate a stage introduced, each leading into its
//! definition.
//! For a literal over a predicate that an import evaluates at its endpoint,
//! the route ends at the literal that reads the import's predicate.

use std::collections::HashMap;

use crate::{
    engine::compile::{compiled::CompiledProgram, translation::TranslationState},
    model::{Directive, Literal, Rule, Statement, Term, TermKind},
    pipeline::{Definition, DefinitionKind, Pipeline, RewriteDeclaration, Rewrites, StatementId},
};

/// A body literal of a statement.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct StatementLiteral {
    /// The statement.
    pub(super) statement: StatementId,
    /// The literal's position in the statement's body.
    pub(super) literal: usize,
}

/// The literal a written literal became in the compiled program, and the way there.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct Route {
    /// The hops on the way, each a literal that reads a predicate which a stage introduced
    /// and the engine evaluates.
    ///
    /// The next literal is in that predicate's definition.
    pub(super) hops: Vec<StatementLiteral>,
    /// The literal the route ends at.
    pub(super) last: StatementLiteral,
    /// Whether the last literal reads a predicate that an import evaluates at its endpoint,
    /// whose rows the written literal is matched against.
    pub(super) remote: bool,
}

/// How a written statement runs.
#[derive(Debug, Clone, Copy)]
pub(super) enum Realization {
    /// The compiled rule with the given index derives its facts.
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
    /// The rewrites that the pipeline stages declared while they produced the compiled program.
    rewrites: &'a Rewrites,
    /// The index of the compiled rule of each statement of the compiled program.
    compiled: HashMap<StatementId, usize>,
}

impl<'a> Router<'a> {
    /// Prepares the routing from the written program into the compiled program,
    /// along the rewrites that the stages declared.
    pub(super) fn new(
        pipeline: &'a Pipeline,
        state: &'a TranslationState,
        program: &'a CompiledProgram,
        rewrites: &'a Rewrites,
    ) -> Self {
        Self {
            pipeline,
            state,
            rewrites,
            compiled: program
                .statements
                .iter()
                .enumerate()
                .map(|(index, &statement)| (statement, index))
                .collect(),
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

    /// Returns the index of the compiled rule of a statement of the compiled program.
    pub(super) fn compiled(&self, statement: StatementId) -> usize {
        self.compiled[&statement]
    }

    /// Returns the route to the literal that a literal of a statement became in the final
    /// revision.
    ///
    /// The literal is given by its [path](crate::pipeline::LiteralPath) in the statement's
    /// rule.
    pub(super) fn route(&self, statement: StatementId, path: &[usize]) -> Route {
        let (&position, rest) = path.split_first().expect("paths are not empty");
        let base = match self.rewrites.later(statement) {
            Some(next) => {
                let Some((RewriteDeclaration::Replaces { correspondence, .. }, _)) =
                    self.pipeline.declaration(next)
                else {
                    unreachable!("a later version replaces its input")
                };

                self.route(next, &correspondence.literals[position])
            }
            None => Route {
                hops: Vec::new(),
                last: StatementLiteral {
                    statement,
                    literal: position,
                },
                remote: false,
            },
        };
        if rest.is_empty() || base.remote {
            return base;
        }
        let Some(definition) = self.definition_at(statement, position) else {
            return base;
        };
        if definition.kind == DefinitionKind::View {
            return Route {
                remote: true,
                ..base
            };
        }

        let inner = self.route(definition.statement, rest);
        let mut hops = base.hops;
        hops.push(base.last);
        hops.extend(inner.hops);

        Route {
            hops,
            last: inner.last,
            remote: inner.remote,
        }
    }

    /// Returns the definition of the predicate that a literal of a statement reads,
    /// if a stage defined the predicate.
    fn definition_at(&self, statement: StatementId, position: usize) -> Option<&'a Definition> {
        let atom = self.rule(statement)?.body[position].atom()?;
        let name = self.state.resolve(&atom.predicate)?;
        self.rewrites.definition(&name)
    }

    /// Returns the view of the import defining the predicate
    /// that a remote route's last literal reads.
    pub(super) fn remote_view(&self, route: &Route) -> Option<&'a Rule> {
        let definition = self.definition_at(route.last.statement, route.last.literal)?;
        match self.pipeline.declaration(definition.statement) {
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
        while let Some(next) = self.rewrites.later(statement) {
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

    /// Returns the index of the compiled body atom that a positive literal of a statement's
    /// rule became, which is its position among the positive literals.
    pub(super) fn atom_of(&self, statement: StatementId, literal: usize) -> usize {
        let rule = self.rule(statement).expect("routes end at rule literals");

        rule.body[..literal]
            .iter()
            .filter(|literal| matches!(literal, Literal::Positive(_)))
            .count()
    }

    /// Returns the resolved name of the predicate that a literal of a statement reads.
    pub(super) fn predicate_at(&self, statement: StatementId, literal: usize) -> Option<String> {
        let atom = self.rule(statement)?.body[literal].atom()?;
        self.state.resolve(&atom.predicate)
    }
}

/// Returns, for each written column, the column of a view's rows that contains it,
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
