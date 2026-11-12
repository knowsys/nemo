//! What the stages declared about the statements of a revision:
//! which statement replaces which, and which predicates the stages defined.

use std::collections::{HashMap, HashSet};

use crate::{
    model::{Directive, Identifier, Rule, Statement},
    pipeline::{NameScope, Pipeline, RevisionId, RewriteDeclaration, StatementId},
};

/// How a stage defined a predicate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DefinitionKind {
    /// By a rule, which the engine evaluates.
    Rule,
    /// By an import, which evaluates the predicate at its endpoint.
    ///
    /// The import's view states what the predicate holds.
    View,
    /// By a rule that other statements use only to restrict their matches.
    Filter,
}

/// A predicate that a stage defined.
#[derive(Debug, Clone)]
pub(crate) struct Definition {
    /// The resolved name of the predicate.
    pub(crate) predicate: String,
    /// The statement that defines the predicate: a rule or an import.
    pub(crate) statement: StatementId,
    /// How the statement defines it.
    pub(crate) kind: DefinitionKind,
}

/// The rewrites that the stages declared on the way to a revision.
///
/// Results over the revision are mapped back onto the written program through them.
#[derive(Debug, Clone, Default)]
pub(crate) struct Rewrites {
    /// For each statement that a later stage rewrote, the statement that replaces it.
    later: HashMap<StatementId, StatementId>,
    /// The predicates that the stages defined, in the order in which the statements were
    /// found.
    definitions: Vec<Definition>,
    /// For each predicate, by resolved name, the import that replaces a rule deriving it.
    replacing: HashMap<String, StatementId>,
}

impl Rewrites {
    /// Returns the statement that replaces a statement, if a later stage rewrote it.
    pub(crate) fn later(&self, statement: StatementId) -> Option<StatementId> {
        self.later.get(&statement).copied()
    }

    /// Returns every predicate that the stages defined, filters included.
    pub(crate) fn definitions(&self) -> &[Definition] {
        &self.definitions
    }

    /// Returns the definition of a predicate, by resolved name,
    /// if a stage defined it for other statements to read its rows.
    ///
    /// Returns `None` for a filter, which no result is mapped through.
    pub(crate) fn definition(&self, predicate: &str) -> Option<&Definition> {
        self.definitions.iter().rev().find(|definition| {
            definition.kind != DefinitionKind::Filter && definition.predicate == predicate
        })
    }

    /// Returns the import that replaces a rule deriving a predicate, by resolved name,
    /// if a stage replaced one.
    pub(crate) fn replacing_import(&self, predicate: &str) -> Option<StatementId> {
        self.replacing.get(predicate).copied()
    }
}

impl Pipeline {
    /// Returns the rewrites that the stages declared on the way to a revision.
    ///
    /// They are found by following the provenance back from the revision's statements.
    /// Names are resolved in `scope`, and a predicate whose name does not resolve is left out.
    pub(crate) fn rewrites(&self, revision: RevisionId, scope: &NameScope) -> Rewrites {
        let mut rewrites = Rewrites::default();
        let mut pending: Vec<StatementId> = self.revision_statements(revision).to_vec();
        let mut seen = HashSet::new();
        while let Some(id) = pending.pop() {
            if !seen.insert(id) {
                continue;
            }
            let Some((declaration, sources)) = self.declaration(id) else {
                continue;
            };

            let kind = match declaration {
                RewriteDeclaration::Replaces { view, .. } => {
                    rewrites.later.insert(sources[0], id);
                    if view.is_some()
                        && let Statement::Directive(Directive::Import(import)) = self.statement(id)
                        && let Some(name) = scope.lookup(&import.predicate)
                    {
                        rewrites.replacing.insert(name, id);
                    }

                    pending.push(sources[0]);
                    continue;
                }
                RewriteDeclaration::Defines { view: Some(_) } => DefinitionKind::View,
                RewriteDeclaration::Defines { view: None } => DefinitionKind::Rule,
                RewriteDeclaration::DefinesFilter => DefinitionKind::Filter,
            };
            if let Some(predicate) = self
                .defined_predicate(id)
                .and_then(|name| scope.lookup(name))
            {
                rewrites.definitions.push(Definition {
                    predicate,
                    statement: id,
                    kind,
                });
            }
        }

        rewrites
    }

    /// Returns the predicate that a defining statement defines, as the statement writes it:
    /// the head of a rule, or the predicate of an import.
    pub(crate) fn defined_predicate(&self, statement: StatementId) -> Option<&Identifier> {
        match self.statement(statement) {
            Statement::Rule(rule) => Some(&rule.head.first()?.predicate),
            Statement::Directive(Directive::Import(import)) => Some(&import.predicate),
            Statement::Fact(_) | Statement::Directive(_) => None,
        }
    }

    /// Returns the rule that states what a defined predicate holds:
    /// the defining rule itself, or the view of the defining import.
    pub(crate) fn definition_rule(&self, definition: &Definition) -> Option<&Rule> {
        match (
            self.declaration(definition.statement),
            self.statement(definition.statement),
        ) {
            (Some((RewriteDeclaration::Defines { view: Some(view) }, _)), _) => Some(view),
            (_, Statement::Rule(rule)) => Some(rule),
            _ => None,
        }
    }
}
