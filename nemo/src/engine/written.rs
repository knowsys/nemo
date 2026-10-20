//! The written program: the program as loaded, before the pipeline stages rewrite it.
//!
//! Results, traces and diagnostics refer to statements by *statement id*:
//! a statement's position in the written program.
//! The ids follow from the program text alone, and an extension appends to them.
//! The pipeline stages rewrite the compiled program,
//! and results are mapped back onto these statements.
//! A predicate a stage introduced is *hidden*: no user wrote it,
//! and a diagnostic names the written literals it stands for instead.
//! Whether a predicate is derived or extensional also follows the written program,
//! however the stages rewrote its rules.

use std::collections::{HashMap, HashSet};

use nemo_physical::diagnostics::Diagnostic;

use crate::{
    model::{Atom, Directive, Identifier, IdentifierRole, Rule, Span, Statement},
    pipeline::{NameScope, Pipeline, RevisionId, RewriteDeclaration, StatementId},
};

/// The written program of a loaded engine.
#[derive(Debug, Clone)]
pub(crate) struct WrittenProgram {
    /// The written revision.
    revision: RevisionId,
    /// The written statements, in order: a statement's position is its
    /// id.
    statements: Vec<StatementId>,
    /// Per written statement, its id.
    ids: HashMap<StatementId, usize>,
    /// Every predicate the written statements name, by resolved name,
    /// with what they establish about it.
    predicates: HashMap<String, WrittenPredicate>,
    /// The predicates the written statements declare `@output` or
    /// export, in declaration order.
    outputs: Vec<String>,
    /// Per name of a hidden predicate, as written or resolved,
    /// the written literals it stands for, as a diagnostic names them.
    hidden: Vec<(String, String)>,
}

/// What the written statements establish about one predicate.
#[derive(Debug, Clone, Copy, Default)]
struct WrittenPredicate {
    /// Whether a written rule has it in a head.
    derived: bool,
    /// Whether a written fact, import, or `@input` produces facts for it.
    input_facts: bool,
    /// Its number of columns, if a written atom gives them by position.
    arity: Option<usize>,
}

/// How a statement uses a predicate.
#[derive(Debug, Clone, Copy)]
enum PredicateUse {
    /// It reads or declares it.
    Read,
    /// A rule derives into it.
    Derived,
    /// A fact, an import, or an `@input` produces facts for it.
    Input,
}

impl WrittenProgram {
    /// Collects the written program of a revision, and the hidden
    /// predicates of the revision made of it.
    pub(crate) fn new(pipeline: &Pipeline, written: RevisionId, revision: RevisionId) -> Self {
        let statements: Vec<StatementId> = pipeline.statements(written).map(|(id, _)| id).collect();
        let ids = statements
            .iter()
            .enumerate()
            .map(|(position, &id)| (id, position))
            .collect();
        let texts: Vec<Statement> = pipeline
            .statements(written)
            .map(|(_, statement)| statement.clone())
            .collect();
        let scope = NameScope::of(&texts);
        let outputs = declared_outputs(&texts, &scope);
        let mut predicates = HashMap::new();
        for statement in texts {
            note_statement(&mut predicates, &scope, statement);
        }
        Self {
            revision: written,
            statements,
            ids,
            predicates,
            outputs,
            hidden: hidden(pipeline, revision, &scope),
        }
    }

    /// Returns the written revision.
    pub(crate) fn revision(&self) -> RevisionId {
        self.revision
    }

    /// Returns the id of a written statement, or `None` for a statement a stage made.
    pub(crate) fn id(&self, statement: StatementId) -> Option<usize> {
        self.ids.get(&statement).copied()
    }

    /// Returns the statement behind an id.
    pub(crate) fn statement(&self, id: usize) -> Option<StatementId> {
        self.statements.get(id).copied()
    }

    /// Returns whether the written program names a predicate, by its
    /// resolved name.
    pub(crate) fn names(&self, predicate: &str) -> bool {
        self.predicates.contains_key(predicate)
    }

    /// Returns the resolved names of the written program's predicates.
    pub(crate) fn predicates(&self) -> impl Iterator<Item = &str> {
        self.predicates.keys().map(String::as_str)
    }

    /// Returns the predicates a written rule derives into.
    pub(crate) fn derived(&self) -> impl Iterator<Item = &str> {
        self.predicates
            .iter()
            .filter(|(_, predicate)| predicate.derived)
            .map(|(name, _)| name.as_str())
    }

    /// Returns whether a written rule derives into a predicate.
    pub(crate) fn derives(&self, predicate: &str) -> bool {
        self.predicates
            .get(predicate)
            .is_some_and(|predicate| predicate.derived)
    }

    /// Returns whether a written fact, import, or `@input` produces facts for a
    /// predicate.
    pub(crate) fn has_input_facts(&self, predicate: &str) -> bool {
        self.predicates
            .get(predicate)
            .is_some_and(|predicate| predicate.input_facts)
    }

    /// Returns the predicates the written statements declare `@output` or export,
    /// in declaration order, or none if they declare none.
    pub(crate) fn declared_outputs(&self) -> &[String] {
        &self.outputs
    }

    /// Returns a predicate's number of columns, if a written atom gives them by position.
    pub(crate) fn arity(&self, predicate: &str) -> Option<usize> {
        self.predicates.get(predicate)?.arity
    }

    /// Returns a diagnostic rewritten to refer to the written program:
    /// every hidden predicate it names is replaced by the written literals it stands for.
    pub(crate) fn present(&self, diagnostic: Diagnostic) -> Diagnostic {
        if self.hidden.is_empty() {
            return diagnostic;
        }
        diagnostic.map_text(|text| {
            let mut text = text.to_owned();
            for (spelling, literals) in &self.hidden {
                if text == *spelling {
                    return literals.clone();
                }
                text = text.replace(&format!("`{spelling}`"), literals);
            }
            text
        })
    }
}

/// Returns the predicates statements declare `@output` or export, by
/// resolved name, each once, in declaration order.
fn declared_outputs(statements: &[Statement], scope: &NameScope) -> Vec<String> {
    let mut outputs: Vec<String> = Vec::new();
    for statement in statements {
        let predicates: Vec<&Identifier> = match statement {
            Statement::Directive(Directive::Output(output)) => output.predicates.iter().collect(),
            Statement::Directive(Directive::Export(export)) => vec![&export.predicate],
            _ => continue,
        };
        for predicate in predicates {
            if let Ok(name) = scope.resolve(predicate, None)
                && !outputs.contains(&name)
            {
                outputs.push(name);
            }
        }
    }
    outputs
}

/// Notes every predicate a written statement names, and what the statement
/// establishes about it.
fn note_statement(
    predicates: &mut HashMap<String, WrittenPredicate>,
    scope: &NameScope,
    mut statement: Statement,
) {
    let mut note = |identifier: &Identifier, usage: PredicateUse, arity: Option<usize>| {
        let Ok(name) = scope.resolve(identifier, None) else {
            return;
        };
        let predicate: &mut WrittenPredicate = predicates.entry(name).or_default();
        predicate.derived |= matches!(usage, PredicateUse::Derived);
        predicate.input_facts |= matches!(usage, PredicateUse::Input);
        predicate.arity = predicate.arity.or(arity);
    };
    let by_position = |atom: &Atom| atom.labels.is_none().then_some(atom.terms.len());
    match &statement {
        Statement::Fact(fact) => {
            let arity = fact.labels.is_none().then_some(fact.terms.len());
            note(&fact.predicate, PredicateUse::Input, arity);
            return;
        }
        Statement::Rule(rule) => {
            for atom in &rule.head {
                note(&atom.predicate, PredicateUse::Derived, by_position(atom));
            }
            for atom in rule.body.iter().filter_map(|literal| literal.atom()) {
                note(&atom.predicate, PredicateUse::Read, by_position(atom));
            }
        }
        Statement::Directive(Directive::Import(import)) => {
            let arity = (!import.bindings.is_empty()).then_some(import.variables.len());
            note(&import.predicate, PredicateUse::Input, arity);
            for atom in &import.bindings {
                note(&atom.predicate, PredicateUse::Read, by_position(atom));
            }
        }
        Statement::Directive(Directive::Input(input)) => {
            note(&input.predicate, PredicateUse::Input, None);
        }
        Statement::Directive(_) => {}
    }
    // Every other predicate the statement names, as a declaration or an
    // output does.
    let mut named = |identifier: &Identifier,
                     role: IdentifierRole,
                     _: Option<Span>|
     -> Result<Option<Identifier>, Diagnostic> {
        if role == IdentifierRole::Predicate {
            note(identifier, PredicateUse::Read, None);
        }
        Ok(None)
    };
    statement
        .rename_identifiers(&mut named)
        .expect("noting names fails nowhere");
}

/// Returns, per name of a predicate a stage introduced on the way to a revision,
/// as written or resolved, the written literals it stands for: "the literals `a`, `b`".
fn hidden(pipeline: &Pipeline, revision: RevisionId, scope: &NameScope) -> Vec<(String, String)> {
    let mut hidden = Vec::new();
    let mut pending: Vec<StatementId> = pipeline.statements(revision).map(|(id, _)| id).collect();
    let mut seen = HashSet::new();
    while let Some(id) = pending.pop() {
        if !seen.insert(id) {
            continue;
        }
        let Some((declaration, sources)) = pipeline.declaration(id) else {
            continue;
        };
        let meaning: &Rule = match (declaration, pipeline.statement(id)) {
            (RewriteDeclaration::Replaces { .. }, _) => {
                pending.push(sources[0]);
                continue;
            }
            (RewriteDeclaration::Defines { view: Some(view) }, _) => view,
            (
                RewriteDeclaration::Defines { view: None } | RewriteDeclaration::DefinesFilter,
                Statement::Rule(rule),
            ) => rule,
            _ => continue,
        };
        let predicate = match pipeline.statement(id) {
            Statement::Directive(Directive::Import(import)) => &import.predicate,
            _ => &meaning.head[0].predicate,
        };
        let literals: Vec<String> = meaning
            .body
            .iter()
            .map(|literal| format!("`{literal}`"))
            .collect();
        let literals = format!("the literals {}", literals.join(", "));
        let mut spellings = vec![predicate.to_string()];
        if let Ok(name) = scope.resolve(predicate, None)
            && !spellings.contains(&name)
        {
            spellings.push(name);
        }
        for spelling in spellings {
            hidden.push((spelling, literals.clone()));
        }
    }
    hidden
}
