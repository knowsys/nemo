//! The written program: the program as loaded, before the pipeline stages rewrite it.
//!
//! Results, traces and diagnostics refer to statements by *statement id*:
//! a statement's position in the written program.
//! The ids follow from the program text alone, and an extension appends to them.
//! The pipeline stages rewrite the program into the compiled program,
//! and results are mapped back onto the written statements.
//! No user wrote a predicate that a stage made,
//! so a diagnostic names the written literals that the predicate stands for instead.
//! Whether a predicate is derived or extensional also follows the written program,
//! however the stages rewrote its rules.

use std::collections::HashMap;

use nemo_physical::diagnostics::Diagnostic;

use crate::{
    model::{Atom, Directive, Identifier, Statement},
    pipeline::{NameScope, Pipeline, RevisionId, Rewrites, StatementId},
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
    /// The rewrites that the pipeline stages declared on the way to the compiled program.
    rewrites: Rewrites,
    /// The predicates that the stages made, as diagnostics present them.
    stage_made: Vec<StageMade>,
}

/// A predicate that a pipeline stage made, as a diagnostic presents it.
#[derive(Debug, Clone)]
struct StageMade {
    /// The names under which a diagnostic may refer to the predicate:
    /// as the stage wrote it, and resolved.
    spellings: Vec<String>,
    /// The phrase that names the written literals that the predicate stands for,
    /// such as "the literals `a(?x)`, `b(?x)`".
    literals: String,
    /// The resolved name of the written predicate of these literals,
    /// if they are all literals of one predicate.
    predicate: Option<String>,
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
    /// The statement reads or declares it.
    Read,
    /// The statement is a rule that derives into it.
    Derived,
    /// A fact, an import, or an `@input` produces facts for it.
    Input,
}

impl WrittenProgram {
    /// Collects the written program from the revision `written`,
    /// and what the pipeline stages declared on the way to `revision`.
    pub(crate) fn new(pipeline: &Pipeline, written: RevisionId, revision: RevisionId) -> Self {
        let statements: Vec<StatementId> = pipeline.statements(written).map(|(id, _)| id).collect();
        let ids = statements
            .iter()
            .enumerate()
            .map(|(position, &id)| (id, position))
            .collect();
        let texts = || pipeline.statements(written).map(|(_, statement)| statement);
        let scope = NameScope::of(texts());
        let outputs = declared_outputs(texts(), &scope);
        let mut predicates = HashMap::new();
        for statement in texts() {
            note_statement(&mut predicates, &scope, statement);
        }

        let rewrites = pipeline.rewrites(revision, &scope);
        let stage_made = stage_made(pipeline, &rewrites, &scope, &predicates);

        Self {
            revision: written,
            statements,
            ids,
            predicates,
            outputs,
            stage_made,
            rewrites,
        }
    }

    /// Returns the written revision.
    pub(crate) fn revision(&self) -> RevisionId {
        self.revision
    }

    /// Returns the rewrites that the pipeline stages declared on the way to the compiled
    /// program.
    pub(crate) fn rewrites(&self) -> &Rewrites {
        &self.rewrites
    }

    /// Returns the id of a written statement, or `None` for a statement a stage made.
    pub(crate) fn id(&self, statement: StatementId) -> Option<usize> {
        self.ids.get(&statement).copied()
    }

    /// Returns the statement with the given id, or `None` for an id beyond the written
    /// program.
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

    /// Returns a diagnostic rewritten to refer to the written program.
    ///
    /// Where a sentence of the diagnostic names a predicate that a stage made,
    /// it names the written literals that the predicate stands for instead.
    /// A field whose value is such a predicate gets the written predicate of these literals,
    /// and keeps its value if the literals are of several predicates.
    pub(crate) fn present(&self, diagnostic: Diagnostic) -> Diagnostic {
        if self.stage_made.is_empty() {
            return diagnostic;
        }

        let written: Vec<(&'static str, String)> = diagnostic
            .fields()
            .iter()
            .filter_map(|field| {
                let made = self
                    .stage_made
                    .iter()
                    .find(|made| made.spellings.contains(&field.value))?;
                Some((field.name, made.predicate.clone()?))
            })
            .collect();
        let diagnostic = written
            .into_iter()
            .fold(diagnostic, |diagnostic, (name, predicate)| {
                diagnostic.with_field(name, predicate)
            });

        diagnostic.map_text(|text| {
            let mut text = text.to_owned();
            for made in &self.stage_made {
                for spelling in &made.spellings {
                    text = text.replace(&format!("`{spelling}`"), &made.literals);
                }
            }

            text
        })
    }
}

/// Returns the predicates statements declare `@output` or export, by
/// resolved name, each once, in declaration order.
fn declared_outputs<'s>(
    statements: impl IntoIterator<Item = &'s Statement>,
    scope: &NameScope,
) -> Vec<String> {
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
    statement: &Statement,
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
    match statement {
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
    statement.visit_predicates(|predicate| note(predicate, PredicateUse::Read, None));
}

/// Returns the predicates that the stages defined, as diagnostics present them.
///
/// `scope` resolves the names of the written program, and `predicates` are its predicates.
fn stage_made(
    pipeline: &Pipeline,
    rewrites: &Rewrites,
    scope: &NameScope,
    predicates: &HashMap<String, WrittenPredicate>,
) -> Vec<StageMade> {
    let mut made = Vec::new();
    for definition in rewrites.definitions() {
        let (Some(meaning), Some(name)) = (
            pipeline.definition_rule(definition),
            pipeline.defined_predicate(definition.statement),
        ) else {
            continue;
        };

        let literals: Vec<String> = meaning
            .body
            .iter()
            .map(|literal| format!("`{literal}`"))
            .collect();

        let mut spellings = vec![name.to_string()];
        if !spellings.contains(&definition.predicate) {
            spellings.push(definition.predicate.clone());
        }

        // The literals name one written predicate, several, or none.
        let mut read: Vec<String> = meaning
            .body
            .iter()
            .filter_map(|literal| scope.lookup(&literal.atom()?.predicate))
            .collect();
        read.sort();
        read.dedup();
        let predicate = match read.as_slice() {
            [only] if predicates.contains_key(only) => Some(only.clone()),
            _ => None,
        };

        made.push(StageMade {
            spellings,
            literals: format!("the literals {}", literals.join(", ")),
            predicate,
        });
    }

    made
}
