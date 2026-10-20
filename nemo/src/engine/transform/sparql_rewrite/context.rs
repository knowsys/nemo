//! What the SPARQL rewrite knows of the program: its name scope and parameters,
//! the candidate imports, and the declarations, writers and dependencies of its predicates.

use std::collections::{HashMap, HashSet};

use nemo_io::sparql::{QueryShape, SparqlQuery};

use crate::{
    engine::transform::sparql_rewrite::is_sparql,
    model::{
        DeclareDirective, Directive, Identifier, ImportDirective, Literal, OnError, Rule,
        Statement, Term, TermKind,
    },
    pipeline::{NameScope, StatementId},
};

/// A plain import the stage may rewrite rules over.
pub(super) struct CandidateImport {
    /// The directive's statement.
    pub(super) id: StatementId,
    /// The directive.
    pub(super) directive: ImportDirective,
    /// Its validated query.
    pub(super) query: SparqlQuery,
    /// The local part of the predicate's name, seeding fresh names.
    pub(super) stem: String,
}

impl CandidateImport {
    /// Returns the import's `endpoint` option.
    pub(super) fn endpoint(&self) -> Option<&Term> {
        option(&self.directive, "endpoint")
    }

    /// Returns what a failed fetch of the import does.
    pub(super) fn on_error(&self) -> OnError {
        option(&self.directive, "on_error")
            .and_then(OnError::from_term)
            .unwrap_or_default()
    }

    /// Returns whether the query is never merged with another:
    /// it names its own dataset, which applies to a whole query,
    /// or it [keeps its variable names](SparqlQuery::keeps_names),
    /// which a second instance in one query could not.
    pub(super) fn stands_alone(&self) -> bool {
        self.query.dataset().is_some() || self.query.keeps_names()
    }
}

/// What the stage knows of the program while rewriting it.
pub(super) struct RewriteContext {
    /// The name scope.
    pub(super) scope: NameScope,
    /// The parameters with values, by name.
    pub(super) parameters: HashMap<String, Term>,
    /// The candidate imports by resolved predicate name.
    pub(super) candidates: HashMap<String, CandidateImport>,
    /// How many rule heads write each predicate.
    pub(super) writers: HashMap<String, usize>,
    /// The declarations by resolved predicate name,
    /// which a fresh import copies from the candidate imports it replaces.
    pub(super) declarations: HashMap<String, DeclareDirective>,
    /// Per rule head predicate, the predicates its rules read,
    /// the dependency graph in which a binding must not close a negative cycle.
    dependencies: HashMap<String, HashSet<String>>,
}

impl RewriteContext {
    /// Surveys the program: its scope, parameters, candidates, and predicates.
    pub(super) fn new(statements: &[Statement], ids: &[StatementId]) -> Self {
        let mut builder = RewriteContextBuilder::new(NameScope::of(statements));
        for (&id, statement) in ids.iter().zip(statements) {
            builder.statement(id, statement);
        }
        builder.finish()
    }

    /// Returns whether the atoms at the given body positions depend on one of the rule's head
    /// predicates, reading it, or reading what is derived from it.
    pub(super) fn recurs_through(&self, positions: &[usize], rule: &Rule) -> bool {
        let heads: HashSet<String> = rule
            .head
            .iter()
            .filter_map(|atom| self.scope.resolve(&atom.predicate, atom.span).ok())
            .collect();
        let mut pending: Vec<String> = positions
            .iter()
            .filter_map(|&position| match &rule.body[position] {
                Literal::Positive(atom) | Literal::Negative(atom) => {
                    self.scope.resolve(&atom.predicate, atom.span).ok()
                }
                Literal::Operation(_) => None,
            })
            .collect();
        let mut visited: HashSet<String> = HashSet::new();
        while let Some(name) = pending.pop() {
            if heads.contains(&name) {
                return true;
            }
            if visited.insert(name.clone())
                && let Some(read) = self.dependencies.get(&name)
            {
                pending.extend(read.iter().cloned());
            }
        }
        false
    }
}

/// A rewrite context under construction: what each statement contributes,
/// before the candidates are chosen.
struct RewriteContextBuilder<'a> {
    /// The name scope.
    scope: NameScope,
    /// The parameters with values, by name.
    parameters: HashMap<String, Term>,
    /// How many rule heads write each predicate.
    writers: HashMap<String, usize>,
    /// The predicates that cannot be candidates: those written by facts,
    /// rule heads, or `@input`, and those read by exports, outputs, or a
    /// bound import's fetch.
    sealed: HashSet<String>,
    /// The import directives of each predicate.
    imports: HashMap<String, Vec<(StatementId, &'a ImportDirective)>>,
    /// The declarations by resolved predicate name.
    declarations: HashMap<String, DeclareDirective>,
    /// Per rule head predicate, the predicates its rules read.
    dependencies: HashMap<String, HashSet<String>>,
}

impl<'a> RewriteContextBuilder<'a> {
    /// Creates an empty builder in a scope.
    fn new(scope: NameScope) -> Self {
        Self {
            scope,
            parameters: HashMap::new(),
            writers: HashMap::new(),
            sealed: HashSet::new(),
            imports: HashMap::new(),
            declarations: HashMap::new(),
            dependencies: HashMap::new(),
        }
    }

    /// Returns the resolved name of a predicate, or `None` if it does not resolve.
    fn resolve(&self, predicate: &Identifier) -> Option<String> {
        self.scope.resolve(predicate, None).ok()
    }

    /// Notes a predicate that cannot be a candidate.
    fn seal(&mut self, predicate: &Identifier) {
        if let Some(name) = self.resolve(predicate) {
            self.sealed.insert(name);
        }
    }

    /// Records one statement.
    fn statement(&mut self, id: StatementId, statement: &'a Statement) {
        match statement {
            Statement::Fact(fact) => self.seal(&fact.predicate),
            Statement::Rule(rule) => self.rule(rule),
            Statement::Directive(Directive::Import(import)) => self.import(id, import),
            Statement::Directive(Directive::Declare(declare)) => {
                if let Some(name) = self.resolve(&declare.predicate) {
                    self.declarations.insert(name, declare.clone());
                }
            }
            Statement::Directive(Directive::Export(export)) => self.seal(&export.predicate),
            Statement::Directive(Directive::Output(output)) => {
                for predicate in &output.predicates {
                    self.seal(predicate);
                }
            }
            Statement::Directive(Directive::Input(input)) => self.seal(&input.predicate),
            Statement::Directive(Directive::Parameter(parameter)) => {
                if let Some(value) = &parameter.value {
                    self.parameters
                        .insert(parameter.variable.name.clone(), value.clone());
                }
            }
            Statement::Directive(_) => {}
        }
    }

    /// Records a rule: its heads as writers, and what they depend on.
    fn rule(&mut self, rule: &Rule) {
        let mut heads = Vec::with_capacity(rule.head.len());
        for atom in &rule.head {
            if let Some(name) = self.resolve(&atom.predicate) {
                *self.writers.entry(name.clone()).or_default() += 1;
                self.sealed.insert(name.clone());
                heads.push(name);
            }
        }
        for literal in &rule.body {
            let Some(atom) = literal.atom() else {
                continue;
            };
            if let Some(name) = self.resolve(&atom.predicate) {
                for head in &heads {
                    self.dependencies
                        .entry(head.clone())
                        .or_default()
                        .insert(name.clone());
                }
            }
        }
    }

    /// Records an import: its directive under its predicate,
    /// and the binding predicates its fetch reads,
    /// which must be stored as written and so cannot be candidates.
    fn import(&mut self, id: StatementId, import: &'a ImportDirective) {
        if let Some(name) = self.resolve(&import.predicate) {
            self.imports.entry(name).or_default().push((id, import));
        }
        for atom in &import.bindings {
            self.seal(&atom.predicate);
        }
    }

    /// Chooses the candidates and returns the finished context.
    ///
    /// A candidate is a plain `sparql` import that is the only statement defining its predicate,
    /// whose predicate only rule bodies read,
    /// and whose query is a string that parses and can be restricted.
    fn finish(self) -> RewriteContext {
        let mut candidates = HashMap::new();
        for (name, directives) in self.imports {
            let [(id, directive)] = directives.as_slice() else {
                continue;
            };
            if !is_sparql(directive)
                || !directive.bindings.is_empty()
                || self.sealed.contains(&name)
            {
                continue;
            }
            let Some(query) = query_of(directive, &self.scope) else {
                continue;
            };
            if matches!(query.shape(), QueryShape::Opaque(_)) {
                continue;
            }
            candidates.insert(
                name,
                CandidateImport {
                    id: *id,
                    directive: (*directive).clone(),
                    query,
                    stem: stem(&directive.predicate),
                },
            );
        }
        RewriteContext {
            scope: self.scope,
            parameters: self.parameters,
            candidates,
            writers: self.writers,
            declarations: self.declarations,
            dependencies: self.dependencies,
        }
    }
}

/// Returns a directive option's value by name.
fn option<'a>(directive: &'a ImportDirective, name: &str) -> Option<&'a Term> {
    directive.options.iter().find_map(|(key, value)| {
        matches!(&key.kind, TermKind::Constant(Identifier::Bare(key)) if key == name)
            .then_some(value)
    })
}

/// Returns the validated query of a plain import,
/// if its `query` option is a string (or absent) and parses in the scope.
fn query_of(directive: &ImportDirective, scope: &NameScope) -> Option<SparqlQuery> {
    let text = match option(directive, "query") {
        None => nemo_io::sparql::DEFAULT_QUERY.to_owned(),
        Some(Term {
            kind: TermKind::String(text),
            ..
        }) => text.clone(),
        Some(_) => return None,
    };
    let base = match option(directive, "base") {
        None => scope.base().map(str::to_owned),
        Some(Term {
            kind: TermKind::Constant(name),
            ..
        }) => scope.resolve(name, None).ok(),
        Some(Term {
            kind: TermKind::String(iri),
            ..
        }) => Some(iri.clone()),
        Some(_) => return None,
    };
    SparqlQuery::parse(
        &text,
        base.as_deref(),
        scope
            .prefixes()
            .iter()
            .map(|(name, iri)| (name.as_str(), iri.as_str())),
    )
    .ok()
}

/// Returns the local part of a predicate name, as a bare-name stem.
fn stem(predicate: &Identifier) -> String {
    let local = match predicate {
        Identifier::Bare(name)
        | Identifier::Prefixed { name, .. }
        | Identifier::Qualified { name, .. } => name.clone(),
        Identifier::Iri(iri) => iri.rsplit(['/', '#', ':']).next().unwrap_or(iri).to_owned(),
    };
    let stem: String = local
        .chars()
        .filter(|character| character.is_ascii_alphanumeric() || *character == '_')
        .collect();
    if stem.is_empty() || stem.starts_with(|character: char| character.is_ascii_digit()) {
        format!("import{stem}")
    } else {
        stem
    }
}
