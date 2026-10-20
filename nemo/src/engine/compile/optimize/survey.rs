//! What the import passes read of a program: its rules in positional
//! form, and which predicates must be stored whole.

use std::collections::{HashMap, HashSet};

use nemo_physical::diagnostics::Diagnostic;

use crate::{
    engine::compile::translation::Translator,
    model::{
        Atom, Directive, Identifier, ImportDirective, Literal, Rule, Statement, Term, TermKind,
        Variable, VariableKind, Walk,
    },
};

/// A program as the import passes see it.
pub(crate) struct ImportSurvey {
    /// The rules, named arguments resolved to declared positions.
    pub(crate) rules: Vec<Rule>,
    /// The predicates an import feeds.
    pub(crate) imported: HashSet<String>,
    /// The predicates that must be stored whole,
    /// since something other than a rule can read them:
    /// those written by facts, rule heads, or `@input`, the outputs,
    /// and those derived in the written program by rules a pipeline stage replaced.
    pub(crate) sealed: HashSet<String>,
    /// The predicates some rule negates.
    pub(crate) negated: HashSet<String>,
    /// Per predicate of an import with a `where` clause,
    /// which of its kept columns the clause reads, whatever the rules read:
    /// the columns its atoms bind, the keys its fetch sends,
    /// and the columns its conditions test.
    pub(crate) bound: HashMap<String, Vec<bool>>,
    /// The predicates of an import with conditions on columns the survey cannot place
    /// among the kept ones, since its `format` option is no list of names:
    /// they are stored whole.
    pub(crate) conditioned: HashSet<String>,
}

impl ImportSurvey {
    /// Surveys the statements of one translation.
    pub(crate) fn new(
        translator: &Translator<'_>,
        statements: &[Statement],
    ) -> Result<Self, Diagnostic> {
        let mut survey = Self {
            rules: Vec::new(),
            imported: HashSet::new(),
            sealed: HashSet::new(),
            negated: HashSet::new(),
            bound: HashMap::new(),
            conditioned: HashSet::new(),
        };
        // A predicate the written program derives is stored whole, even
        // if a pipeline stage replaced its rules with an import.
        if let Some(written) = translator.written {
            survey.sealed.extend(written.derived().map(str::to_owned));
        }
        for statement in statements {
            match statement {
                Statement::Fact(fact) => {
                    survey
                        .sealed
                        .insert(translator.resolve(&fact.predicate, fact.span)?);
                }
                Statement::Rule(rule) => survey.take_rule(translator, rule)?,
                Statement::Directive(Directive::Import(import)) => {
                    survey.take_import(translator, import)?;
                }
                Statement::Directive(Directive::Export(export)) => {
                    survey
                        .sealed
                        .insert(translator.resolve(&export.predicate, export.span)?);
                }
                Statement::Directive(Directive::Output(output)) => {
                    for predicate in &output.predicates {
                        survey
                            .sealed
                            .insert(translator.resolve(predicate, output.span)?);
                    }
                }
                Statement::Directive(Directive::Input(input)) => {
                    survey
                        .sealed
                        .insert(translator.resolve(&input.predicate, input.span)?);
                }
                Statement::Directive(_) => {}
            }
        }
        Ok(survey)
    }

    /// Adds a rule: its head predicates are sealed, its negated predicates recorded,
    /// and the rule, with named arguments made positional, joins the surveyed rules.
    fn take_rule(&mut self, translator: &Translator<'_>, rule: &Rule) -> Result<(), Diagnostic> {
        let rule = translator
            .positional_rule(rule)?
            .unwrap_or_else(|| rule.clone());
        for atom in &rule.head {
            self.sealed
                .insert(translator.resolve(&atom.predicate, atom.span)?);
        }
        for literal in &rule.body {
            if let Literal::Negative(atom) = literal {
                self.negated
                    .insert(translator.resolve(&atom.predicate, atom.span)?);
            }
        }
        self.rules.push(rule);
        Ok(())
    }

    /// Adds an import.
    ///
    /// An import with a `where` clause reads the clause's predicates like a rule
    /// whose head is the import.
    /// Every named column is read there, so the passes keep what the fetch needs.
    fn take_import(
        &mut self,
        translator: &Translator<'_>,
        import: &ImportDirective,
    ) -> Result<(), Diagnostic> {
        let name = translator.resolve(&import.predicate, import.span)?;
        self.imported.insert(name.clone());
        if !import.conditions.is_empty() {
            match tested_columns(import) {
                Some(tested) => self.read_by_import(&name, tested),
                None => {
                    self.conditioned.insert(name.clone());
                }
            }
        }
        if import.bindings.is_empty() {
            return Ok(());
        }
        let mentioned: HashSet<&str> = import
            .bindings
            .iter()
            .flat_map(|atom| atom.terms.iter())
            .filter_map(|term| match &term.kind {
                TermKind::Variable(variable) if variable.kind == VariableKind::Universal => {
                    Some(variable.name.as_str())
                }
                _ => None,
            })
            .collect();
        let bound: Vec<bool> = import
            .variables
            .iter()
            .map(|variable| mentioned.contains(variable.as_str()))
            .collect();
        self.read_by_import(&name, bound);
        self.rules.push(Rule {
            attributes: Vec::new(),
            head: vec![Atom::new(
                import.predicate.clone(),
                import
                    .variables
                    .iter()
                    .map(|name| Term::variable(Variable::universal(name))),
            )],
            body: import
                .bindings
                .iter()
                .cloned()
                .map(Literal::Positive)
                .collect(),
            span: import.span,
        });
        Ok(())
    }

    /// Returns whether a predicate is a candidate: import-fed, and read by
    /// rules only.
    pub(crate) fn optimizable(&self, name: &str) -> bool {
        self.imported.contains(name) && !self.sealed.contains(name)
    }
}

/// Returns how often each universal variable occurs in a rule.
///
/// Every position counts: head and body, atoms and conditions, nested terms included.
/// A variable inside an aggregate counts twice,
/// since an aggregate reads it in ways a single occurrence does not capture.
pub(crate) fn occurrences(rule: &Rule) -> HashMap<String, usize> {
    let mut counts = HashMap::new();
    for atom in &rule.head {
        for term in &atom.terms {
            count(term, 1, &mut counts);
        }
    }
    for literal in &rule.body {
        match literal {
            Literal::Positive(atom) | Literal::Negative(atom) => {
                for term in &atom.terms {
                    count(term, 1, &mut counts);
                }
            }
            Literal::Operation(operation) => {
                for term in &operation.arguments {
                    count(term, 1, &mut counts);
                }
            }
        }
    }
    counts
}

/// Adds `weight` to every universal variable a term mentions, and two
/// to every one inside an aggregate.
fn count(term: &Term, weight: usize, counts: &mut HashMap<String, usize>) {
    match &term.kind {
        TermKind::Variable(variable) if variable.kind == VariableKind::Universal => {
            *counts.entry(variable.name.clone()).or_insert(0) += weight;
        }
        TermKind::Aggregate(_) => {
            for variable in term.variables() {
                if variable.kind == VariableKind::Universal {
                    *counts.entry(variable.name.clone()).or_insert(0) += 2;
                }
            }
        }
        _ => {
            for subterm in term.subterms() {
                count(subterm, weight, counts);
            }
        }
    }
}

/// Returns whether a rule counts its body matches with `#count()`.
///
/// Every body position then shapes the count,
/// even an anonymous one, or one whose variable is mentioned nowhere else.
pub(crate) fn counts_matches(rule: &Rule) -> bool {
    rule.head.iter().any(|atom| {
        atom.any_term(|term| {
            matches!(&term.kind, TermKind::Aggregate(aggregate) if aggregate.counts_matches())
        })
    })
}

impl ImportSurvey {
    /// Records columns of a predicate that an import's `where` clause reads,
    /// by their positions among the kept columns.
    ///
    /// Flags of another width than earlier ones belong to an import of another arity,
    /// which is reported elsewhere, and keep the predicate whole.
    fn read_by_import(&mut self, name: &str, read: Vec<bool>) {
        match self.bound.get_mut(name) {
            Some(flags) if flags.len() == read.len() => {
                for (flag, read) in flags.iter_mut().zip(read) {
                    *flag |= read;
                }
            }
            Some(_) => {
                self.conditioned.insert(name.to_owned());
            }
            None => {
                self.bound.insert(name.to_owned(), read);
            }
        }
    }
}

/// Returns which kept columns the conditions of an import's `where` clause read,
/// or `None` if the import's `format` option is no list of format names,
/// which leaves unknown which source columns are kept.
///
/// The column variables name the source columns, and a `skip` format drops a source column.
fn tested_columns(import: &ImportDirective) -> Option<Vec<bool>> {
    let mut kept = vec![true; import.variables.len()];
    for (key, value) in &import.options {
        if !matches!(&key.kind, TermKind::Constant(Identifier::Bare(name)) if name == "format") {
            continue;
        }
        let names: Vec<&Term> = match &value.kind {
            TermKind::Tuple(terms) => terms.iter().collect(),
            _ => vec![value],
        };
        if names.len() != kept.len() {
            return None;
        }
        for (keep, name) in kept.iter_mut().zip(names) {
            let TermKind::Constant(Identifier::Bare(name)) = &name.kind else {
                return None;
            };
            *keep = name != "skip";
        }
    }
    let read: HashSet<&str> = import
        .conditions
        .iter()
        .flat_map(Walk::variables)
        .filter(|variable| variable.kind == VariableKind::Universal)
        .map(|variable| variable.name.as_str())
        .collect();
    Some(
        import
            .variables
            .iter()
            .zip(&kept)
            .filter(|(_, keep)| **keep)
            .map(|(variable, _)| read.contains(variable.as_str()))
            .collect(),
    )
}
