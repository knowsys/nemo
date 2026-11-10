//! The survey of a program for the import passes:
//! its rules with positional arguments, and the predicates that must be stored whole.

use std::collections::{HashMap, HashSet};

use nemo_physical::diagnostics::Diagnostic;

use crate::{
    engine::compile::translation::Translator,
    model::{
        Atom, Directive, Identifier, ImportDirective, Literal, Rule, Statement, Term, TermKind,
        Variable, VariableKind, Walk,
    },
};

/// What the import passes need to know about the statements of one translation.
pub(crate) struct ImportSurvey {
    /// The rules, with named arguments replaced by positional ones.
    ///
    /// An import whose `where` clause has atoms adds a rule
    /// whose head is the imported predicate and whose body is these atoms.
    pub(crate) rules: Vec<Rule>,
    /// The imported predicates.
    pub(crate) imported: HashSet<String>,
    /// The predicates that must be stored whole,
    /// since something other than a rule body can read them.
    ///
    /// These are the predicates of facts, rule heads, exports, `@output` and `@input`,
    /// those with rows given through [import_rows](crate::engine::Engine::import_rows)
    /// before the first load,
    /// and every predicate that the written program derives,
    /// even if a pipeline stage replaced its rules with an import.
    pub(crate) sealed: HashSet<String>,
    /// The predicates that some rule negates.
    pub(crate) negated: HashSet<String>,
    /// Per predicate of an import with a `where` clause, whether the clause reads each kept
    /// column.
    ///
    /// These columns are read whatever the rules read:
    /// the clause's atoms bind them, and its conditions test them.
    pub(crate) bound: HashMap<String, Vec<bool>>,
    /// The predicates of imports whose `where` clauses the survey cannot place among the kept
    /// columns, which are stored whole.
    ///
    /// This happens if an import's `format` option is not a list of format names,
    /// or if two imports of a predicate disagree on its arity.
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
        // So is a predicate with rows given through `import_rows`, as one with facts is.
        survey
            .sealed
            .extend(translator.imported_predicates().map(str::to_owned));
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

    /// Adds a rule, with named arguments made positional.
    ///
    /// Its head predicates are added to [sealed](Self::sealed),
    /// and its negated predicates to [negated](Self::negated).
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

    /// Adds an import directive.
    ///
    /// The columns that its `where` clause reads are recorded in [bound](Self::bound).
    /// The clause's atoms read their own predicates like the body of a rule,
    /// so they are also added as a rule whose head is the imported predicate.
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

    /// Returns whether the import passes may leave out parts of a predicate's load,
    /// which is the case if it is imported and only rule bodies read it.
    pub(crate) fn optimizable(&self, name: &str) -> bool {
        self.imported.contains(name) && !self.sealed.contains(name)
    }
}

/// Returns how often each universal variable occurs in a rule.
///
/// Every occurrence counts, in the head and the body, in atoms and conditions,
/// and inside other terms.
/// A variable inside an aggregate counts twice,
/// so that the aggregate alone makes the variable read.
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

/// Adds `weight` to the count of every universal variable that a term mentions,
/// and two for every one inside an aggregate.
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
/// Every body position then affects the count,
/// even an anonymous one or one whose variable occurs nowhere else.
pub(crate) fn counts_matches(rule: &Rule) -> bool {
    rule.head.iter().any(|atom| {
        atom.any_term(|term| {
            matches!(&term.kind, TermKind::Aggregate(aggregate) if aggregate.counts_matches())
        })
    })
}

impl ImportSurvey {
    /// Records which columns of a predicate an import's `where` clause reads,
    /// as one flag per kept column.
    ///
    /// Flags of another length than earlier ones belong to an import of another arity,
    /// which is an error of its own, and the predicate is then stored whole.
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

/// Returns, per kept column, whether the conditions of an import's `where` clause read it.
///
/// The import's variables name its source columns,
/// and a source column with the `skip` format is not kept.
/// The function returns `None` if the `format` option is not a list of format names,
/// since the kept columns are then unknown.
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
