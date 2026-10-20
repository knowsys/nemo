//! Parameters without a value, as a check of a program reads them.
//!
//! A program may leave a parameter for whoever runs it to supply, as `@parameter $limit: int .`
//! does, and a module leaves one for each `@use` of it.
//! A check looks at the program on its own and has no value for such a parameter,
//! so it reads the parameter as an input instead:
//! a one-column predicate holding any value of the parameter's declared type,
//! as an `@input` predicate holds any rows its declaration allows.
//! Every statement reading the parameter is then checked as for any value it may take.
//!
//! Before the check translates the program, its statements are rewritten:
//! - The parameter's directive becomes an `@input` of a hidden predicate,
//!   typed by the parameter's declared type, or untyped if it declares none.
//! - A rule reading the parameter reads a hidden variable instead,
//!   which an extra body atom over that predicate binds.
//! - A fact reading the parameter becomes such a rule.
//! - An import reading the parameter becomes an untyped `@input` of its predicate,
//!   since what the import would read is unknown,
//!   and an export reading it is left out.
//!
//! A parameter whose value reads a parameter without a value has none either.
//! A parameter declared twice is left as written, for translation to refuse.
//! The hidden names begin with a NUL byte, which no program can write,
//! so diagnostics and the inferred types leave them out.
//! The rewritten statements keep their ids, and their terms their source positions,
//! so diagnostics point at the program as written.

use std::collections::{HashMap, HashSet};

use crate::{
    model::{
        Atom, ColumnDeclaration, Directive, Identifier, InputDirective, Literal,
        ParameterDirective, Rule, Statement, Substitution, Term, TermKind, Variable, VariableKind,
        Walk,
    },
    pipeline::StatementId,
};

/// Rewrites the statements of a program for its check,
/// so that its parameters without a value read as inputs (see the module documentation).
///
/// `ids` are the statements' ids, in the same order;
/// the ids returned are aligned with the statements returned.
pub(in crate::engine) fn open_parameters(
    statements: Vec<Statement>,
    ids: Vec<StatementId>,
) -> (Vec<Statement>, Vec<StatementId>) {
    let unvalued = unvalued_parameters(&statements);
    if unvalued.is_empty() {
        return (statements, ids);
    }
    statements
        .into_iter()
        .zip(ids)
        .filter_map(|(statement, id)| Some((open_statement(statement, &unvalued)?, id)))
        .unzip()
}

/// Returns the names of the parameters without a value:
/// those declaring none, and those whose value reads one without a value.
/// A name declared more than once is left out.
fn unvalued_parameters(statements: &[Statement]) -> HashSet<String> {
    let mut declared: HashMap<&str, Vec<&ParameterDirective>> = HashMap::new();
    for statement in statements {
        if let Statement::Directive(Directive::Parameter(parameter)) = statement {
            declared
                .entry(parameter.variable.name.as_str())
                .or_default()
                .push(parameter);
        }
    }
    let parameters: Vec<&ParameterDirective> = declared
        .into_values()
        .filter(|directives| directives.len() == 1)
        .flatten()
        .collect();
    let mut unvalued: HashSet<String> = parameters
        .iter()
        .filter(|parameter| parameter.value.is_none())
        .map(|parameter| parameter.variable.name.clone())
        .collect();
    loop {
        let before = unvalued.len();
        for parameter in &parameters {
            if parameter
                .value
                .as_ref()
                .is_some_and(|value| reads(value, &unvalued))
            {
                unvalued.insert(parameter.variable.name.clone());
            }
        }
        if unvalued.len() == before {
            return unvalued;
        }
    }
}

/// Returns whether a component reads one of the given parameters.
fn reads(component: &impl Walk, parameters: &HashSet<String>) -> bool {
    component.any_term(|term| global(term).is_some_and(|name| parameters.contains(name)))
}

/// Returns the parameter a term is, if it is one.
fn global(term: &Term) -> Option<&str> {
    match &term.kind {
        TermKind::Variable(variable) if variable.kind == VariableKind::Global => {
            Some(&variable.name)
        }
        _ => None,
    }
}

/// Returns a statement as the check reads it, or `None` if the check leaves it out.
fn open_statement(statement: Statement, unvalued: &HashSet<String>) -> Option<Statement> {
    match statement {
        Statement::Directive(Directive::Parameter(parameter))
            if unvalued.contains(&parameter.variable.name) =>
        {
            Some(Statement::Directive(Directive::Input(InputDirective {
                predicate: hidden_predicate(&parameter.variable.name),
                columns: parameter.datatype.map(|datatype| {
                    vec![ColumnDeclaration {
                        name: None,
                        datatype,
                    }]
                }),
                span: parameter.span,
            })))
        }
        statement if !reads(&statement, unvalued) => Some(statement),
        Statement::Rule(rule) => Some(Statement::Rule(open_rule(rule, unvalued))),
        Statement::Fact(fact) => {
            let rule = Rule {
                attributes: Vec::new(),
                head: vec![Atom {
                    predicate: fact.predicate,
                    terms: fact.terms,
                    labels: fact.labels,
                    span: fact.span,
                }],
                body: Vec::new(),
                span: fact.span,
            };
            Some(Statement::Rule(open_rule(rule, unvalued)))
        }
        Statement::Directive(Directive::Import(import)) => {
            Some(Statement::Directive(Directive::Input(InputDirective {
                predicate: import.predicate,
                columns: None,
                span: import.span,
            })))
        }
        Statement::Directive(Directive::Export(_)) => None,
        // A `@use` or an `@define` reading the parameter is left to translation.
        statement => Some(statement),
    }
}

/// Returns a rule reading hidden variables in place of the parameters without a value,
/// each bound by an extra body atom over the parameter's hidden predicate.
fn open_rule(mut rule: Rule, unvalued: &HashSet<String>) -> Rule {
    let mut read: Vec<String> = Vec::new();
    rule.walk(&mut |term| {
        if let Some(name) = global(term)
            && unvalued.contains(name)
            && !read.iter().any(|known| known == name)
        {
            read.push(name.to_owned());
        }
    });
    let substitution: Substitution = read
        .iter()
        .map(|name| (Variable::global(name.as_str()), hidden_variable(name)))
        .collect();
    substitution.apply(&mut rule);
    rule.body.extend(read.iter().map(|name| {
        Literal::Positive(Atom {
            predicate: hidden_predicate(name),
            terms: vec![hidden_variable(name)],
            labels: None,
            span: None,
        })
    }));
    rule
}

/// Returns the hidden predicate a parameter without a value reads as.
fn hidden_predicate(name: &str) -> Identifier {
    Identifier::Iri(format!("\u{0}${name}"))
}

/// Returns the hidden variable a rule reads a parameter without a value as.
fn hidden_variable(name: &str) -> Term {
    Term::new(TermKind::Variable(Variable::universal(format!(
        "\u{0}${name}"
    ))))
}
