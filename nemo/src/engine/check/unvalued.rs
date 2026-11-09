//! Parameters without a value, as a check of a program treats them.
//!
//! A program may leave a parameter for whoever runs it to supply, as `@parameter $limit: int .`
//! does, and a module leaves one for each `@use` of it.
//! A check looks at the program on its own and has no value for such a parameter,
//! so it treats the parameter as an input instead:
//! a one-column predicate that may contain any value of the parameter's declared type,
//! as an `@input` predicate may contain any rows its declaration allows.
//! Every statement that reads the parameter is then checked as for any value it may take.
//! Before the check translates the program, [rewrite_unvalued] rewrites its statements
//! to this end, with *hidden* predicates and variables.
//!
//! The hidden names begin with a NUL byte, which no program can write,
//! and the places that name a program's variables and predicates leave them out:
//! the compiler's `source_names` and `written_name`, and `InferredTypes::of_check`.

use std::collections::{HashMap, HashSet};

use crate::{
    model::{
        Atom, ColumnDeclaration, Directive, Identifier, InputDirective, Literal,
        ParameterDirective, Rule, Statement, Substitution, Term, TermKind, Variable, VariableKind,
        Walk,
    },
    pipeline::StatementId,
};

/// Rewrites the statements of a program for its check, each with its id,
/// so that its parameters without a value are treated as inputs.
///
/// - The parameter's directive becomes an `@input` of a hidden predicate,
///   typed by the parameter's declared type, or untyped if it declares none.
/// - A rule that reads the parameter reads a hidden variable instead,
///   which an extra body atom over that predicate binds.
/// - A fact that reads the parameter becomes such a rule.
/// - An import that reads the parameter becomes an untyped `@input` of its predicate,
///   since what the import would read is unknown.
///   The other imports into that predicate are left out,
///   since the input may already contain any value.
/// - An export that reads the parameter is left out.
///
/// A parameter whose value reads a parameter without a value has none either.
/// A parameter declared twice is left as written, for translation to refuse.
/// The rewritten statements keep their ids, and their terms their source positions,
/// so that diagnostics point at the program as written.
pub(in crate::engine) fn rewrite_unvalued(
    statements: Vec<(StatementId, Statement)>,
) -> Vec<(StatementId, Statement)> {
    let unvalued = unvalued_parameters(&statements);
    if unvalued.is_empty() {
        return statements;
    }
    let as_inputs = input_predicates(&statements, &unvalued);
    let mut given = HashSet::new();
    statements
        .into_iter()
        .filter_map(|(id, statement)| {
            let rewritten = rewrite_statement(statement, &unvalued, &as_inputs, &mut given)?;
            Some((id, rewritten))
        })
        .collect()
}

/// Returns the predicates of the imports that read a parameter without a value,
/// which the check treats as inputs.
fn input_predicates(
    statements: &[(StatementId, Statement)],
    unvalued: &HashSet<String>,
) -> HashSet<Identifier> {
    statements
        .iter()
        .filter_map(|(_, statement)| match statement {
            Statement::Directive(Directive::Import(import)) if reads(statement, unvalued) => {
                Some(import.predicate.clone())
            }
            _ => None,
        })
        .collect()
}

/// Returns the names of the parameters without a value:
/// those that declare none, and those whose value reads one without a value.
///
/// A name declared more than once is left out.
fn unvalued_parameters(statements: &[(StatementId, Statement)]) -> HashSet<String> {
    let mut declared: HashMap<&str, Vec<&ParameterDirective>> = HashMap::new();
    for (_, statement) in statements {
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

/// Returns the name of the parameter that a term is, if it is one.
fn global(term: &Term) -> Option<&str> {
    match &term.kind {
        TermKind::Variable(variable) if variable.kind == VariableKind::Global => {
            Some(&variable.name)
        }
        _ => None,
    }
}

/// Returns a statement as the check reads it, or `None` if the check leaves it out.
///
/// `as_inputs` are the predicates the check treats as inputs (see [input_predicates]),
/// and `given` those given their `@input` so far.
fn rewrite_statement(
    statement: Statement,
    unvalued: &HashSet<String>,
    as_inputs: &HashSet<Identifier>,
    given: &mut HashSet<Identifier>,
) -> Option<Statement> {
    match statement {
        Statement::Directive(Directive::Import(import))
            if as_inputs.contains(&import.predicate) =>
        {
            let first = given.insert(import.predicate.clone());
            first.then_some(Statement::Directive(Directive::Input(InputDirective {
                predicate: import.predicate,
                columns: None,
                span: import.span,
            })))
        }
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
        Statement::Rule(rule) => Some(Statement::Rule(rewrite_rule(rule, unvalued))),
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
            Some(Statement::Rule(rewrite_rule(rule, unvalued)))
        }
        Statement::Directive(Directive::Export(_)) => None,
        // Any other statement that reads the parameter is an `@define`, which is kept for
        // translation to ignore with a warning, as a load does.
        // Linking expanded every `@use`, so none is left.
        statement => Some(statement),
    }
}

/// Returns a rule that reads hidden variables in place of the parameters without a value,
/// each bound by an extra body atom over the parameter's hidden predicate.
fn rewrite_rule(mut rule: Rule, unvalued: &HashSet<String>) -> Rule {
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

/// Returns the hidden predicate that replaces a parameter without a value, `\0$name`.
///
/// The NUL byte hides it, and the `$` keeps it apart from the compiler's hidden names,
/// such as `\0unit` from a parameter named `unit`.
fn hidden_predicate(name: &str) -> Identifier {
    Identifier::Iri(format!("\u{0}${name}"))
}

/// Returns the hidden variable through which a rule reads a parameter without a value,
/// `\0$name`.
///
/// The NUL byte hides it, and the `$` keeps it apart from the compiler's hidden variables,
/// such as `\0expression0` from a parameter named `expression0`.
fn hidden_variable(name: &str) -> Term {
    Term::new(TermKind::Variable(Variable::universal(format!(
        "\u{0}${name}"
    ))))
}
