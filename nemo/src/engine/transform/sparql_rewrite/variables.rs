//! The variables of rules and terms, as the rewrite reads them:
//! which universal variables a term, a literal, or a rule mentions,
//! and helpers over lists of variable names.

use std::collections::{HashMap, HashSet};

use crate::model::{Rule, Term, Variable, VariableKind, Walk};

/// Returns the universal variables a rule, literal or term mentions, nested terms included.
pub(super) fn universal(component: &impl Walk) -> Vec<String> {
    component
        .variables()
        .into_iter()
        .filter(|variable| variable.kind == VariableKind::Universal)
        .map(|variable| variable.name.clone())
        .collect()
}

/// Returns every universal variable a rule mentions.
pub(super) fn universal_variables(rule: &Rule) -> HashSet<String> {
    universal(rule).into_iter().collect()
}

/// Returns the universal variables a rule joins on: those more than one body literal mentions.
pub(super) fn joined_variables(rule: &Rule) -> HashSet<String> {
    let mut counts: HashMap<String, usize> = HashMap::new();
    for literal in &rule.body {
        let mut seen: HashSet<String> = HashSet::new();
        for variable in universal(literal) {
            if seen.insert(variable.clone()) {
                *counts.entry(variable).or_default() += 1;
            }
        }
    }
    counts
        .into_iter()
        .filter(|(_, count)| *count > 1)
        .map(|(name, _)| name)
        .collect()
}

/// Returns whether two variable lists share a name.
pub(super) fn shares(left: &[String], right: &[String]) -> bool {
    left.iter().any(|variable| right.contains(variable))
}

/// Adds the variables to a list, in first-occurrence order.
pub(super) fn extend_unique(variables: &mut Vec<String>, more: impl IntoIterator<Item = String>) {
    for variable in more {
        if !variables.contains(&variable) {
            variables.push(variable);
        }
    }
}

/// Returns variable terms for names.
pub(super) fn variable_terms(names: &[String]) -> Vec<Term> {
    names
        .iter()
        .map(|name| Term::variable(Variable::universal(name)))
        .collect()
}

/// Returns a variable name private to one atom instance, a stem and a number,
/// different from every variable the rule mentions.
pub(super) fn private_name(
    rule_variables: &HashSet<String>,
    stem: &str,
    counter: &mut usize,
) -> String {
    loop {
        *counter += 1;
        let name = format!("{stem}{counter}");
        if !rule_variables.contains(&name) {
            return name;
        }
    }
}
