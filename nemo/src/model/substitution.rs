//! Substitutions: variables replaced by terms.

use std::collections::HashMap;

use crate::model::{Term, TermKind, Variable, Walk};

/// A substitution: the terms that replace variables.
///
/// Applied to a component, it replaces every occurrence of a variable it maps by that
/// variable's term, which takes the span of the occurrence it replaces.
/// A variable an aggregate's scope names is renamed if it maps to a variable,
/// and kept otherwise.
///
/// # Example
///
/// ```
/// use nemo::model::{Rule, Substitution, Term, Variable};
///
/// let mut rule = Rule::parse("p(?x) :- q(?x, ?y) .").expect("the rule parses");
/// let mut substitution = Substitution::new();
/// substitution.insert(Variable::universal("y"), Term::integer(5));
/// substitution.apply(&mut rule);
/// assert_eq!(rule.to_string(), "p(?x) :- q(?x, 5) .");
/// ```
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Substitution {
    /// The term replacing each mapped variable.
    replacements: HashMap<Variable, Term>,
}

impl Substitution {
    /// Creates the empty substitution.
    pub fn new() -> Self {
        Self::default()
    }

    /// Maps a variable to a term, returning the term it was mapped to
    /// before.
    pub fn insert(&mut self, variable: Variable, term: Term) -> Option<Term> {
        self.replacements.insert(variable, term)
    }

    /// Returns the term a variable is mapped to.
    pub fn get(&self, variable: &Variable) -> Option<&Term> {
        self.replacements.get(variable)
    }

    /// Returns whether the substitution maps no variable.
    pub fn is_empty(&self) -> bool {
        self.replacements.is_empty()
    }

    /// Returns the mapped variables with their terms, in no particular
    /// order.
    pub fn iter(&self) -> impl Iterator<Item = (&Variable, &Term)> {
        self.replacements.iter()
    }

    /// Replaces the mapped variables of a component.
    pub fn apply(&self, component: &mut impl Walk) {
        if self.replacements.is_empty() {
            return;
        }
        component.walk_mut(&mut |term| match &mut term.kind {
            TermKind::Variable(variable) => {
                if let Some(replacement) = self.replacements.get(variable) {
                    let span = term.span;
                    *term = replacement.clone();
                    term.span = span.or(term.span);
                }
            }
            TermKind::Aggregate(aggregate) => {
                for variable in &mut aggregate.scope {
                    if let Some(Term {
                        kind: TermKind::Variable(renamed),
                        ..
                    }) = self.replacements.get(variable)
                    {
                        *variable = renamed.clone();
                    }
                }
            }
            _ => {}
        });
    }
}

impl FromIterator<(Variable, Term)> for Substitution {
    fn from_iter<I: IntoIterator<Item = (Variable, Term)>>(pairs: I) -> Self {
        Self {
            replacements: pairs.into_iter().collect(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::model::Rule;

    #[test]
    fn substitution_replaces_every_occurrence_once() {
        let mut rule =
            Rule::parse("p(?x, #count(?y per ?x)) :- q(?x, f(?x)) .").expect("the rule parses");
        let substitution: Substitution = [
            (
                Variable::universal("x"),
                Term::variable(Variable::universal("z")),
            ),
            (
                Variable::universal("y"),
                Term::variable(Variable::universal("x")),
            ),
        ]
        .into_iter()
        .collect();
        substitution.apply(&mut rule);
        assert_eq!(
            rule.to_string(),
            "p(?z, #count(?x per ?z)) :- q(?z, f(?z)) ."
        );
    }

    #[test]
    fn replacements_take_the_span_of_the_occurrence() {
        let mut term = Term::parse("?x").expect("the term parses");
        let occurrence = term.span;
        let substitution: Substitution = [(Variable::universal("x"), Term::integer(1))]
            .into_iter()
            .collect();
        substitution.apply(&mut term);
        assert_eq!(term, Term::integer(1));
        assert_eq!(term.span, occurrence);
    }
}
