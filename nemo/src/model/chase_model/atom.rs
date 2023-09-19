use std::collections::HashSet;

use thiserror::Error;

use crate::model::{Atom, Filter, FilterOperation, Identifier, Term, TermOperation, Variable};

/// Representation of an atom used in [`super::ChaseRule`].
#[derive(Debug, Clone)]
pub struct ChaseAtom {
    predicate: Identifier,
    terms: Vec<Term>,
}

/// Errors than can occur during rule translation
#[derive(Error, Debug, Copy, Clone)]
#[allow(clippy::enum_variant_names)]
pub enum RuleTranslationError {
    /// Arithmetic operation in body
    #[error("Arithmetic operations are currently not allowed in the body of a rule.")]
    UnsupportedFeatureBodyArithmetic,
}

impl ChaseAtom {
    /// Construct a new Atom.
    pub fn new(predicate: Identifier, terms: Vec<Term>) -> Self {
        Self { predicate, terms }
    }

    /// Construct a [`ChaseAtom`] from an [`Atom`] that does not contain term trees that are not leaves.
    pub fn from_flat_atom(atom: Atom) -> Result<Self, RuleTranslationError> {
        let terms: Vec<Term> = atom
            .term_trees()
            .iter()
            .map(|t| {
                if let TermOperation::Term(term) = t.operation() {
                    Ok(term.clone())
                } else {
                    Err(RuleTranslationError::UnsupportedFeatureBodyArithmetic)
                }
            })
            .collect::<Result<Vec<Term>, RuleTranslationError>>()?;

        Ok(Self {
            predicate: atom.predicate(),
            terms,
        })
    }

    /// Converts atom into normalized form, i.e. a(x1, x2, ...) with all x
    /// being *distinct* variables.
    /// This means replacing duplicate variables and constants with newly
    /// generated variables and introducing corresponding constraints.
    pub fn normalize(
        &mut self,
        generate_variable: &mut impl FnMut() -> Variable,
        constraints: &mut impl Extend<Filter>,
    ) {
        let mut seen_variables = HashSet::new();

        for term in self.terms_mut() {
            if let Term::Variable(variable) = term {
                if !seen_variables.insert(variable.clone()) {
                    let fresh_variable = generate_variable();
                    constraints.extend(Some(Filter {
                        operation: FilterOperation::Equals,
                        lhs: fresh_variable.clone(),
                        rhs: std::mem::replace(term, Term::Variable(fresh_variable)),
                    }));
                }
            } else {
                let fresh_variable = generate_variable();
                constraints.extend(Some(Filter {
                    operation: FilterOperation::Equals,
                    lhs: fresh_variable.clone(),
                    rhs: std::mem::replace(term, Term::Variable(fresh_variable)),
                }))
            }
        }
    }

    /// Return the predicate [`Identifier`].
    #[must_use]
    pub fn predicate(&self) -> Identifier {
        self.predicate.clone()
    }

    /// Return the terms in the atom - immutable.
    #[must_use]
    pub fn terms(&self) -> &Vec<Term> {
        &self.terms
    }

    /// Return the terms in the atom - mutable.
    #[must_use]
    pub fn terms_mut(&mut self) -> &mut Vec<Term> {
        &mut self.terms
    }

    /// Return all variables in the atom.
    pub fn variables(&self) -> impl Iterator<Item = &Variable> + '_ {
        self.terms().iter().filter_map(|term| match term {
            Term::Variable(var) => Some(var),
            _ => None,
        })
    }

    /// Return all universally quantified variables in the atom.
    pub fn universal_variables(&self) -> impl Iterator<Item = &Variable> + '_ {
        self.variables()
            .filter(|var| matches!(var, Variable::Universal(_)))
    }

    /// Return all existentially quantified variables in the atom.
    pub fn existential_variables(&self) -> impl Iterator<Item = &Variable> + '_ {
        self.variables()
            .filter(|var| matches!(var, Variable::Existential(_)))
    }

    /// Substitutes all occurrences of `variable` with `subst`.
    pub fn substitute_variable(&mut self, variable: &Variable, subst: &Variable) {
        for term in &mut self.terms {
            term.substitute_variable(variable, subst)
        }
    }
}

impl std::fmt::Display for ChaseAtom {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.predicate.fmt(f)?;
        f.write_str("(")?;
        for (index, term) in self.terms().iter().enumerate() {
            term.fmt(f)?;
            if index < self.terms.len() - 1 {
                f.write_str(", ")?;
            }
        }
        f.write_str(")")
    }
}
