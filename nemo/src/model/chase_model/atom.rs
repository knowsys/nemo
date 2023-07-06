use thiserror::Error;

use crate::model::{Atom, Identifier, Term, TermOperation, Variable};

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
            .terms()
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
}
