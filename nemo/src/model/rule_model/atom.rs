use super::{Aggregate, Identifier, Term, Variable};

/// An atom.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct Atom {
    /// The predicate.
    predicate: Identifier,
    /// The terms.
    terms: Vec<Term>,
}

impl Atom {
    /// Construct a new Atom.
    pub fn new(predicate: Identifier, terms: Vec<Term>) -> Self {
        Self { predicate, terms }
    }

    /// Return the predicate [`Identifier`].
    #[must_use]
    pub fn predicate(&self) -> Identifier {
        self.predicate.clone()
    }

    /// Return the terms trees in the atom - immutable.
    #[must_use]
    pub fn term_trees(&self) -> &Vec<Term> {
        &self.terms
    }

    /// Return the terms trees in the atom - mutable.
    #[must_use]
    pub fn terms_trees_mut(&mut self) -> &mut Vec<Term> {
        &mut self.terms
    }

    /// Return all variables in the atom.
    pub fn variables(&self) -> impl Iterator<Item = &Variable> {
        self.terms.iter().flat_map(|t| t.variables())
    }

    /// Return all universally quantified variables in the atom.
    pub fn universal_variables(&self) -> impl Iterator<Item = &Variable> {
        self.variables()
            .filter(|var| matches!(var, Variable::Universal(_)))
    }

    /// Return all existentially quantified variables in the atom.
    pub fn existential_variables(&self) -> impl Iterator<Item = &Variable> {
        self.variables()
            .filter(|var| matches!(var, Variable::Existential(_)))
    }

    /// Return all aggregate in the atom.
    pub fn aggregates(&self) -> impl Iterator<Item = &Aggregate> + '_ {
        self.terms.iter().flat_map(|t| t.aggregates())
    }
}
