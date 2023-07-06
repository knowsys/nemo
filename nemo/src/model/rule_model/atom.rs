use super::{Identifier, Term, TermTree, Variable};

/// An atom.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct Atom {
    /// The predicate.
    predicate: Identifier,
    /// The terms.
    terms: Vec<TermTree>,
}

impl Atom {
    /// Construct a new Atom.
    pub fn new(predicate: Identifier, terms: Vec<TermTree>) -> Self {
        Self { predicate, terms }
    }

    /// Return the predicate [`Identifier`].
    #[must_use]
    pub fn predicate(&self) -> Identifier {
        self.predicate.clone()
    }

    /// Return the terms in the atom - immutable.
    #[must_use]
    pub fn terms(&self) -> &Vec<TermTree> {
        &self.terms
    }

    /// Return the terms in the atom - mutable.
    #[must_use]
    pub fn terms_mut(&mut self) -> &mut Vec<TermTree> {
        &mut self.terms
    }

    /// Return all variables in the atom.
    pub fn variables(&self) -> impl Iterator<Item = &Variable> + '_ {
        self.terms
            .iter()
            .flat_map(|t| t.terms())
            .filter_map(|term| match term {
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
