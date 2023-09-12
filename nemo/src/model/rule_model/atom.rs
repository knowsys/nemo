use super::{Aggregate, Identifier, Term, TermTree, Variable};

/// An atom.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct Atom {
    /// The predicate.
    predicate: Identifier,
    /// The terms.
    term_trees: Vec<TermTree>,
}

impl Atom {
    /// Construct a new Atom.
    pub fn new(predicate: Identifier, terms: Vec<TermTree>) -> Self {
        Self {
            predicate,
            term_trees: terms,
        }
    }

    /// Return the predicate [`Identifier`].
    #[must_use]
    pub fn predicate(&self) -> Identifier {
        self.predicate.clone()
    }

    /// Return the terms trees in the atom - immutable.
    #[must_use]
    pub fn term_trees(&self) -> &Vec<TermTree> {
        &self.term_trees
    }

    /// Return the terms trees in the atom - mutable.
    #[must_use]
    pub fn terms_trees_mut(&mut self) -> &mut Vec<TermTree> {
        &mut self.term_trees
    }

    /// Returns all terms at the leave of the term trees of the atom.
    pub fn terms(&self) -> impl Iterator<Item = &Term> {
        self.term_trees.iter().flat_map(|t| t.terms())
    }

    /// Return all variables in the atom.
    pub fn variables(&self) -> impl Iterator<Item = &Variable> {
        self.terms().filter_map(|term| match term {
            Term::Variable(var) => Some(var),
            _ => None,
        })
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
        self.terms().filter_map(|term| match term {
            Term::Aggregate(aggregate) => Some(aggregate),
            _ => None,
        })
    }

    /// Replace one [`Term`] with another.
    pub fn replace_term(&mut self, old: &Term, new: &Term) {
        for tree in &mut self.term_trees {
            tree.replace_term(old, new);
        }
    }
}

impl std::fmt::Display for Atom {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.predicate.fmt(f)?;
        f.write_str("(")?;
        for (index, tree) in self.terms().enumerate() {
            tree.fmt(f)?;
            if index < self.term_trees.len() - 1 {
                f.write_str(", ")?;
            }
        }
        f.write_str(")")
    }
}
