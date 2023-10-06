use crate::model::{Atom, Identifier, PrimitiveTerm, Term, Variable};

/// An atom which may only use [`PrimitiveTerm`]s
#[derive(Debug, Clone)]
pub struct PrimitiveAtom {
    predicate: Identifier,
    terms: Vec<PrimitiveTerm>,
}

impl PrimitiveAtom {
    /// Construct a new [`PrimitiveAtom`].
    pub fn new(predicate: Identifier, terms: Vec<PrimitiveTerm>) -> Self {
        Self { predicate, terms }
    }

    /// Construct a [`PrimitiveAtom`] from an [`Atom`].
    ///
    /// # Panics
    /// Panics if the provided atom contains complex terms.
    pub fn from_flat_atom(atom: &Atom) -> Self {
        Self {
            predicate: atom.predicate(),
            terms: atom
                .terms()
                .iter()
                .map(|t| {
                    t.as_primitive()
                        .expect("Function assumes that input atom only contains primitive terms.")
                })
                .collect(),
        }
    }

    /// Return the predicate [`Identifier`].
    #[must_use]
    pub fn predicate(&self) -> Identifier {
        self.predicate.clone()
    }

    /// Return the terms in the atom - immutable.
    #[must_use]
    pub fn terms(&self) -> &Vec<PrimitiveTerm> {
        &self.terms
    }

    /// Return the terms in the atom - mutable.
    #[must_use]
    pub fn terms_mut(&mut self) -> &mut Vec<PrimitiveTerm> {
        &mut self.terms
    }
}

impl std::fmt::Display for PrimitiveAtom {
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

/// An atom which may only use [`Variable`]s.
#[derive(Debug, Clone)]
pub struct VariableAtom {
    predicate: Identifier,
    variables: Vec<Variable>,
}

impl VariableAtom {
    /// Construct a new Atom.
    pub fn new(predicate: Identifier, variables: Vec<Variable>) -> Self {
        Self {
            predicate,
            variables,
        }
    }

    /// Construct a [`VariableAtom`] from an [`Atom`].
    ///
    /// # Panics
    /// Panics if the provided atom contains terms that are not variables.
    pub fn from_flat_atom(atom: &Atom) -> Self {
        Self {
            predicate: atom.predicate(),
            variables: atom
                .terms()
                .iter()
                .map(|t| {
                    if let Term::Primitive(PrimitiveTerm::Variable(variable)) = t {
                        variable.clone()
                    } else {
                        unreachable!(
                            "Function assumes that input atom only contains primitive terms."
                        )
                    }
                })
                .collect(),
        }
    }

    /// Return the predicate [`Identifier`].
    #[must_use]
    pub fn predicate(&self) -> Identifier {
        self.predicate.clone()
    }

    /// Return the variables in the atom - immutable.
    #[must_use]
    pub fn variables(&self) -> &Vec<Variable> {
        &self.variables
    }

    /// Return the variables in the atom - mutable.
    #[must_use]
    pub fn variables_mut(&mut self) -> &mut Vec<Variable> {
        &mut self.variables
    }
}

impl std::fmt::Display for VariableAtom {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.predicate.fmt(f)?;
        f.write_str("(")?;
        for (index, term) in self.variables().iter().enumerate() {
            term.fmt(f)?;
            if index < self.variables.len() - 1 {
                f.write_str(", ")?;
            }
        }
        f.write_str(")")
    }
}
