use std::fmt::Display;

use crate::model::{Atom, Constant, Identifier, PrimitiveTerm, Term, Variable};

/// An atom used within a [`super::ChaseRule`]
pub trait ChaseAtom {
    /// Type of the terms within the atom.
    type TypeTerm;

    /// Return the predicate [`Identifier`].
    fn predicate(&self) -> Identifier;

    /// Return the terms in the atom - immutable.
    fn terms(&self) -> &Vec<Self::TypeTerm>;

    /// Return the terms in the atom - mutable.
    fn terms_mut(&mut self) -> &mut Vec<Self::TypeTerm>;

    /// Return the arity of the atom
    fn arity(&self) -> usize {
        self.terms().len()
    }

    /// Return a set of all variables used in this atom
    fn get_variables(&self) -> Vec<Variable>;
}

impl<T: Display> Display for dyn ChaseAtom<TypeTerm = T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.predicate().fmt(f)?;
        f.write_str("(")?;
        for (index, term) in self.terms().iter().enumerate() {
            term.fmt(f)?;
            if index < self.terms().len() - 1 {
                f.write_str(", ")?;
            }
        }
        f.write_str(")")
    }
}

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
}

impl ChaseAtom for PrimitiveAtom {
    type TypeTerm = PrimitiveTerm;

    /// Return the predicate [`Identifier`].
    fn predicate(&self) -> Identifier {
        self.predicate.clone()
    }

    /// Return the terms in the atom - immutable.
    fn terms(&self) -> &Vec<PrimitiveTerm> {
        &self.terms
    }

    /// Return the terms in the atom - mutable.
    fn terms_mut(&mut self) -> &mut Vec<PrimitiveTerm> {
        &mut self.terms
    }

    /// Return a set of all variables used in this atom
    fn get_variables(&self) -> Vec<Variable> {
        self.terms
            .iter()
            .filter_map(|t| {
                if let PrimitiveTerm::Variable(v) = t {
                    Some(v.clone())
                } else {
                    None
                }
            })
            .collect()
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
                        unreachable!("Function assumes that input atom only contains variables.")
                    }
                })
                .collect(),
        }
    }
}

impl From<VariableAtom> for PrimitiveAtom {
    fn from(atom: VariableAtom) -> Self {
        Self {
            predicate: atom.predicate,
            terms: atom
                .variables
                .into_iter()
                .map(PrimitiveTerm::Variable)
                .collect(),
        }
    }
}

impl ChaseAtom for VariableAtom {
    type TypeTerm = Variable;

    /// Return the predicate [`Identifier`].
    fn predicate(&self) -> Identifier {
        self.predicate.clone()
    }

    /// Return the variables in the atom - immutable.
    fn terms(&self) -> &Vec<Variable> {
        &self.variables
    }

    /// Return the variables in the atom - mutable.
    fn terms_mut(&mut self) -> &mut Vec<Variable> {
        &mut self.variables
    }

    /// Return a set of all variables used in this atom
    fn get_variables(&self) -> Vec<Variable> {
        self.terms().to_vec()
    }
}

/// An atom which may only contain [`Constant`]s.
#[derive(Debug, Clone)]
pub struct ChaseFact {
    predicate: Identifier,
    constants: Vec<Constant>,
}

impl ChaseFact {
    /// Create a new [`ChaseFact`].
    pub fn new(predicate: Identifier, constants: Vec<Constant>) -> Self {
        Self {
            predicate,
            constants,
        }
    }

    /// Construct a [`ChaseFact`] from an [`Atom`].
    ///
    /// # Panics
    /// Panics if the provided atom contains complex terms.
    pub fn from_flat_atom(atom: &Atom) -> Self {
        Self {
            predicate: atom.predicate(),
            constants: atom
                .terms()
                .iter()
                .map(|t| {
                    if let Term::Primitive(PrimitiveTerm::Constant(constant)) = t {
                        constant.clone()
                    } else {
                        unreachable!("Function assumes that input atom only contains constants.")
                    }
                })
                .collect(),
        }
    }
}

impl ChaseAtom for ChaseFact {
    type TypeTerm = Constant;

    fn predicate(&self) -> Identifier {
        self.predicate.clone()
    }

    fn get_variables(&self) -> Vec<Variable> {
        vec![]
    }

    fn terms(&self) -> &Vec<Constant> {
        &self.constants
    }

    fn terms_mut(&mut self) -> &mut Vec<Constant> {
        &mut self.constants
    }
}

impl Display for ChaseFact {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.predicate().fmt(f)?;
        f.write_str("(")?;
        for (index, term) in self.terms().iter().enumerate() {
            term.fmt(f)?;
            if index < self.terms().len() - 1 {
                f.write_str(", ")?;
            }
        }
        f.write_str(")")
    }
}
