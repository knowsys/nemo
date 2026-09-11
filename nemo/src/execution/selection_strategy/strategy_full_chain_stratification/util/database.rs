use std::collections::{HashMap, HashSet};

use crate::execution::selection_strategy::strategy_full_chain_stratification::chain::atoms::{
    Atom, Constant, Predicate, Var,
};
use crate::execution::selection_strategy::strategy_full_chain_stratification::chain::substitution::Substitution;

/// The resolved value of a term slot: either it turned out to be bound to a constant, or it is
/// still a (possibly still-free) variable.
#[derive(Eq, PartialEq, Hash, Debug, Clone, Copy)]
pub(crate) enum Value {
    Constant(Constant),
    Variable(Var),
}

#[derive(Eq, PartialEq, Hash, Debug, Clone)]
pub(crate) struct RepresentativeAtom {
    predicate: Predicate,
    values: Vec<Value>,
}

impl RepresentativeAtom {
    pub(crate) fn predicate(&self) -> Predicate {
        self.predicate
    }

    pub(crate) fn values(&self) -> &[Value] {
        &self.values
    }

    pub(crate) fn variables(&self) -> impl Iterator<Item = Var> + '_ {
        self.values.iter().filter_map(|v| match v {
            Value::Variable(x) => Some(*x),
            Value::Constant(_) => None,
        })
    }

    /// Resolve `atom`'s terms w.r.t. `eta`, and classify each resolved id as constant or variable
    /// via `consts` (which must cover every rule whose variable ids `atom` or `eta`'s range may
    /// reference, e.g. via [`super::super::chain::atoms::combined_consts`]).
    pub(crate) fn from_atom_with_substitution(
        eta: &Substitution,
        consts: &HashMap<Var, Constant>,
        atom: &Atom,
    ) -> Self {
        Self {
            predicate: atom.predicate(),
            values: atom
                .terms()
                .iter()
                .map(|&t| {
                    let resolved = eta.resolve(t);
                    match consts.get(&resolved) {
                        Some(&c) => Value::Constant(c),
                        None => Value::Variable(resolved),
                    }
                })
                .collect(),
        }
    }

    pub(crate) fn substitute_atoms<'a>(
        eta: &'a Substitution,
        consts: &'a HashMap<Var, Constant>,
        atoms: impl IntoIterator<Item = &'a Atom> + 'a,
    ) -> impl Iterator<Item = Self> + 'a {
        atoms
            .into_iter()
            .map(move |atom| Self::from_atom_with_substitution(eta, consts, atom))
    }
}

#[derive(Debug)]
pub(crate) struct RepresentativeDatabase(HashMap<Predicate, HashSet<Vec<Value>>>);

impl RepresentativeDatabase {
    pub(crate) fn new<'a>(facts: impl IntoIterator<Item = &'a RepresentativeAtom>) -> Self {
        Self(HashMap::new()).add_facts(facts)
    }

    pub(crate) fn add_facts<'a>(
        mut self,
        facts: impl IntoIterator<Item = &'a RepresentativeAtom>,
    ) -> Self {
        // here, variables are allowed in the database with the understanding that they are replaced with fresh variables injectively
        for fact in facts.into_iter() {
            self.0
                .entry(fact.predicate())
                .or_default()
                .insert(fact.values().to_vec());
        }
        self
    }

    pub(crate) fn entails<'a>(
        &self,
        existentials: &HashSet<Var>,
        atoms: impl IntoIterator<Item = &'a RepresentativeAtom>,
    ) -> bool {
        // we assume that all but the existential variables are actually constants (or otherwise fixed)
        let atoms = atoms.into_iter().collect::<Vec<_>>();

        // first, check that all mentioned predicates are present in the database
        let mut atom_pred_extend = Vec::with_capacity(atoms.len());
        for &atom in &atoms {
            if let Some(pred_extend) = self.0.get(&atom.predicate()) {
                atom_pred_extend.push(pred_extend);
            } else {
                return false;
            }
        }

        // then restrict the predicate to only those facts where non-existential values match
        let mut atom_facts = Vec::with_capacity(atoms.len());

        for (i, &atom) in atoms.iter().enumerate() {
            let options = atom_pred_extend[i]
                .iter()
                .filter(|fact| {
                    atom.values()
                        .iter()
                        .enumerate()
                        .filter(|(_, value)| match value {
                            Value::Variable(var) => !existentials.contains(var),
                            Value::Constant(_) => true,
                        })
                        .all(|(k, value)| &fact[k] == value)
                })
                .collect::<HashSet<_>>();
            if options.is_empty() {
                return false;
            }
            atom_facts.push(options);
        }

        let mut existentials_map = HashMap::new();

        fn is_entailed<'a>(
            remaining_atoms: &'a [&'a RepresentativeAtom],
            atom_facts: &'a [HashSet<&'a Vec<Value>>],
            existentials_map: &mut HashMap<Var, Value>,
        ) -> bool {
            if remaining_atoms.is_empty() {
                return true;
            }

            let atom = &remaining_atoms[0];
            let options = &atom_facts[0];

            for option in options.iter() {
                let mut inserted_terms = Vec::new();
                if atom.values().iter().enumerate().all(|(k, value)| {
                    if let Value::Variable(var) = value {
                        match existentials_map.get(var) {
                            Some(val) => *val == option[k],
                            None => {
                                existentials_map.insert(*var, option[k]);
                                inserted_terms.push(*var);
                                true
                            }
                        }
                    } else {
                        true
                    }
                }) && is_entailed(&remaining_atoms[1..], &atom_facts[1..], existentials_map)
                {
                    return true;
                }
                for t in &inserted_terms {
                    existentials_map.remove(t);
                }
            }
            false
        }

        is_entailed(&atoms[..], &atom_facts[..], &mut existentials_map)
    }

    pub(crate) fn contains<'a>(
        &self,
        facts: impl IntoIterator<Item = &'a RepresentativeAtom>,
    ) -> bool {
        facts.into_iter().all(|fact| {
            self.0
                .get(&fact.predicate())
                .is_some_and(|extent| extent.contains(fact.values()))
        })
    }

    /// Debug-format a set of facts (for `log::trace!` call sites); `db`, if given, restricts the
    /// printed facts to those already contained in it.
    pub(crate) fn display<'a>(
        facts: impl IntoIterator<Item = &'a RepresentativeAtom>,
        db: Option<&Self>,
    ) -> String {
        let parts: Vec<String> = facts
            .into_iter()
            .filter(|fact| db.map(|db| db.contains([*fact])).unwrap_or(true))
            .map(|fact| format!("{:?}({:?})", fact.predicate(), fact.values()))
            .collect();
        format!("{{ {} }}", parts.join(", "))
    }
}
