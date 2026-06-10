use std::collections::{HashMap, HashSet};

use crate::execution::selection_strategy::strategy_full_chain_stratification::util::atom::{
    Atom, Predicate,
};
use crate::rule_model::{
    components::term::primitive::{Primitive, variable::Variable},
    substitution::Substitution,
};

#[derive(Eq, PartialEq, Hash, Debug)]
pub struct RepresentativeAtom {
    predicate: Predicate,
    primitives: Vec<Primitive>,
}

impl RepresentativeAtom {
    pub fn predicate(&self) -> &Predicate {
        &self.predicate
    }
    pub fn primitives(&self) -> &Vec<Primitive> {
        &self.primitives
    }
    pub fn variables(&self) -> impl Iterator<Item = &Variable> {
        self.primitives().into_iter().filter_map(|prim| match prim {
            Primitive::Variable(variable) => Some(variable),
            Primitive::Ground(_) => None,
        })
    }
    pub fn from_atom_with_substitution<T: Atom>(eta: &Substitution, atom: &T) -> Self {
        Self {
            predicate: atom.pred(),
            primitives: atom
                .primitives()
                .map(|p| eta.get_primitive(p.as_ref()).unwrap_or(p.as_ref()).clone())
                .collect(),
        }
    }
    pub fn substitute_atoms<'a, T: Atom + 'a>(
        eta: &Substitution,
        atoms: impl IntoIterator<Item = &'a T>,
    ) -> impl Iterator<Item = Self> {
        atoms
            .into_iter()
            .map(|atom| Self::from_atom_with_substitution(eta, atom))
    }
}

impl<T: Atom> From<&T> for RepresentativeAtom {
    fn from(atom: &T) -> Self {
        Self {
            predicate: atom.pred(),
            primitives: atom.primitives().map(|p| p.as_ref().clone()).collect(),
        }
    }
}

#[derive(Debug)]
pub struct RepresentativeDatabase(HashMap<Predicate, HashSet<Vec<Primitive>>>);

impl RepresentativeDatabase {
    pub fn new<'a>(facts: impl IntoIterator<Item = &'a RepresentativeAtom>) -> Self {
        Self(HashMap::new()).add_facts(facts)
    }

    pub fn add_facts<'a>(
        mut self,
        facts: impl IntoIterator<Item = &'a RepresentativeAtom>,
    ) -> Self {
        // here, variables are allowed in the database with the understanding that they are replaced with fresh variables injectively
        for fact in facts.into_iter() {
            self.0
                .entry(fact.predicate().clone())
                .or_insert_with(HashSet::new)
                .insert(fact.primitives().clone());
        }
        self
    }

    pub fn entails<'a>(
        &self,
        existentials: &HashSet<&Variable>,
        atoms: impl IntoIterator<Item = &'a RepresentativeAtom>,
    ) -> bool {
        // we assume that all but the existential variables are actually constants
        let atoms = atoms.into_iter().collect::<Vec<_>>();

        // first, check that all mentioned predicates are present in the databse
        let mut atom_pred_extend = Vec::with_capacity(atoms.len());
        for &atom in &atoms {
            if let Some(pred_extend) = self.0.get(atom.predicate()) {
                atom_pred_extend.push(pred_extend);
            } else {
                return false;
            }
        }

        // then restrict the predicate to only those facts where universals match
        let mut atom_facts = Vec::with_capacity(atoms.len());

        for (i, &atom) in (&atoms).into_iter().enumerate() {
            let options = atom_pred_extend[i]
                .iter()
                .filter(|fact| {
                    atom.primitives()
                        .iter()
                        .enumerate()
                        .filter(|(_, arg)| {
                            if let Primitive::Variable(var) = arg {
                                !existentials.contains(var)
                            } else {
                                true
                            }
                        })
                        .all(|(k, arg)| &fact[k] == arg)
                })
                .collect::<HashSet<_>>();
            if options.len() == 0 {
                return false;
            }
            atom_facts.push(options);
        }

        let mut existentials_map = HashMap::new();

        fn is_entailed<'a>(
            remaining_atoms: &'a [&'a RepresentativeAtom],
            atom_facts: &'a [HashSet<&'a Vec<Primitive>>],
            existentials: &HashSet<&Variable>,
            existentials_map: &mut HashMap<&'a Variable, Primitive>,
        ) -> bool {
            if remaining_atoms.len() == 0 {
                return true;
            }

            let atom = &remaining_atoms[0];
            let options = &atom_facts[0];

            for option in options.iter() {
                let mut inserted_terms = HashSet::new();
                if atom.primitives().iter().enumerate().all(|(k, arg)| {
                    if let Primitive::Variable(var) = arg {
                        match existentials_map.get(var) {
                            Some(val) => *val == option[k],
                            None => {
                                existentials_map.insert(var, option[k].clone());
                                inserted_terms.insert(var);
                                true
                            }
                        }
                    } else {
                        true
                    }
                }) {
                    if is_entailed(
                        &remaining_atoms[1..],
                        &atom_facts[1..],
                        existentials,
                        existentials_map,
                    ) {
                        return true;
                    }
                }
                inserted_terms.iter().for_each(|t| {
                    existentials_map.remove(t);
                });
            }
            false
        }

        is_entailed(
            &atoms[..],
            &atom_facts[..],
            existentials,
            &mut existentials_map,
        )
    }

    pub fn contains<'a>(&self, facts: impl IntoIterator<Item = &'a RepresentativeAtom>) -> bool {
        facts
            .into_iter()
            .all(|fact| match self.0.get(fact.predicate()) {
                Some(extent) => extent.contains(fact.primitives()),
                None => false,
            })
    }

    pub fn display<'a>(
        facts: impl IntoIterator<Item = &'a RepresentativeAtom>,
        db: Option<&Self>,
    ) -> String {
        let it = ["{ ".to_string()].into_iter();
        let it = it.chain(
            facts
                .into_iter()
                .filter(|fact| {
                    if let Some(db) = db {
                        db.contains([*fact])
                    } else {
                        true
                    }
                })
                .map(|fact| {
                    format!(
                        "{}({})",
                        fact.predicate(),
                        fact.primitives()
                            .into_iter()
                            .map(|p| p.to_string())
                            .intersperse(",".to_string())
                            .collect::<String>()
                    )
                })
                .intersperse(", ".to_string()),
        );
        let it = it.chain([" }".to_string()].into_iter());
        it.collect()
    }
}
