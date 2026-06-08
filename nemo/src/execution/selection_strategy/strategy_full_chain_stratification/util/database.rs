use std::collections::{HashMap, HashSet};

use crate::rule_model::components::term::primitive::Primitive;

use crate::execution::selection_strategy::strategy_full_chain_stratification::util::atom::{
    Atom, Predicate,
};
use crate::rule_model::components::term::primitive::variable::Variable;

pub struct RepresentativeDatabase(HashMap<Predicate, HashSet<Vec<Primitive>>>);

impl RepresentativeDatabase {
    pub fn new<'a, T: Atom + 'a>(ground_atoms: impl IntoIterator<Item = &'a T>) -> Self {
        Self::collect(ground_atoms, Self(HashMap::new()))
    }

    pub fn collect<'a, T: Atom + 'a>(
        ground_atoms: impl IntoIterator<Item = &'a T>,
        mut db: Self,
    ) -> Self {
        // here, variables are allowed in the database with the understanding that they are replaced with fresh variables injectively
        for ground_atom in ground_atoms.into_iter() {
            db.0.entry(ground_atom.pred())
                .or_insert_with(HashSet::new)
                .insert(
                    ground_atom
                        .primitives()
                        .map(|p| p.as_ref().clone())
                        .collect(),
                );
        }
        db
    }

    pub fn entails<'a, T: Atom + 'a>(
        &self,
        existentials: &HashSet<&Variable>,
        atoms: impl IntoIterator<Item = &'a T>,
    ) -> bool {
        todo!()
    }

    pub fn contains<'a, T: Atom + 'a>(
        &self,
        atoms: impl IntoIterator<Item = &'a T>,
    ) -> bool {
        todo!()
    }
}
