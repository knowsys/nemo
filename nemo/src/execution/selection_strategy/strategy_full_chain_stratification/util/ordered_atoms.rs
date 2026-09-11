use std::collections::{HashMap, HashSet};

use crate::execution::selection_strategy::strategy_full_chain_stratification::chain::atoms::{
    Atom, Predicate, Rule, Var,
};

/// A simple `Vec<Option<T>>`-backed per-rule-index cache.
#[derive(Debug)]
pub struct Mem<T>(Vec<Option<T>>);

impl<T> Mem<T> {
    pub fn new(len: usize) -> Self {
        Self((0..len).map(|_| None).collect())
    }

    pub fn get_or_insert_with(&mut self, index: usize, f: impl FnOnce() -> T) -> &T {
        self.0[index].get_or_insert_with(f)
    }
}

/// Reorder `atoms` so that, scanning left-to-right through `order`, every atom becomes eligible
/// (all its variables known) as early as possible. Mirrors the join-order heuristic used
/// elsewhere for query planning, just applied to pick a good order to extend an atom mapping in.
pub(crate) fn reorder_atoms(atoms: &[Atom], order: &[Var]) -> Vec<Atom> {
    let mut atoms: Vec<Atom> = atoms.to_vec();
    let mut reordered = Vec::with_capacity(atoms.len());
    let mut vars_so_far: HashSet<Var> = HashSet::new();

    for &v in order {
        vars_so_far.insert(v);
        let (ready, remainder): (Vec<_>, Vec<_>) = atoms
            .into_iter()
            .partition(|atom| atom.variables().all(|var| vars_so_far.contains(&var)));
        reordered.extend(ready);
        atoms = remainder;
    }
    // any atoms whose variables weren't all covered by `order` keep their original relative order
    reordered.extend(atoms);

    reordered
}

#[derive(Clone, Debug)]
pub(crate) struct SortedHeadAtoms {
    pub(crate) sorted_atoms: Vec<Atom>,
    pub(crate) ranges: HashMap<Predicate, (usize, usize)>,
}

/// Sort `rule`'s head atoms by predicate, and record the `[start, end)` range each predicate
/// occupies (to be applied to the head of rule1 before calling `extend`).
pub(crate) fn sorted_head_atoms(rule: &Rule) -> SortedHeadAtoms {
    let mut sorted_atoms: Vec<Atom> = rule.head().to_vec();
    sorted_atoms.sort_unstable_by_key(|atom| atom.predicate().0);

    let mut ranges = HashMap::new();
    let mut last = 0;
    for i in 1..sorted_atoms.len() {
        let p = sorted_atoms[i - 1].predicate();
        if p != sorted_atoms[i].predicate() {
            ranges.insert(p, (last, i));
            last = i;
        }
    }
    if let Some(last_atom) = sorted_atoms.last() {
        ranges.insert(last_atom.predicate(), (last, sorted_atoms.len()));
    }

    SortedHeadAtoms {
        sorted_atoms,
        ranges,
    }
}
