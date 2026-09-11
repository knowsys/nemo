use std::collections::{HashMap, HashSet};

use crate::execution::selection_strategy::strategy_full_chain_stratification::chain::atoms::{
    Atom, Predicate, Rule, Var,
};
use crate::execution::selection_strategy::strategy_full_chain_stratification::util::pieces::{
    Piece, compute_pieces,
};

/// Computes the per-rule-index value a [Mem] cache holds, so that every insertion for a given
/// `T` is guaranteed to go through the same computation.
pub(crate) trait GetRuleMem {
    fn compute(rule: &Rule) -> Self;
}

/// A simple per-rule-index cache: `get` computes (via [GetRuleMem::compute]) and caches a value
/// the first time a given `rule_index` is requested, and just returns the cached value on
/// subsequent requests.
#[derive(Debug)]
pub struct Mem<T>(Vec<Option<T>>);

impl<T: GetRuleMem> Mem<T> {
    pub fn new(len: usize) -> Self {
        Self((0..len).map(|_| None).collect())
    }

    pub fn get(&mut self, rule: &Rule, rule_index: usize) -> &T {
        self.0[rule_index].get_or_insert_with(|| T::compute(rule))
    }
}

impl GetRuleMem for Vec<Piece> {
    fn compute(rule: &Rule) -> Self {
        compute_pieces(rule)
    }
}

/// Reorder `atoms` so that, scanning left-to-right through `order`, every atom becomes eligible
/// (all its variables known) as early as possible. Mirrors the join-order heuristic used
/// elsewhere for query planning, just applied to pick a good order to extend an atom mapping in.
fn reorder_atoms(atoms: &[Atom], order: &[Var]) -> Vec<Atom> {
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

impl GetRuleMem for SortedHeadAtoms {
    /// Sort `rule`'s head atoms by predicate, and record the `[start, end)` range each predicate
    /// occupies (to be applied to the head of rule1 before calling `extend`).
    fn compute(rule: &Rule) -> Self {
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
}

// The three wrappers below hold the heuristically-reordered atoms of one part of a rule (see
// `util::atom::AtomsPart`), each computed against the rule's *native* (unprimed) ids and
// cached/shared across every pair the rule appears in; callers needing a primed (offset-shifted)
// copy for a specific comparison shift this (small) cached result themselves, rather than
// re-running the heuristic. They're separate types (rather than all just `Vec<Atom>`) so each can
// have its own `GetRuleMem` impl.

#[derive(Debug)]
pub(crate) struct ReorderedPositive(pub(crate) Vec<Atom>);

impl GetRuleMem for ReorderedPositive {
    fn compute(rule: &Rule) -> Self {
        ReorderedPositive(reorder_atoms(rule.positive(), rule.body_variable_order()))
    }
}

#[derive(Debug)]
pub(crate) struct ReorderedNegative(pub(crate) Vec<Atom>);

impl GetRuleMem for ReorderedNegative {
    fn compute(rule: &Rule) -> Self {
        ReorderedNegative(reorder_atoms(
            rule.negative_atoms(),
            rule.negative_variable_order(),
        ))
    }
}

#[derive(Debug)]
pub(crate) struct ReorderedHead(pub(crate) Vec<Atom>);

impl GetRuleMem for ReorderedHead {
    fn compute(rule: &Rule) -> Self {
        ReorderedHead(reorder_atoms(rule.head(), rule.head_variable_order()))
    }
}

#[derive(Debug)]
pub(crate) struct ReorderAtoms {
    pub(crate) positive: Mem<ReorderedPositive>,
    pub(crate) negative: Mem<ReorderedNegative>,
    pub(crate) head: Mem<ReorderedHead>,
}

impl ReorderAtoms {
    pub fn new(len: usize) -> Self {
        Self {
            positive: Mem::new(len),
            negative: Mem::new(len),
            head: Mem::new(len),
        }
    }
}
