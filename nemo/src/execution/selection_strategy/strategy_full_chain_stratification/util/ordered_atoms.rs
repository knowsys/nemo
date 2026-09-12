use std::collections::HashSet;

use crate::execution::selection_strategy::strategy_full_chain_stratification::chain::atoms::{
    Atom, Atoms, EdgeId, Rule, Var,
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

/// Order `atoms`' edges so that, scanning left-to-right through `order`, every atom becomes
/// eligible (all its variables known) as early as possible. Mirrors the join-order heuristic used
/// elsewhere for query planning, just applied to pick a good order to extend an atom mapping in.
fn reorder_edges(atoms: &Atoms, order: &[Var]) -> Vec<EdgeId> {
    let mut remaining: Vec<(EdgeId, Atom)> = atoms.iter().collect();
    let mut reordered = Vec::with_capacity(remaining.len());
    let mut vars_so_far: HashSet<Var> = HashSet::new();

    for &v in order {
        vars_so_far.insert(v);
        let (ready, rest): (Vec<_>, Vec<_>) = remaining
            .into_iter()
            .partition(|(_, atom)| atom.variables().all(|var| vars_so_far.contains(&var)));
        reordered.extend(ready.into_iter().map(|(edge, _)| edge));
        remaining = rest;
    }
    // any atoms whose variables weren't all covered by `order` keep their original relative order
    reordered.extend(remaining.into_iter().map(|(edge, _)| edge));

    reordered
}

// The three wrappers below hold the heuristically-reordered edges of one part of a rule (see
// `util::atom::AtomsPart`), each computed against the rule's *native* (unprimed) ids and
// cached/shared across every pair the rule appears in; callers needing a primed (offset-shifted)
// copy for a specific comparison shift the (small) rows fetched via these edges themselves,
// rather than re-running the heuristic. They're separate types (rather than all just `Vec<EdgeId>`)
// so each can have its own `GetRuleMem` impl.

#[derive(Debug)]
pub(crate) struct ReorderedPositive(pub(crate) Vec<EdgeId>);

impl GetRuleMem for ReorderedPositive {
    fn compute(rule: &Rule) -> Self {
        ReorderedPositive(reorder_edges(rule.positive(), rule.body_variable_order()))
    }
}

#[derive(Debug)]
pub(crate) struct ReorderedNegative(pub(crate) Vec<EdgeId>);

impl GetRuleMem for ReorderedNegative {
    fn compute(rule: &Rule) -> Self {
        ReorderedNegative(reorder_edges(
            rule.negative_atoms(),
            rule.negative_variable_order(),
        ))
    }
}

#[derive(Debug)]
pub(crate) struct ReorderedHead(pub(crate) Vec<EdgeId>);

impl GetRuleMem for ReorderedHead {
    fn compute(rule: &Rule) -> Self {
        ReorderedHead(reorder_edges(rule.head(), rule.head_variable_order()))
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
