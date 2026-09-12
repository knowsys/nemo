use crate::execution::selection_strategy::strategy_full_chain_stratification::chain::atoms::{
    Atoms, EdgeId, Rule,
};
use crate::execution::selection_strategy::strategy_full_chain_stratification::util::ordered_atoms::{
    GetRuleMem, Mem, ReorderAtoms, ReorderedHead, ReorderedNegative, ReorderedPositive,
};

/// Marker for which part of a rule a search step (see `util::extend`) operates over.
pub(crate) trait AtomsPart {
    type Reordered: GetRuleMem;

    /// This part's atoms.
    fn atoms(rule: &Rule) -> &Atoms;

    /// Select this part's slot in the per-rule-index reordering cache.
    fn reordered_mem(cache: &mut ReorderAtoms) -> &mut Mem<Self::Reordered>;

    /// Unwrap the heuristically-ordered edge list out of the cached value.
    fn edges(reordered: &Self::Reordered) -> &[EdgeId];
}

pub(crate) struct Positive;
pub(crate) struct Negative;
pub(crate) struct Head;

impl AtomsPart for Positive {
    type Reordered = ReorderedPositive;

    fn atoms(rule: &Rule) -> &Atoms {
        rule.positive()
    }

    fn reordered_mem(cache: &mut ReorderAtoms) -> &mut Mem<Self::Reordered> {
        &mut cache.positive
    }

    fn edges(reordered: &Self::Reordered) -> &[EdgeId] {
        &reordered.0
    }
}

impl AtomsPart for Negative {
    type Reordered = ReorderedNegative;

    fn atoms(rule: &Rule) -> &Atoms {
        rule.negative_atoms()
    }

    fn reordered_mem(cache: &mut ReorderAtoms) -> &mut Mem<Self::Reordered> {
        &mut cache.negative
    }

    fn edges(reordered: &Self::Reordered) -> &[EdgeId] {
        &reordered.0
    }
}

impl AtomsPart for Head {
    type Reordered = ReorderedHead;

    fn atoms(rule: &Rule) -> &Atoms {
        rule.head()
    }

    fn reordered_mem(cache: &mut ReorderAtoms) -> &mut Mem<Self::Reordered> {
        &mut cache.head
    }

    fn edges(reordered: &Self::Reordered) -> &[EdgeId] {
        &reordered.0
    }
}
