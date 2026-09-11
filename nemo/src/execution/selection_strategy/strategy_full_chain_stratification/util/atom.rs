use crate::execution::selection_strategy::strategy_full_chain_stratification::chain::atoms::Atom;
use crate::execution::selection_strategy::strategy_full_chain_stratification::util::ordered_atoms::{
    GetRuleMem, Mem, ReorderAtoms, ReorderedHead, ReorderedNegative, ReorderedPositive,
};

/// Marker for which part of a rule a search step (see `util::extend`) operates over.
pub(crate) trait AtomsPart {
    type Reordered: GetRuleMem;

    /// Select this part's slot in the per-rule-index reordering cache.
    fn reordered_mem(cache: &mut ReorderAtoms) -> &mut Mem<Self::Reordered>;

    /// Unwrap the reordered atoms out of the cached value.
    fn atoms(reordered: &Self::Reordered) -> &[Atom];
}

pub(crate) struct Positive;
pub(crate) struct Negative;
pub(crate) struct Head;

impl AtomsPart for Positive {
    type Reordered = ReorderedPositive;

    fn reordered_mem(cache: &mut ReorderAtoms) -> &mut Mem<Self::Reordered> {
        &mut cache.positive
    }

    fn atoms(reordered: &Self::Reordered) -> &[Atom] {
        &reordered.0
    }
}

impl AtomsPart for Negative {
    type Reordered = ReorderedNegative;

    fn reordered_mem(cache: &mut ReorderAtoms) -> &mut Mem<Self::Reordered> {
        &mut cache.negative
    }

    fn atoms(reordered: &Self::Reordered) -> &[Atom] {
        &reordered.0
    }
}

impl AtomsPart for Head {
    type Reordered = ReorderedHead;

    fn reordered_mem(cache: &mut ReorderAtoms) -> &mut Mem<Self::Reordered> {
        &mut cache.head
    }

    fn atoms(reordered: &Self::Reordered) -> &[Atom] {
        &reordered.0
    }
}
