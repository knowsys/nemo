use crate::execution::selection_strategy::strategy_full_chain_stratification::chain::atoms::{
    Atom, Rule, Var,
};

/// Marker for which part of a rule a search step (see `util::extend`) operates over.
pub(crate) trait AtomsPart {
    fn atoms(rule: &Rule) -> &[Atom];
    fn variable_order(rule: &Rule) -> &[Var];
}

pub(crate) struct Positive;
pub(crate) struct Negative;
pub(crate) struct Head;

impl AtomsPart for Positive {
    fn atoms(rule: &Rule) -> &[Atom] {
        rule.positive()
    }

    fn variable_order(rule: &Rule) -> &[Var] {
        rule.body_variable_order()
    }
}

impl AtomsPart for Negative {
    fn atoms(rule: &Rule) -> &[Atom] {
        rule.negative_atoms()
    }

    fn variable_order(rule: &Rule) -> &[Var] {
        rule.negative_variable_order()
    }
}

impl AtomsPart for Head {
    fn atoms(rule: &Rule) -> &[Atom] {
        rule.head()
    }

    fn variable_order(rule: &Rule) -> &[Var] {
        rule.head_variable_order()
    }
}
