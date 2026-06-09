use std::collections::{HashMap, HashSet};

use crate::execution::planning::analysis::variable_order::{
    VariableOrder, build_preferable_variable_orders_for_rule,
};
use crate::execution::planning::normalization::{
    atom::{
        body::{BodyAtom, NegBodyAtom},
        head::HeadAtom,
    },
    rule::NormalizedRule,
};
use crate::rule_model::components::{tag::Tag, term::primitive::Primitive};

use crate::execution::selection_strategy::strategy_full_chain_stratification::util::atom::{
    Atom, Predicate,
};

pub type ReorderedAtoms<'a, T> = Vec<&'a T>;
pub type ReorderedBodyAtoms<'a> = ReorderedAtoms<'a, BodyAtom>;
pub type ReorderedNegBodyAtoms<'a> = ReorderedAtoms<'a, NegBodyAtom>;
pub type ReorderedHeadAtoms<'a> = ReorderedAtoms<'a, HeadAtom>;

#[derive(Debug)]
pub struct Mem<T>(Vec<Option<T>>);

impl<T: Clone> Mem<T> {
    pub fn new(len: usize) -> Self {
        Self(vec![None; len])
    }
}

pub trait GetRuleMem<'a> {
    fn compute(rule: &'a NormalizedRule) -> Self;
}

impl<'a, T: GetRuleMem<'a>> Mem<T> {
    pub fn get(&mut self, rules: &'a Vec<&'a NormalizedRule>, rule_index: usize) -> &T {
        self.0[rule_index].get_or_insert_with(|| T::compute(&rules[rule_index]))
    }
}

fn reorder_atoms<'a, T: Atom>(
    atoms: impl IntoIterator<Item = &'a T>,
    order: &VariableOrder,
) -> ReorderedAtoms<'a, T> {
    let mut atoms: Vec<_> = atoms.into_iter().collect();
    let mut reordered = Vec::with_capacity(atoms.len());
    let mut vars_so_far = HashSet::new();
    for v in order.iter() {
        vars_so_far.insert(v);
        // find all atoms which only use variables from vars_so_far
        let (only_active_vars, remainder): (Vec<_>, Vec<_>) = atoms
            .into_iter()
            .partition(|atom| atom.variables().all(|var| vars_so_far.contains(var)));
        reordered.extend_from_slice(&only_active_vars);
        atoms = remainder;
    }
    reordered
}

impl<'a> GetRuleMem<'a> for ReorderedBodyAtoms<'a> {
    /// Reorder body atoms.
    /// to be applied to body / head of rule2 before calling extend
    /// maybe reuse Nemo's heuristic for join order?
    /// use NormalizeRule::variable_order --> should be Some
    fn compute(rule: &'a NormalizedRule) -> ReorderedBodyAtoms<'a> {
        reorder_atoms(rule.positive(), rule.body_variable_order())
    }
}

impl<'a> GetRuleMem<'a> for ReorderedNegBodyAtoms<'a> {
    /// Reorder negative body atoms.
    /// Such a variable order has not been computed anwhere else, as negative atoms don't partake in joins, so we just apply the heuristic manually.
    fn compute(rule: &'a NormalizedRule) -> ReorderedNegBodyAtoms<'a> {
        fn construct_auxiliary_negation_rule(rule: &NormalizedRule) -> NormalizedRule {
            let mut universal_variables = rule
                .negative()
                .flat_map(|atom| atom.terms())
                .collect::<Vec<_>>();
            universal_variables.dedup();

            let head = HeadAtom::new(
                Tag::new(String::from("__AUX")),
                universal_variables
                    .into_iter()
                    .cloned()
                    .map(Primitive::from),
            );

            NormalizedRule::positive_rule(vec![head], rule.negative().cloned().collect(), vec![])
        }

        let auxiliary_rule = construct_auxiliary_negation_rule(rule);
        let auxiliary_order = build_preferable_variable_orders_for_rule(&auxiliary_rule, None)
            .restrict_to(&rule.variables().cloned().collect::<HashSet<_>>());

        reorder_atoms(rule.negative_atoms(), &auxiliary_order)
    }
}

impl<'a> GetRuleMem<'a> for ReorderedHeadAtoms<'a> {
    /// Reorder head atoms.
    fn compute(rule: &'a NormalizedRule) -> ReorderedHeadAtoms<'a> {
        reorder_atoms(rule.head(), rule.head_variable_order())
    }
}

#[derive(Debug)]
pub(crate) struct ReorderAtoms<'a> {
    pub(crate) reordered_body_atoms: Mem<ReorderedBodyAtoms<'a>>,
    pub(crate) reordered_negative_body_atoms: Mem<ReorderedNegBodyAtoms<'a>>,
    pub(crate) reordered_head_atoms: Mem<ReorderedHeadAtoms<'a>>,
}

#[derive(Clone, Debug)]
pub(crate) struct SortedHeadAtoms<'a> {
    pub(crate) sorted_atoms: ReorderedHeadAtoms<'a>,
    pub(crate) ranges: HashMap<Predicate, (usize, usize)>,
}

impl<'a> GetRuleMem<'a> for SortedHeadAtoms<'a> {
    /// to be applied to head of rule1 before calling extend
    fn compute(rule: &'a NormalizedRule) -> SortedHeadAtoms<'a> {
        let mut sorted_atoms: Vec<_> = rule.head().iter().collect();
        sorted_atoms.sort_unstable_by_key(|atom| atom.predicate());
        let mut ranges = HashMap::new();
        let mut last = 0;
        for i in 1..sorted_atoms.len() {
            let p = sorted_atoms[i - 1].predicate();
            if &p != &sorted_atoms[i].predicate() {
                ranges.insert(p.name().to_string(), (last, i));
                last = i;
            }
        }
        SortedHeadAtoms {
            sorted_atoms,
            ranges,
        }
    }
}
