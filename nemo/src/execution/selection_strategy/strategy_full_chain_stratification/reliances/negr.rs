use crate::execution::planning::normalization::{atom::body::NegBodyAtom, rule::NormalizedRule};
use crate::rule_model::substitution::Substitution;

use crate::execution::selection_strategy::strategy_full_chain_stratification::reliance_memoization::RuleMemoization;
use crate::execution::selection_strategy::strategy_full_chain_stratification::util::extend::{AtomMapping, CheckResult, Reliance, extend_init};

fn check_negr(
    rule1: &NormalizedRule,
    rule2: &NormalizedRule,
    mu: &AtomMapping,
    eta: &Substitution,
) -> CheckResult {
    todo!()
}

pub fn is_negation_reliance<'b, 'a: 'b>(
    mem: &'b mut RuleMemoization<'a>,
    rule1_index: usize,
    rule2_index: usize,
    previous_opt: Option<&Reliance>,
) -> Option<Reliance> {
    extend_init::<NegBodyAtom>(mem, rule1_index, rule2_index, check_negr, previous_opt)
}
