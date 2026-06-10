use crate::execution::planning::normalization::{atom::head::HeadAtom, rule::NormalizedRule};
use crate::rule_model::substitution::Substitution;

use crate::execution::selection_strategy::strategy_full_chain_stratification::reliance_memoization::RuleMemoization;
use crate::execution::selection_strategy::strategy_full_chain_stratification::util::extend::{AtomMapping, CheckResult, Reliance, extend_init};

fn check_self_restr(
    rule1: &NormalizedRule,
    rule2: &NormalizedRule,
    mu: &AtomMapping,
    eta: &Substitution,
) -> CheckResult {
    todo!()
}

pub fn is_self_restraint_reliance<'b, 'a: 'b>(
    mem: &'b mut RuleMemoization<'a>,
    rule_index: usize,
    previous_opt: Option<&Reliance>,
) -> Option<Reliance> {
    extend_init::<HeadAtom>(mem, rule_index, rule_index, check_self_restr, previous_opt)
}
