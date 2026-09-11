use crate::execution::selection_strategy::strategy_full_chain_stratification::chain::atoms::combined_consts;
use crate::execution::selection_strategy::strategy_full_chain_stratification::chain::substitution::Substitution;
use crate::execution::selection_strategy::strategy_full_chain_stratification::util::atom::Positive;
use crate::execution::selection_strategy::strategy_full_chain_stratification::util::unify::unify;

use crate::execution::selection_strategy::strategy_full_chain_stratification::reliance_memoization::RuleMemoization;
use crate::execution::selection_strategy::strategy_full_chain_stratification::reliances::posr::{
    check_posr, is_positive_reliance,
};
use crate::execution::selection_strategy::strategy_full_chain_stratification::util::extend::{
    Reliance, extend_init,
};

pub fn is_aggregation_reliance<'b, 'a: 'b>(
    mem: &'b mut RuleMemoization<'a>,
    rule1_index: usize,
    rule2_index: usize,
    previous_opt: Option<&Reliance>,
) -> Option<Reliance> {
    let (rule1, rule2_native) = mem.rules.get_two(rule1_index, rule2_index);
    let rule2 = rule2_native.prime(rule1.var_count());

    let have_same_heads = rule1.head() == rule2.head();

    if have_same_heads {
        log::trace!("check that group-by variables are unifiable");
        // NOTE: this codebase does not yet model aggregates on the new `Rule` representation, so
        // group-by unification degenerates to head unification here (falling straight through to
        // a check for positive reliance, same as the "heads differ" case below).
        let consts = combined_consts(rule1, &rule2);
        let eta = unify(
            rule1.head().iter().flat_map(|a| a.terms().iter().copied()),
            rule2.head().iter().flat_map(|a| a.terms().iter().copied()),
            &consts,
            Substitution::default(),
        )?;

        log::trace!(
            "test for positive reliance when unifying the group-by variables in the aggregate atoms"
        );
        return extend_init::<Positive>(
            mem,
            rule1_index,
            rule2_index,
            check_posr,
            previous_opt,
            eta,
        );
    }

    log::trace!("heads of rule1 and rule2 differ => check for normal positive reliance");
    is_positive_reliance(mem, rule1_index, rule2_index, previous_opt)
}
