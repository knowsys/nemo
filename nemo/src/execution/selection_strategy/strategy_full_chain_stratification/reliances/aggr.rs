use crate::execution::planning::normalization::atom::body::BodyAtom;
use crate::execution::selection_strategy::strategy_full_chain_stratification::util::unify::unify;
use crate::rule_model::components::term::primitive::Primitive;
use crate::rule_model::substitution::Substitution;

use crate::execution::selection_strategy::strategy_full_chain_stratification::reliance_memoization::RuleMemoization;
use crate::execution::selection_strategy::strategy_full_chain_stratification::util::extend::{Reliance, extend_init};
use crate::execution::selection_strategy::strategy_full_chain_stratification::reliances::posr::{is_positive_reliance, check_posr};

pub fn is_aggregation_reliance<'b, 'a: 'b>(
    mem: &'b mut RuleMemoization<'a>,
    rule1_index: usize,
    rule2_index: usize,
    previous_opt: Option<&Reliance>,
) -> Option<Reliance> {
    let rule1 = mem.rules[rule1_index];
    let rule2 = mem.rules[rule2_index];
    debug_assert!(
        rule2.contains_aggregates(),
        "aggregation reliance checks should not be called with aggregate-free target rules",
    );
    let have_same_heads = rule1.head() == rule2.head();

    if have_same_heads {
        let terms_a = rule1
            .aggregate()
            .expect("should have aggregate")
            .group_by_variables()
            .iter()
            .map(|v| Primitive::Variable(v.clone()));
        let terms_b = rule2
            .aggregate()
            .expect("should have aggregate")
            .group_by_variables()
            .iter()
            .map(|v| Primitive::Variable(v.clone()));

        log::trace!("check that group-by variables are unifiable");
        let eta = unify(terms_a, terms_b, Substitution::default())?;

        log::trace!(
            "test for positive reliance when unifying the group-by variables in the aggregate atoms"
        );
        return extend_init::<BodyAtom>(
            mem,
            rule1_index,
            rule2_index,
            check_posr,
            previous_opt,
            eta,
        );
    }

    log::trace!("heads of rule1 and rule2 differ => check for normal positive reliance");
    is_positive_reliance(mem, rule1_index, rule1_index, previous_opt)
}
