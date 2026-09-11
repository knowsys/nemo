use std::collections::HashMap;

use crate::execution::selection_strategy::strategy_full_chain_stratification::chain::atoms::{
    Constant, Var,
};
use crate::execution::selection_strategy::strategy_full_chain_stratification::chain::substitution::Substitution;

/// Extend MGU eta to also unify the given iterators of terms.
/// terms_a and terms_b should correspond to atoms with the same predicate symbol, i.e. the same arity,
/// so unify(.) assumes that both iterators have the same number of values.
///
/// `consts` classifies which resolved ids are actually bound to a ground value (see
/// [`super::super::chain::atoms::combined_consts`]); it must cover every rule `terms_a`/`terms_b`
/// or `eta`'s range may reference.
///
/// Try to preserve variables of atom_a by mapping variables of atom_b onto them.
/// We will produce an MGU with dom(eta) ∩ ran(eta) = ∅, i.e. A->B means that B is not replaced.
pub(crate) fn unify(
    terms_a: impl Iterator<Item = Var>,
    terms_b: impl Iterator<Item = Var>,
    consts: &HashMap<Var, Constant>,
    mut eta: Substitution,
) -> Option<Substitution> {
    for (term_a, term_b) in terms_a.zip(terms_b) {
        if term_a != term_b {
            let mapped_term_a = eta.get_variable(term_a).unwrap_or(term_a);
            let mapped_term_b = eta.get_variable(term_b).unwrap_or(term_b);
            let const_a = consts.get(&mapped_term_a).copied();
            let const_b = consts.get(&mapped_term_b).copied();

            match (const_a, const_b) {
                (Some(a), Some(b)) => {
                    if a != b {
                        log::trace!(
                            "cannot assign constant {mapped_term_a} to constant {mapped_term_b}"
                        );
                        return None;
                    }
                }
                (Some(_), None) => {
                    log::trace!("remap {term_b} from {mapped_term_b} to constant {mapped_term_a}");
                    eta.remap(mapped_term_b, mapped_term_a);
                }
                (None, Some(_)) => {
                    log::trace!("remap {term_a} from {mapped_term_a} to constant {mapped_term_b}");
                    eta.remap(mapped_term_a, mapped_term_b);
                }
                (None, None) => {
                    if mapped_term_a != mapped_term_b {
                        log::trace!(
                            "remap {term_b} from {mapped_term_b} to variable {mapped_term_a}"
                        );
                        // have a choice, so leave the mapping for term_a in place and change it for term_b!
                        eta.remap(mapped_term_b, mapped_term_a);
                    }
                }
            }
        }
    }

    Some(eta)
}
