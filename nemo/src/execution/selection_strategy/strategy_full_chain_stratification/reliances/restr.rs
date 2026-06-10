use std::collections::HashSet;

use crate::execution::planning::normalization::{atom::head::HeadAtom, rule::NormalizedRule};
use crate::execution::selection_strategy::strategy_full_chain_stratification::util::database::{
    RepresentativeAtom, RepresentativeDatabase,
};
use crate::rule_model::substitution::Substitution;

use crate::execution::selection_strategy::strategy_full_chain_stratification::reliance_memoization::RuleMemoization;
use crate::execution::selection_strategy::strategy_full_chain_stratification::util::extend::{AtomMapping, CheckResult, Reliance, extend_init};

fn check_restr(
    rule1: &NormalizedRule,
    rule2: &NormalizedRule,
    mu: &AtomMapping,
    eta: &Substitution,
) -> CheckResult {
    let r1_existentials = rule1.existentials();
    if r1_existentials.iter().any(|x| {
        if let Some(to) = eta.get_variable(x) {
            r1_existentials.contains(to)
        } else {
            false
        }
    }) {
        log::trace!("eta collapsed existentials of rule1");
        return CheckResult::Reject;
    }

    let r2_universals = rule2.universals();
    let r1_r2_universals = rule1
        .universals()
        .union(&r2_universals)
        .copied()
        .collect::<HashSet<_>>();

    // mu failed if a universal variable has been mapped onto an existential of rule1 by eta
    if r1_r2_universals.iter().any(|x| {
        if let Some(to) = eta.get_variable(*x) {
            r1_existentials.contains(to)
        } else {
            false
        }
    }) {
        log::trace!(
            "union of universals of rule1 and rule2 under eta is not disjoint from existentail variables of rule1 under eta => mu failed"
        );
        return CheckResult::Reject;
    }

    let eta_forall = eta.restriction(&r1_r2_universals);
    log::trace!("eta_forall = {eta_forall}");

    let rule2_head_mapped_set = mu.mapped(&rule2.head()).collect::<HashSet<_>>();
    let rule2_head_unmapped = rule2
        .head()
        .iter()
        .collect::<HashSet<_>>()
        .difference(&rule2_head_mapped_set)
        .copied()
        .collect::<HashSet<_>>();
    let mu_maxidx = mu.maxidx();
    let rule2_head_unmapped_left = rule2.head()[..mu_maxidx]
        .iter()
        .collect::<HashSet<_>>()
        .difference(&rule2_head_mapped_set)
        .copied()
        .collect::<HashSet<_>>();

    // mu failed if an existential in rule2_head_unmapped_left has been mapped onto an existential of rule1 by eta
    let r2_existentials = rule2.existentials();
    let rule2_head_unmapped_left_vars_exists_eta = eta
        .substitute_variables(
            rule2_head_unmapped_left
                .iter()
                .flat_map(|atom| atom.variables())
                .collect::<HashSet<_>>()
                .intersection(&r2_existentials)
                .copied(),
        )
        .collect::<HashSet<_>>();
    let r1_existentials_eta = eta
        .substitute_variables(r1_existentials.iter().copied())
        .collect();
    if !rule2_head_unmapped_left_vars_exists_eta.is_disjoint(&r1_existentials_eta) {
        log::trace!(
            "existentials in the left unmapped part of the head of rule2 under eta are not disjoint from existentials in the head of rule1 under eta => mu failed"
        );
        return CheckResult::Reject;
    }

    let rule2_head_unmapped_right = rule2.head()[mu_maxidx..]
        .iter()
        .collect::<HashSet<_>>()
        .difference(&rule2_head_mapped_set)
        .copied()
        .collect::<HashSet<_>>();

    // mu has to be extended if eta assigned an existential in rule2_head_unmapped_right onto an existential of rule1
    let rule2_head_unmapped_right_vars_exists_eta = eta
        .substitute_variables(
            rule2_head_unmapped_right
                .iter()
                .flat_map(|atom| atom.variables())
                .collect::<HashSet<_>>()
                .intersection(&r2_existentials)
                .copied()
                .collect::<HashSet<_>>(),
        )
        .collect::<HashSet<_>>();
    if !rule2_head_unmapped_right_vars_exists_eta.is_disjoint(&r1_existentials_eta) {
        log::trace!(
            "existentials in the right unmapped part of rule2 under eta are not disjoint from existentials in the head of rule1 under eta => mu must be extended"
        );
        return CheckResult::Extend;
    }
    // mu has to be extended if there are no existential variables in rule2_head_mapped
    if rule2_head_mapped_set
        .iter()
        .all(|atom| atom.variables().all(|x| r2_universals.contains(x)))
    {
        log::trace!(
            "no existential variables in mapped part of the head of rule2 => mu must be extended"
        );
        return CheckResult::Extend;
    }

    // construct representative interpretation I_a'
    let rule2_body_eta_forall =
        RepresentativeAtom::substitute_atoms(&eta_forall, rule2.positive()).collect::<HashSet<_>>();
    let interpretation_a_pre_db = RepresentativeDatabase::new(&rule2_body_eta_forall);
    log::trace!(
        "I_a' = {}",
        RepresentativeDatabase::display(&rule2_body_eta_forall, None)
    );

    // mu has failed if rule2 under eta_forall is satisfied on I_a'
    let rule2_head_eta_forall =
        RepresentativeAtom::substitute_atoms(&eta_forall, rule2.head()).collect::<HashSet<_>>();
    let r2_existentials_eta_forall = eta_forall
        .substitute_variables(rule2.existentials())
        .collect();
    if interpretation_a_pre_db.entails(&r2_existentials_eta_forall, &rule2_head_eta_forall) {
        log::trace!("I_a' models the head of rule2 under eta_forall => mu failed");
        return CheckResult::Reject;
    }
    // construct representative interpretation I_a
    log::trace!(
        "I_a = I_a' U {}",
        RepresentativeDatabase::display(&rule2_head_eta_forall, Some(&interpretation_a_pre_db))
    );
    let interpretation_a_db = interpretation_a_pre_db.add_facts(&rule2_head_eta_forall);

    // construct representative interpretation I_b'
    let rule1_body_eta_cup_rule2_head_unmapped_eta =
        RepresentativeAtom::substitute_atoms(&eta_forall, rule1.positive())
            .into_iter()
            .chain(RepresentativeAtom::substitute_atoms(
                eta,
                rule2_head_unmapped.iter().copied(),
            ))
            .collect::<HashSet<_>>();
    log::trace!(
        "I_b' = I_a {}",
        RepresentativeDatabase::display(
            &rule1_body_eta_cup_rule2_head_unmapped_eta,
            Some(&interpretation_a_db)
        )
    );
    let interpretation_b_pre_db =
        interpretation_a_db.add_facts(&rule1_body_eta_cup_rule2_head_unmapped_eta);

    let rule1_head_eta_forall =
        RepresentativeAtom::substitute_atoms(&eta_forall, rule1.head()).collect::<HashSet<_>>();
    let rule1_vars_exists_eta_forall = eta_forall.substitute_variables(r1_existentials).collect();
    // \mu has to be extended if rule1 under \eta_\forall is satisfied on I_b' --> I_b' \models \psi_1\eta_\forall
    if interpretation_b_pre_db.entails(&rule1_vars_exists_eta_forall, &rule1_head_eta_forall) {
        log::trace!("I_b' models the head of rule1 under eta_forall => mu must be extended");
        return CheckResult::Extend;
    }
    // have to make sure that eta_exists takes variables from rule2 to variables of rule1 and not the other way around!!!
    // mu has to be extended if rule2_head under eta is fully contained in I_b'
    let rule2_head_eta =
        RepresentativeAtom::substitute_atoms(eta, rule2.head()).collect::<HashSet<_>>();
    if interpretation_b_pre_db.contains(&rule2_head_eta) {
        log::trace!("head of rule2 under eta subseteq I_b' => mu must be extended",);
        return CheckResult::Extend;
    }

    let r1_universals_eta = eta
        .substitute_variables(rule1.universals())
        .collect::<HashSet<_>>();
    for n in rule1.negative() {
        let n = RepresentativeAtom::from_atom_with_substitution(eta, n);
        let existentials = n
            .variables()
            .filter(|v| !r1_universals_eta.contains(v))
            .collect();
        if interpretation_b_pre_db.entails(&existentials, [&n]) {
            log::trace!("I_b is not disjoint from negative body of rule1 under eta => mu failed");
            return CheckResult::Reject;
        }
    }

    // the alternative match for rule1 should now always take effect
    #[cfg(debug_assertions)]
    {
        let rule1_head_eta =
            RepresentativeAtom::substitute_atoms(&eta_forall, rule1.head()).collect::<HashSet<_>>();
        log::trace!(
            "I_b = I_b' U {}",
            RepresentativeDatabase::display(&rule1_head_eta, Some(&interpretation_b_pre_db))
        );
        let interpretation_b_db = interpretation_b_pre_db.add_facts(&rule1_head_eta);
        debug_assert!(
            interpretation_b_db.contains(&rule2_head_eta),
            "should be contained"
        );
    }

    log::trace!("=> rule1 <[] rule2");
    CheckResult::Accept
}

pub fn is_restraint_reliance<'b, 'a: 'b>(
    mem: &'b mut RuleMemoization<'a>,
    rule1_index: usize,
    rule2_index: usize,
    previous_opt: Option<&Reliance>,
) -> Option<Reliance> {
    extend_init::<HeadAtom>(
        mem,
        rule1_index,
        rule2_index,
        check_restr,
        previous_opt,
        Substitution::default(),
    )
}
