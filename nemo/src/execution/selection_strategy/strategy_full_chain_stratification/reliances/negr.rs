use std::collections::HashSet;

use crate::execution::planning::normalization::{atom::body::NegBodyAtom, rule::NormalizedRule};
use crate::execution::selection_strategy::strategy_full_chain_stratification::util::database::{
    RepresentativeAtom, RepresentativeDatabase,
};
use crate::rule_model::substitution::Substitution;

use crate::execution::selection_strategy::strategy_full_chain_stratification::reliance_memoization::RuleMemoization;
use crate::execution::selection_strategy::strategy_full_chain_stratification::util::extend::{AtomMapping, CheckResult, Reliance, extend_init};

fn check_negr(
    rule1: &NormalizedRule,
    rule2: &NormalizedRule,
    _mu: &AtomMapping,
    eta: &Substitution,
) -> CheckResult {
    // idea:
    // - rule1 and rule2 start off both being applicable
    // - then rule1 is applied
    // - and now rule2 has lost its match (because a negative body literal now is in the database)

    let r1_existentials = rule1.existentials();

    // mu failed if eta assigned a variable in rule1.body to an existential
    if r1_existentials.iter().any(|v| eta.contains_variable(*v)) {
        log::trace!("existential of rule1 mapped");
        return CheckResult::Reject;
    }

    let r1_universals_eta = eta
        .substitute_variables(rule1.universals())
        .collect::<HashSet<_>>();
    if !r1_universals_eta.is_disjoint(&r1_existentials) {
        log::trace!(
            "universals of rule1 under eta are not disjoint from existentials of rule1 => mu failed"
        );
        return CheckResult::Reject;
    }

    // construct representative interpretation I_a
    let rule1_body_eta_cup_rule2_body_eta =
        RepresentativeAtom::substitute_atoms(eta, rule1.positive())
            .chain(RepresentativeAtom::substitute_atoms(eta, rule2.positive()))
            .collect::<HashSet<_>>();
    log::trace!(
        "I = {}",
        RepresentativeDatabase::display(&rule1_body_eta_cup_rule2_body_eta, None)
    );
    let interpretation_db = RepresentativeDatabase::new(&rule1_body_eta_cup_rule2_body_eta);

    // mu has to be extended if rule1 under eta is satisfied on I_a
    let rule1_head_eta =
        RepresentativeAtom::substitute_atoms(eta, rule1.head()).collect::<HashSet<_>>();
    if interpretation_db.entails(&r1_existentials, &rule1_head_eta) {
        log::trace!("I models head of rule1 under eta => mu failed");
        return CheckResult::Reject;
        // if allowing multi-atom negations, this would be CheckResult::Extend instead
    }

    for n in rule1.negative() {
        let n = RepresentativeAtom::from_atom_with_substitution(eta, n);
        let existentials = n
            .variables()
            .filter(|v| !r1_universals_eta.contains(v))
            .collect();
        if interpretation_db.entails(&existentials, [&n]) {
            log::trace!("I is not disjoint from negative body of rule1 under eta => mu failed");
            return CheckResult::Reject;
        }
    }

    // mu has to be extended if rule2 under eta is satisfied on I_a
    let rule2_head_eta =
        RepresentativeAtom::substitute_atoms(eta, rule2.head()).collect::<HashSet<_>>();
    let r2_existentials_eta = eta
        .substitute_variables(rule2.existentials())
        .collect::<HashSet<_>>();
    if interpretation_db.entails(&r2_existentials_eta, &rule2_head_eta) {
        log::trace!("I models head of rule2 under eta => mu must be extended");
        return CheckResult::Reject;
        // if allowing multi-atom negations, this would be CheckResult::Extend instead
    }

    let r2_universals_eta = eta
        .substitute_variables(rule2.universals())
        .collect::<HashSet<_>>();
    for n in rule2.negative() {
        let n = RepresentativeAtom::from_atom_with_substitution(eta, n);
        let existentials = n
            .variables()
            .filter(|v| !r2_universals_eta.contains(v))
            .collect();
        if interpretation_db.entails(&existentials, [&n]) {
            log::trace!("I is not disjoint from negative body of rule2 under eta => mu failed");
            return CheckResult::Reject;
        }
    }

    // done, as I\cup\head(\rho_1)\eta = I_b \cap \body_neg(\rho_2) is ensured by unifying the non-empty atom mapping \mu

    log::trace!("=> rule1 <- rule2",);
    CheckResult::Accept
}

pub fn is_negation_reliance<'b, 'a: 'b>(
    mem: &'b mut RuleMemoization<'a>,
    rule1_index: usize,
    rule2_index: usize,
    previous_opt: Option<&Reliance>,
) -> Option<Reliance> {
    extend_init::<NegBodyAtom>(
        mem,
        rule1_index,
        rule2_index,
        check_negr,
        previous_opt,
        Substitution::default(),
    )
}
