use std::collections::HashSet;

use crate::execution::planning::normalization::{atom::body::BodyAtom, rule::NormalizedRule};
use crate::execution::selection_strategy::strategy_full_chain_stratification::util::atom::Atom;
use crate::rule_model::substitution::Substitution;

use crate::execution::selection_strategy::strategy_full_chain_stratification::reliance_memoization::RuleMemoization;
use crate::execution::selection_strategy::strategy_full_chain_stratification::util::extend::{AtomMapping, CheckResult, Reliance, extend_init};
use crate::execution::selection_strategy::strategy_full_chain_stratification::util::database::{RepresentativeDatabase, RepresentativeAtom};

fn check_posr(
    rule1: &NormalizedRule,
    rule2: &NormalizedRule,
    mu: &AtomMapping,
    eta: &Substitution,
) -> CheckResult {
    let r1_universals_eta = eta
        .substitute_variables(rule1.universals())
        .collect::<HashSet<_>>();
    let r1_existentials = rule1.existentials();

    // mu failed if eta assigned a variable in rule1.body to an existential
    if r1_existentials.iter().any(|v| eta.contains_variable(*v)) {
        log::trace!("existential of rule1 mapped");
        return CheckResult::Reject;
    }
    if !r1_universals_eta.is_disjoint(&r1_existentials) {
        log::trace!(
            "universals of rule1 under eta are not disjoint from existentials of rule1 => mu failed"
        );
        return CheckResult::Reject;
    }

    let rule2_body_mapped_set = mu.mapped(&rule2.positive()).collect::<HashSet<_>>();

    let mu_maxidx = mu.maxidx();

    let rule2_body_unmapped_left = &rule2.positive()[..mu_maxidx]
        .iter()
        .collect::<HashSet<_>>()
        .difference(&rule2_body_mapped_set)
        .cloned()
        .collect::<HashSet<_>>();

    // mu failed if eta assigned a variable in the left unmapped portion of rule2.body to an existential
    let rule2_body_unmapped_left_vars_eta = eta
        .substitute_variables(
            rule2_body_unmapped_left
                .iter()
                .flat_map(|atom| atom.variables()),
        )
        .collect::<HashSet<_>>();
    if !rule2_body_unmapped_left_vars_eta.is_disjoint(&r1_existentials) {
        log::trace!(
            "variables of the left unmapped part of rule2 under eta are not disjoint from existentials of rule1 => mu failed"
        );
        return CheckResult::Reject;
    }

    let rule2_body_unmapped_right = &rule2.positive()[mu_maxidx..]
        .iter()
        .collect::<HashSet<_>>()
        .difference(&rule2_body_mapped_set)
        .cloned()
        .collect::<HashSet<_>>();

    // mu has to be extended if eta assigned a variable from the right unmapped portion of rule2.body to an existential
    let rule2_body_unmapped_right_vars_eta = eta
        .substitute_variables(
            rule2_body_unmapped_right
                .iter()
                .flat_map(|atom| atom.variables()),
        )
        .collect::<HashSet<_>>();
    if !rule2_body_unmapped_right_vars_eta.is_disjoint(&r1_existentials) {
        log::trace!(
            "variables of the right unmapped part of rule2 under eta are disjoint from existentials of rule1 => mu must be extended"
        );
        return CheckResult::Extend;
    }

    let rule1_body_eta =
        RepresentativeAtom::substitute_atoms(eta, rule1.positive()).collect::<HashSet<_>>();

    let rule2_body_unmapped_eta = RepresentativeAtom::substitute_atoms(
        eta,
        rule2
            .positive()
            .iter()
            .collect::<HashSet<_>>()
            .difference(&rule2_body_mapped_set)
            .copied(),
    )
    .collect();

    // construct representative interpretation I_a
    let rule1_body_eta_cup_rule2_body_unmapped_eta = rule1_body_eta
        .union(&rule2_body_unmapped_eta)
        .collect::<HashSet<_>>();
    log::trace!(
        "I_a = {}",
        RepresentativeDatabase::display(
            rule1_body_eta_cup_rule2_body_unmapped_eta.iter().copied(),
            None
        )
    );
    let interpretation_a_db =
        RepresentativeDatabase::new(rule1_body_eta_cup_rule2_body_unmapped_eta.into_iter());

    // mu has to be extended if rule1 under eta is satisfied on I_a
    let rule1_head_eta =
        RepresentativeAtom::substitute_atoms(eta, rule1.head()).collect::<HashSet<_>>();
    if interpretation_a_db.entails(&r1_existentials, &rule1_head_eta) {
        log::trace!("I_a models the head of rule1 under eta => mu must be extended");
        return CheckResult::Extend;
    }

    let rule2_body_eta =
        RepresentativeAtom::substitute_atoms(eta, rule2.positive()).collect::<HashSet<_>>();

    // mu has to be extended if rule2_body under eta is fully contained in I_a
    if interpretation_a_db.contains(&rule2_body_eta) {
        log::trace!("body of rule2 under eta already contained in I_a => mu must be extended");
        return CheckResult::Extend;
    }

    // construct representative interpretation I_b
    log::trace!(
        "I_b = I_a U {}",
        RepresentativeDatabase::display(&rule1_head_eta, Some(&interpretation_a_db))
    );
    let interpretation_b_db = interpretation_a_db.add_facts(&rule1_head_eta);

    // mu has failed if rule2 under eta is satisfied in I_b
    let r2_existentials = eta.substitute_variables(rule2.existentials()).collect();
    let rule2_head_eta =
        RepresentativeAtom::substitute_atoms(eta, rule2.head()).collect::<HashSet<_>>();
    if interpretation_b_db.entails(&r2_existentials, &rule2_head_eta) {
        log::trace!("I_b models head of rule2 under eta => mu failed");
        return CheckResult::Reject;
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
        if interpretation_b_db.entails(&existentials, [&n]) {
            log::trace!("I_b is not disjoint from negative body of rule2 under eta => mu failed");
            return CheckResult::Reject;
        }
    }

    log::trace!("=> rule1 <+ rule2",);
    CheckResult::Accept
}

pub fn is_positive_reliance<'b, 'a: 'b>(
    mem: &'b mut RuleMemoization<'a>,
    rule1_index: usize,
    rule2_index: usize,
    previous_opt: Option<&Reliance>,
) -> Option<Reliance> {
    extend_init::<BodyAtom>(mem, rule1_index, rule2_index, check_posr, previous_opt)
}
