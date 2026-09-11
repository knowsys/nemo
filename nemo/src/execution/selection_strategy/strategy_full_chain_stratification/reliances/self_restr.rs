use std::collections::HashSet;

use crate::execution::selection_strategy::strategy_full_chain_stratification::chain::atoms::{
    Rule, combined_consts,
};
use crate::execution::selection_strategy::strategy_full_chain_stratification::chain::substitution::Substitution;
use crate::execution::selection_strategy::strategy_full_chain_stratification::util::atom::Head;
use crate::execution::selection_strategy::strategy_full_chain_stratification::util::database::{
    RepresentativeAtom, RepresentativeDatabase,
};
use crate::execution::selection_strategy::strategy_full_chain_stratification::util::pieces::compute_pieces;

use crate::execution::selection_strategy::strategy_full_chain_stratification::reliance_memoization::RuleMemoization;
use crate::execution::selection_strategy::strategy_full_chain_stratification::util::extend::{
    AtomMapping, CheckResult, Reliance, extend_init,
};

fn check_self_restr(
    rule: &Rule,
    rule2: &Rule,
    mu: &AtomMapping,
    eta: &Substitution,
) -> CheckResult {
    let consts = combined_consts(rule, rule2);

    let r_universals = rule.universals();
    let r_existentials = rule.existentials();
    let eta_forall = eta.restriction(&r_universals);
    let eta_exists = eta.restriction(&r_existentials);

    let rule_head_mapped = mu.mapped(rule.head()).collect::<HashSet<_>>();
    let rule_head_unmapped = rule
        .head()
        .iter()
        .collect::<HashSet<_>>()
        .difference(&rule_head_mapped)
        .copied()
        .collect::<HashSet<_>>();

    // NOTE: we diverge slightly from the check^\box_{self} algorithm given in appendix of ISWC_2022_Dependency_Paper (page 23)
    // --> line 38 states `if \psi\eta_\exists = \psi\omega_\exists then return false`,
    //     which would amount to `if \eta_\exists = id then return false` in our notation
    //     (the paper has \eta as a mapping \V \to \C\cup\N, while we use it as a mapping \V\to\V\cup\C)
    //     (and \omega is just an arbitrary fixed injective mapping \V\to\C\cup\N, which we can ommit since we assume that \V\subseteq\C\cup\N)
    // --> this is however false, as \eta = id can still yield valid self-restraints, e.g.:
    //     ρ: a(x) -> ∃ v. b(x), r(x,v), c(v)
    //     Ĩ = { a(1),r(1,2),c(2) }
    //     ⇒ ρ ≺☐ ρ

    // mu has to be extended if no existentials are mapped
    // NOTE: cases with identity mappings are handled before
    if eta_exists.is_empty() {
        log::trace!("eta_exists = id => mu must be extended");
        return CheckResult::Extend;
    }

    let rule_body_eta_cup_rule_head_unmapped_eta =
        RepresentativeAtom::substitute_atoms(eta, &consts, rule.positive())
            .into_iter()
            .chain(RepresentativeAtom::substitute_atoms(
                eta,
                &consts,
                rule_head_unmapped.iter().copied(),
            ))
            .collect::<Vec<_>>();

    log::trace!(
        "I' = {}",
        RepresentativeDatabase::display(&rule_body_eta_cup_rule_head_unmapped_eta, None)
    );
    let interpretation_pre_db =
        RepresentativeDatabase::new(&rule_body_eta_cup_rule_head_unmapped_eta);

    // mu has to be extended if rule under eta_forall is satisfied on I'
    let rule_head_eta_forall =
        RepresentativeAtom::substitute_atoms(&eta_forall, &consts, rule.head())
            .collect::<HashSet<_>>();
    if interpretation_pre_db.entails(&r_existentials, &rule_head_eta_forall) {
        log::trace!("I' models the head under eta_forall => mu must be extended");
        return CheckResult::Extend;
    }

    let r_universals_eta = eta
        .substitute_variables(r_universals.iter().copied())
        .collect::<HashSet<_>>();
    for n in rule.negative() {
        let n = RepresentativeAtom::from_atom_with_substitution(eta, &consts, n);
        let existentials = n
            .variables()
            .filter(|v| !r_universals_eta.contains(v))
            .collect();
        if interpretation_pre_db.entails(&existentials, [&n]) {
            log::trace!("I is not disjoint from negative body of rule under eta => mu failed");
            return CheckResult::Reject;
        }
    }

    // now injectively replace the existentials in head and add it to I_pre
    // fresh existentials for the ones that are already in pre db, i.e. in head_unmapped eta
    let rule_head_unmapped_vars_eta = eta
        .substitute_variables(rule_head_unmapped.iter().flat_map(|a| a.variables()))
        .collect::<HashSet<_>>();
    let used_existentials = rule_head_unmapped_vars_eta.intersection(&r_existentials); // existential variables that are already used
    // fresh variable ids, disjoint from both rule's (0..n) and its primed copy's (n..2n) ranges
    let base_fresh = rule.var_count() + rule2.var_count();
    let mut fresh_existentials = Substitution::new();
    for (i, &v) in used_existentials.enumerate() {
        fresh_existentials.insert(v, base_fresh + i);
    }
    let rule_head_eta_forall_fresh_existentials = RepresentativeAtom::substitute_atoms(
        &fresh_existentials.compose(&eta_forall),
        &consts,
        rule.head(),
    )
    .collect::<HashSet<_>>();
    log::trace!(
        "I = I' U {}",
        RepresentativeDatabase::display(
            &rule_head_eta_forall_fresh_existentials,
            Some(&interpretation_pre_db)
        )
    );
    let interpretation_db =
        interpretation_pre_db.add_facts(&rule_head_eta_forall_fresh_existentials);

    // then there should be alt match, i.e. check whether rule head eta is contained in I
    // Example why this is necessary:
    //   rule=p(X,Y) → ∃ V. q(X,Y,V),q(X,V,Y), mu=[1↦ 0,0↦ 0], eta=[V↦ Y])
    //   Ĩ = { p(X,Y) }
    //   I = Ĩ ∪ { q(X,Y,V),q(X,V,Y) }
    //   concluding ρ ≺☐ ρ now would be wrong, so check containment of { q(X,Y,Y) }
    // Note: The prior VLog implementation handled this case by ensuring that `unify` returns specific mappings,
    //       and then forbidding that existantials map to universals.
    let r_head_eta =
        RepresentativeAtom::substitute_atoms(eta, &consts, rule.head()).collect::<HashSet<_>>();
    if !interpretation_db.contains(&r_head_eta) {
        log::trace!("alternative match is not contained...");
        return CheckResult::Extend;
    }

    log::trace!("=> rule <[] rule");
    CheckResult::Accept
}

pub fn is_self_restraint_reliance<'b, 'a: 'b>(
    mem: &'b mut RuleMemoization<'a>,
    rule_index: usize,
    previous_opt: Option<&Reliance>,
) -> Option<Reliance> {
    mem.rules.ensure(rule_index);
    let rule = mem.rules.get(rule_index);

    // assuming the rule is not datalog, it has a trivial self-restraint if there are head atoms w/o existentials
    let universals = rule.universals();
    if rule.head().iter().any(|atom| {
        let not_contained_in_body = !rule.positive().contains(atom);
        not_contained_in_body
            && atom
                .terms()
                .iter()
                .all(|t| rule.consts().contains_key(t) || universals.contains(t))
    }) {
        log::trace!("non-datalog rule with datalog pieces => trivially self-restraining");
        return Some(Reliance::default());
    }

    // trivial self-restraint,
    // if there are multiple pieces and there is an existential piece,
    // s.t. the entire head is not entailed when it is satisfied
    let pp = mem
        .head_pieces
        .get_or_insert_with(rule_index, || compute_pieces(rule));
    if pp.len() > 1 {
        let identity = Substitution::new();
        for p in pp {
            if p.existentials.is_empty() {
                // skip datalog pieces
                continue;
            }
            let rule_body_cup_rule_head_unmapped = rule
                .positive()
                .iter()
                .chain(p.atoms.iter())
                .map(|a| RepresentativeAtom::from_atom_with_substitution(&identity, rule.consts(), a))
                .collect::<HashSet<_>>();
            log::trace!(
                "I' = {}",
                RepresentativeDatabase::display(&rule_body_cup_rule_head_unmapped, None)
            );
            let interpretation_pre_db =
                RepresentativeDatabase::new(&rule_body_cup_rule_head_unmapped);
            let rule_head = rule
                .head()
                .iter()
                .map(|a| RepresentativeAtom::from_atom_with_substitution(&identity, rule.consts(), a))
                .collect::<HashSet<_>>();
            if !interpretation_pre_db.entails(&rule.existentials(), &rule_head) {
                log::trace!("found problem piece");
                return Some(Reliance::default());
            }
        }
    }
    // NOTE: with this pre-check, we know that identity atom mappings do not need to be considered any more

    extend_init::<Head>(
        mem,
        rule_index,
        rule_index,
        check_self_restr,
        previous_opt,
        Substitution::default(),
    )
}
