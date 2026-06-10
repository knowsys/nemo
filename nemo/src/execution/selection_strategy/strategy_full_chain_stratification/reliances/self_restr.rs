use std::collections::HashSet;

use crate::execution::planning::normalization::atom::body::BodyAtom;
use crate::execution::planning::normalization::{atom::head::HeadAtom, rule::NormalizedRule};
use crate::execution::selection_strategy::strategy_full_chain_stratification::util::database::{
    RepresentativeAtom, RepresentativeDatabase,
};
use crate::rule_model::components::term::Term;
use crate::rule_model::components::term::primitive::Primitive;
use crate::rule_model::substitution::Substitution;

use crate::execution::selection_strategy::strategy_full_chain_stratification::reliance_memoization::RuleMemoization;
use crate::execution::selection_strategy::strategy_full_chain_stratification::util::extend::{AtomMapping, CheckResult, Reliance, extend_init};

fn check_self_restr(
    rule: &NormalizedRule,
    _rule: &NormalizedRule,
    mu: &AtomMapping,
    eta: &Substitution,
) -> CheckResult {
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
        RepresentativeAtom::substitute_atoms(eta, rule.positive())
            .into_iter()
            .chain(RepresentativeAtom::substitute_atoms(
                eta,
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
        RepresentativeAtom::substitute_atoms(&eta_forall, rule.head()).collect::<HashSet<_>>();
    if interpretation_pre_db.entails(&r_existentials, &rule_head_eta_forall) {
        log::trace!("I' models the head under eta_forall => mu must be extended");
        return CheckResult::Extend;
    }

    let r_universals_eta = eta
        .substitute_variables(r_universals)
        .collect::<HashSet<_>>();
    for n in rule.negative() {
        let n = RepresentativeAtom::from_atom_with_substitution(eta, n);
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
    let fresh_existentials = Substitution::new(used_existentials.map(|v| {
        (
            Primitive::Variable((*v).clone()),
            Term::Primitive(todo!("v.prime()")),
        )
    }));
    // todo change everything to PrimedVariables !!!
    let rule_head_eta_forall_fresh_existentials =
        RepresentativeAtom::substitute_atoms(&fresh_existentials.compose(&eta_forall), rule.head())
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
    let r_head_eta = RepresentativeAtom::substitute_atoms(eta, rule.head()).collect::<HashSet<_>>();
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
    let rule = mem.rules[rule_index];

    // assuming the rule is not datalog, it has a trivial self-restraint if there are head atoms w/o existentials
    debug_assert!(
        !rule.is_datalog(),
        "self-restraint checks should not be called on datalog rules"
    );
    if rule.head().iter().any(|atom| {
        let not_contained_in_body = if let Some(body_terms) = atom
            .terms()
            .map(|t| match t {
                Primitive::Variable(var) => Some(var.clone()),
                _ => None,
            })
            .collect::<Option<Vec<_>>>()
        {
            !rule
                .positive()
                .contains(&BodyAtom::new(atom.predicate(), body_terms.into_iter()))
        } else {
            true
        };
        not_contained_in_body
            && atom.terms().all(|arg| match arg {
                Primitive::Variable(var) => rule.universals().contains(var),
                _ => true,
            })
    }) {
        log::trace!("non-datalog rule with datalog pieces => trivially self-restraining");
        return Some(Reliance::default());
    }

    // trivial self-restraint,
    // if there are multiple pieces and there is an existential piece,
    // s.t. the entire head is not entailed when it is satisfied
    let pp = mem.head_pieces.get(mem.rules, rule_index);
    if pp.len() > 1 {
        for p in pp {
            if p.existentials.len() == 0 {
                // skip datalog pieces
                continue;
            }
            let rule_body_cup_rule_head_unmapped = rule
                .positive()
                .iter()
                .map(|a| RepresentativeAtom::from(a))
                .chain(p.atoms.iter().map(|a| a.into()))
                .collect::<HashSet<_>>();
            log::trace!(
                "I' = {}",
                RepresentativeDatabase::display(&rule_body_cup_rule_head_unmapped, None)
            );
            let interpretation_pre_db =
                RepresentativeDatabase::new(&rule_body_cup_rule_head_unmapped);
            let rule_head = rule.head().iter().map(|a| a.into()).collect::<HashSet<_>>();
            if !interpretation_pre_db.entails(&rule.existentials(), &rule_head) {
                log::trace!("found problem piece");
                return Some(Reliance::default());
            }
        }
    }
    // NOTE: with this pre-check, we know that identity atom mappings do not need to be considered any more

    extend_init::<HeadAtom>(
        mem,
        rule_index,
        rule_index,
        check_self_restr,
        previous_opt,
        Substitution::default(),
    )
}
