use std::collections::HashSet;

use crate::execution::selection_strategy::strategy_full_chain_stratification::util::atom::{
    Atom, Predicate,
};
use crate::rule_model::components::IterablePrimitives;
use crate::rule_model::substitution::{Substitute, Substitution};
use crate::{
    execution::planning::normalization::{
        atom::{body::BodyAtom, head::HeadAtom},
        rule::NormalizedRule,
    },
    rule_model::components::term::{
        Term,
        primitive::{Primitive, variable::Variable},
    },
};

use crate::execution::selection_strategy::strategy_full_chain_stratification::reliance_memoization::RuleMemoization;
use crate::execution::selection_strategy::strategy_full_chain_stratification::util::extend::{AtomMapping, CheckResult, Reliance, extend_init, mapped, maxidx};
use crate::execution::selection_strategy::strategy_full_chain_stratification::util::database::RepresentativeDatabase;

impl IterablePrimitives for BodyAtom {
    fn primitive_terms<'a>(&'a self) -> Box<dyn Iterator<Item = &'a Primitive> + 'a> {
        todo!()
    }

    fn primitive_terms_mut<'a>(
        &'a mut self,
    ) -> Box<dyn Iterator<Item = &'a mut Self::TermType> + 'a> {
        todo!()
    }
}

impl IterablePrimitives for HeadAtom {
    fn primitive_terms<'a>(&'a self) -> Box<dyn Iterator<Item = &'a Primitive> + 'a> {
        todo!()
    }

    fn primitive_terms_mut<'a>(
        &'a mut self,
    ) -> Box<dyn Iterator<Item = &'a mut Self::TermType> + 'a> {
        todo!()
    }
}

impl IterablePrimitives for Variable {
    fn primitive_terms<'a>(&'a self) -> Box<dyn Iterator<Item = &'a Primitive> + 'a> {
        todo!()
    }

    fn primitive_terms_mut<'a>(
        &'a mut self,
    ) -> Box<dyn Iterator<Item = &'a mut Self::TermType> + 'a> {
        todo!()
    }
}

impl<T: IterablePrimitives> IterablePrimitives for HashSet<&T> {
    fn primitive_terms<'a>(&'a self) -> Box<dyn Iterator<Item = &'a Primitive> + 'a> {
        Box::from(self.iter().flat_map(|t| t.primitive_terms()))
    }

    fn primitive_terms_mut<'a>(
        &'a mut self,
    ) -> Box<dyn Iterator<Item = &'a mut Self::TermType> + 'a> {
        todo!()
    }
}

fn apply_to_variables(eta: &Substitution, vars: HashSet<&Variable>) -> HashSet<Variable> {
    vars.into_iter()
        .filter_map(|v| {
            if let Some(Primitive::Variable(v_out)) = eta.get(&Primitive::Variable(v.clone())) {
                Some(v_out)
            } else {
                None
            }
        })
        .collect()
}

struct SubstitutedAtom {
    predicate: Predicate,
    terms: Vec<Term>,
}

impl From<&BodyAtom> for SubstitutedAtom {
    fn from(value: &BodyAtom) -> Self {
        Self {
            predicate: value.predicate().name().to_string(),
            terms: value
                .terms()
                .map(|v| Term::Primitive(Primitive::Variable(v.clone())))
                .collect(),
        }
    }
}

impl From<&HeadAtom> for SubstitutedAtom {
    fn from(value: &HeadAtom) -> Self {
        Self {
            predicate: value.predicate().name().to_string(),
            terms: value.terms().map(|p| Term::Primitive(p.clone())).collect(),
        }
    }
}

impl Substitute for Term {
    fn substitute(&mut self, eta: &Substitution) -> &mut Self {
        if let Term::Primitive(prim) = self {
            if let Some(p) = eta.get(prim) {
                *self = Term::Primitive(p);
            }
        }
        self
    }
}

impl Substitute for SubstitutedAtom {
    fn substitute(&mut self, eta: &Substitution) -> &mut Self {
        self.terms.iter_mut().for_each(|t| {
            t.substitute(eta);
        });
        self
    }
}

fn check_posr(
    rule1: &NormalizedRule,
    rule2: &NormalizedRule,
    mu: &AtomMapping,
    eta: &Substitution,
) -> CheckResult {
    let mut r1_universals_eta = rule1.universals();
    eta.apply(&mut r1_universals_eta);
    let r1_existentials = rule1.existentials();

    // mu failed if eta assigned a variable in rule1.body to an existential
    if r1_existentials.iter().any(|v| eta.contains_var(*v)) {
        log::trace!("existential of rule1 mapped");
        return CheckResult::Reject;
    }
    if !r1_universals_eta.is_disjoint(&r1_existentials) {
        log::trace!(
            "universals of rule1 under eta are not disjoint from existentials of rule1 => mu failed"
        );
        return CheckResult::Reject;
    }

    let rule2_body_mapped_set = mapped(mu, &rule2.positive()).collect::<HashSet<_>>();

    let mu_maxidx = maxidx(mu);

    let rule2_body_unmapped_left = &rule2.positive()[..mu_maxidx]
        .iter()
        .collect::<HashSet<_>>()
        .difference(&rule2_body_mapped_set)
        .cloned()
        .collect::<HashSet<_>>();

    // mu failed if eta assigned a variable in the left unmapped portion of rule2.body to an existential
    let mut rule2_body_unmapped_left_vars_eta = rule2_body_unmapped_left
        .iter()
        .flat_map(|atom| atom.variables())
        .collect::<HashSet<_>>();
    eta.apply(&mut rule2_body_unmapped_left_vars_eta);
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
    let mut rule2_body_unmapped_right_vars_eta = rule2_body_unmapped_right
        .iter()
        .flat_map(|atom| atom.variables())
        .collect::<HashSet<_>>();
    eta.apply(&mut rule2_body_unmapped_right_vars_eta);
    if !rule2_body_unmapped_right_vars_eta.is_disjoint(&r1_existentials) {
        log::trace!(
            "variables of the right unmapped part of rule2 under eta are disjoint from existentials of rule1 => mu must be extended"
        );
        return CheckResult::Extend;
    }

    let mut rule1_body_eta = rule1.positive().iter().collect::<HashSet<_>>();
    eta.apply(&mut rule1_body_eta);

    let mut rule2_body_unmapped_eta = rule2
        .positive()
        .iter()
        .collect::<HashSet<_>>()
        .difference(&rule2_body_mapped_set)
        .cloned()
        .collect::<HashSet<_>>();
    eta.apply(&mut rule2_body_unmapped_eta);

    // construct representative interpretation I_a
    let rule1_body_eta_cup_rule2_body_unmapped_eta = rule1_body_eta.union(&rule2_body_unmapped_eta);
    let interpretation_a_db = RepresentativeDatabase::new(
        rule1_body_eta_cup_rule2_body_unmapped_eta
            .into_iter()
            .copied(),
    );
    //log::trace!(
    //    "{}",
    //    db_to_string(
    //        "I_a =",
    //        &rule1_body_eta_cup_rule2_body_unmapped_eta,
    //        &Database::new(&vec![])
    //    )
    //);

    // mu has to be extended if rule1 under eta is satisfied on I_a
    let mut rule1_head_eta = rule1.head().iter().collect::<HashSet<_>>();
    eta.apply(&mut rule1_head_eta);
    if interpretation_a_db.entails(&r1_existentials, rule1_head_eta.iter().copied()) {
        log::trace!("I_a models the head of rule1 under eta => mu must be extended");
        return CheckResult::Extend;
    }

    let mut rule2_body_eta = rule2.positive().iter().collect::<HashSet<_>>();
    eta.apply(&mut rule2_body_eta);

    // mu has to be extended if rule2_body under eta is fully contained in I_a
    if interpretation_a_db.contains(rule2_body_eta) {
        log::trace!("body of rule2 under eta already contained in I_a => mu must be extended");
        return CheckResult::Extend;
    }

    // construct representative interpretation I_b
    //log::trace!(
    //    "{}",
    //    db_to_string(
    //        &format!("I_b = I_a {CUP}"),
    //        &rule1_head_eta,
    //        &interpretation_a_db
    //    )
    //);
    let interpretation_b_db = RepresentativeDatabase::collect(rule1_head_eta, interpretation_a_db);

    // mu has failed if rule2 under eta is satisfied in I_b
    let mut r2_existentials = rule2.existentials();
    eta.apply(&mut r2_existentials);
    let mut rule2_head_eta = rule2.head().iter().collect::<HashSet<_>>();
    eta.apply(&mut rule2_head_eta);
    if interpretation_b_db.entails(&r2_existentials, rule2_head_eta) {
        log::trace!("I_b models head of rule2 under eta => mu failed");
        return CheckResult::Reject;
    }

    let mut r2_universals_eta = rule2.universals();
    eta.apply(&mut r2_universals_eta);
    for n in rule2.negative() {
        let mut n = n.clone();
        eta.apply(&mut n);
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
