use nemo::rule_model::{
    components::{
        IterableVariables,
        atom::Atom,
        fact::Fact,
        rule::Rule,
        tag::Tag,
        term::{
            Term,
            function::FunctionTerm,
            primitive::{Primitive, variable::Variable},
        },
    },
    pipeline::transformations::{
        crit_instance::TransformationCriticalInstance, skolem::TransformationSkolemize,
    },
    programs::{ProgramRead, handle::ProgramHandle},
};

use crate::static_checks::cyclicity_checks::{
    Assignment, CoreReasoner, Cyclic, CyclicityStrategy, FactsByPred, StrategySelector, Trigger,
    VarPerAtomIdxPosIdxPerRule, assignments_for_facts, backtrack_sk_term, body_for_assignment,
    build_var_index_for_rule, build_var_index_for_rules, head_for_assignment, predicates_ref,
    predicates_ref_and_lens, reverse_sk, union,
};

use crate::static_checks::collection_traits::InsertAll;
use crate::static_checks::rule_properties::RuleProperties;
use crate::static_checks::rule_set::{RuleRefs, RuleSet};

use std::collections::{HashMap, HashSet};

pub struct MFCStrategy;

impl CyclicityStrategy for MFCStrategy {
    fn is_blocked(&self, trig: Trigger) -> bool {
        trig.ass()
            .values()
            .any(|term| term.is_cyclic(&mut Vec::default()))
    }
}

struct OverapproximationStrategy<'a> {
    rule: &'a Rule,
    skel_of_trig: &'a Vec<Term>,
    head_for_ass: &'a FactsByPred<'a>,
}

impl<'a> OverapproximationStrategy<'a> {
    fn new(rule: &'a Rule, skel_of_trig: &'a Vec<Term>, head_for_ass: &'a FactsByPred<'a>) -> Self {
        Self {
            rule,
            skel_of_trig,
            head_for_ass,
        }
    }
}

impl CyclicityStrategy for OverapproximationStrategy<'_> {
    fn is_blocked(&self, trig: Trigger) -> bool {
        if self.rule != trig.rule() {
            return false;
        }
        &head_for_assignment(trig.rule(), trig.ass()) == self.head_for_ass
    }

    fn map<'a>(&self, facts_by_pred: FactsByPred<'a>) -> FactsByPred<'a> {
        let star_const = Term::from("__STAR__");
        facts_by_pred
            .into_iter()
            .map(|(pred, facts)| {
                (
                    pred,
                    facts
                        .into_iter()
                        .map(|mut fact| {
                            fact.terms_mut().for_each(|term| {
                                if !self.skel_of_trig.contains(&*term) {
                                    *term = star_const.clone();
                                }
                            });
                            fact
                        })
                        .collect(),
                )
            })
            .collect()
    }
}

pub struct DRPCStrategy<'a> {
    rule: &'a Rule,
    rule_set: &'a Vec<&'a Rule>,
    ex_rule_set: Vec<&'a Rule>,
    var_per_atom_idx_pos_idx_per_rule: &'a VarPerAtomIdxPosIdxPerRule<'a>,
}

impl<'a> DRPCStrategy<'a> {
    fn new(
        rule: &'a Rule,
        rule_set: &'a Vec<&'a Rule>,
        var_per_atom_idx_pos_idx_per_rule: &'a VarPerAtomIdxPosIdxPerRule<'a>,
    ) -> Self {
        Self {
            rule,
            rule_set,
            ex_rule_set: rule_set
                .iter()
                .filter(|r| r.contains_func())
                .copied()
                .collect(),
            var_per_atom_idx_pos_idx_per_rule,
        }
    }

    fn is_star_unblockable(&self, trig: &Trigger) -> bool {
        let head_for_ass = head_for_assignment(trig.rule(), trig.ass());
        let h_star_operapproximation = self.h_star_overapproximation(trig, &head_for_ass);

        !trig.is_obsolete(&h_star_operapproximation)
    }

    fn h_star_overapproximation(
        &'a self,
        trig: &Trigger,
        head_for_ass: &FactsByPred<'_>,
    ) -> FactsByPred<'a> {
        let backtrack_of_trigger = backtrack_trigger(&self.ex_rule_set, trig);

        let mut skeleton_of_trigger = skeleton_of_trigger_backtrack(&backtrack_of_trigger);
        let star_const = Term::from("__STAR__");
        skeleton_of_trigger.push(star_const);

        let preds_and_lens = predicates_ref_and_lens(self.rule_set);
        let preds = preds_and_lens.iter().map(|(pred, _)| *pred).collect();

        let possible_facts_for_preds_and_skeleton_consts =
            possible_facts(preds_and_lens, &skeleton_of_trigger);

        let overapprox_strat =
            OverapproximationStrategy::new(trig.rule(), &skeleton_of_trigger, head_for_ass);

        let mut reasoner = CoreReasoner::new(
            &preds,
            self.rule_set,
            self.var_per_atom_idx_pos_idx_per_rule,
            &overapprox_strat,
        );

        let start_overapproximation = union(
            backtrack_of_trigger,
            possible_facts_for_preds_and_skeleton_consts,
        );
        reasoner.run_saturating(start_overapproximation);

        reasoner.into_facts()
    }
}

impl CyclicityStrategy for DRPCStrategy<'_> {
    fn is_blocked(&self, trig: Trigger) -> bool {
        let no_cyclic_terms_in_ass = !trig
            .ass()
            .values()
            .any(|term| term.is_cyclic(&mut Vec::default()));
        let is_star_unblockable = self.is_star_unblockable(&trig);
        let ass_injective = if trig.rule() == self.rule {
            ass_is_injective(trig.ass())
        } else {
            true
        };
        !(no_cyclic_terms_in_ass && is_star_unblockable && ass_injective)
    }
}

fn possible_facts<'a>(
    preds_and_lens: HashSet<(&'a Tag, usize)>,
    skeleton: &[Term],
) -> FactsByPred<'a> {
    let skeleton_consts = skeleton.iter().filter(|term| !term.is_function()).collect();
    preds_and_lens
        .into_iter()
        .fold(FactsByPred::new(), |mut ret_val, (pred, size)| {
            let mut term_sequences = Vec::new();
            construct_term_sequences_rec(
                &mut term_sequences,
                &mut Vec::new(),
                &skeleton_consts,
                size,
            );
            let facts = term_sequences
                .into_iter()
                .map(|seq| Fact::from((pred, seq)))
                .collect();
            ret_val.insert(pred, facts);
            ret_val
        })
}

fn construct_term_sequences_rec(
    ret_val: &mut Vec<Vec<Term>>,
    cur_seq: &mut Vec<Term>,
    skeleton_consts: &Vec<&Term>,
    rem_size: usize,
) {
    if rem_size == 0 {
        ret_val.push(cur_seq.clone());
        return;
    }
    skeleton_consts.iter().for_each(|cons| {
        cur_seq.push((*cons).clone());
        construct_term_sequences_rec(ret_val, cur_seq, skeleton_consts, rem_size - 1);
        cur_seq.pop();
    })
}

fn skeleton_of_trigger_backtrack(backtrack: &FactsByPred) -> Vec<Term> {
    backtrack
        .values()
        .fold(Vec::<Term>::new(), |ret_val, facts| {
            let terms = facts
                .iter()
                .flat_map(|fact| fact.terms().cloned().collect::<Vec<Term>>())
                .collect();
            ret_val.insert_all_take_ret(terms)
        })
}

fn backtrack_trigger<'a>(existential_rules: &Vec<&'a Rule>, trig: &Trigger) -> FactsByPred<'a> {
    // NOTE: MAYBE MOVE FRONT_VARS INTO DRPC STRUCT TO AVOID RECOMPUTING
    let front_vars = trig.rule().frontier_variables();
    front_vars
        .into_iter()
        .fold(FactsByPred::new(), |ret_val, var| {
            let term = trig.ass().get(var).unwrap();
            let facts_of_ass_var = backtrack_sk_term(term, existential_rules, false);
            union(ret_val, facts_of_ass_var)
        })
}

fn ass_is_injective(ass: &Assignment) -> bool {
    let mut seen_terms: HashSet<&Term> = HashSet::new();
    ass.values().all(|term| seen_terms.insert(term))
}

pub fn mfc_handle(handle: ProgramHandle) -> ProgramHandle {
    handle
        .transform(TransformationSkolemize::default())
        .expect("TransformationSkolemize Error")
}

pub async fn check_cyclicity(handle: ProgramHandle, strat: StrategySelector) -> bool {
    let rule_set: RuleSet = RuleSet(handle.rules().cloned().collect());
    let det_rules: Vec<&Rule> = rule_set.0.iter().collect();
    let ex_rules: Vec<&Rule> = rule_set.existential_rules();

    for rule in ex_rules.iter().filter(|rule| rule.contains_func()) {
        if check_cyclicity_for_rule(rule, &det_rules, &strat).await {
            return true;
        }
    }
    false
}

pub async fn check_cyclicity_for_rule(
    rule: &Rule,
    rule_set: &Vec<&Rule>,
    strat: &StrategySelector,
) -> bool {
    let unique_ass: Assignment = unique_ass(rule);

    let mfc_set = union(
        head_for_assignment(rule, &unique_ass),
        body_for_assignment(rule, &unique_ass),
    );

    let preds: HashSet<&Tag> = predicates_ref(rule_set);
    let var_per_atom_idx_pos_idx_per_rule = build_var_index_for_rules(rule_set);

    let strat: &dyn CyclicityStrategy = match strat {
        StrategySelector::MFC => &MFCStrategy,
        StrategySelector::DRPC => {
            &DRPCStrategy::new(rule, rule_set, &var_per_atom_idx_pos_idx_per_rule)
        }
        _ => unreachable!(),
    };

    let mut reasoner: CoreReasoner =
        CoreReasoner::new(&preds, rule_set, &var_per_atom_idx_pos_idx_per_rule, strat);

    reasoner.run_saturating(mfc_set);

    let sk_func_tags_of_rule: Vec<&Tag> = rule
        .head_terms()
        .filter_map(|term| {
            if let Term::FunctionTerm(f_term) = term {
                Some(f_term.tag())
            } else {
                None
            }
        })
        .collect();

    reasoner
        .facts()
        .values()
        .flatten()
        .any(|fact| fact.is_rule_cyclic(&mut Vec::default(), &sk_func_tags_of_rule))
}

fn unique_ass(rule: &'_ Rule) -> Assignment<'_> {
    let mut count = 0;
    rule.positive_variables_iter()
        .fold(Assignment::new(), |mut ass, var| {
            let fresh_const = Term::from(format!("__FR_CONST_{count}__"));
            count += 1;
            ass.insert(var, fresh_const);
            ass
        })
}
