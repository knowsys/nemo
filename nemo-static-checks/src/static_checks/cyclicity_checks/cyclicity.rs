use nemo::rule_model::{
    components::{
        IterableVariables,
        atom::Atom,
        fact::Fact,
        rule::Rule,
        tag::Tag,
        term::{
            Cyclic, RuleCyclic, Term,
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
    Assignment, CoreReasoner, CyclicityStrategy, StrategySelector, VarPerAtomIdxPosIdxPerRule,
    assignments_for_facts, backtrack_sk_term, body_for_assignment, build_var_index_for_rule,
    build_var_index_for_rules, head_for_assignment, predicates_ref, predicates_ref_and_lens,
    reverse_sk, union,
};

use crate::static_checks::collection_traits::InsertAll;
use crate::static_checks::rule_properties::RuleProperties;
use crate::static_checks::rule_set::{RuleRefs, RuleSet};

use std::collections::{HashMap, HashSet};

pub struct MFCStrategy;

impl CyclicityStrategy for MFCStrategy {
    fn is_blocked(&self, _rule: &Rule, ass: &Assignment) -> bool {
        ass.values().any(|term| term.is_cyclic(&mut Vec::default()))
    }
}

struct OverapproximationStrategy<'a> {
    rule: &'a Rule,
    ass: &'a Assignment<'a>,
    skel_of_trig: &'a Vec<Term>,
    head_for_ass: &'a HashMap<&'a Tag, HashSet<Fact>>,
}

impl<'a> OverapproximationStrategy<'a> {
    fn new(
        rule: &'a Rule,
        ass: &'a Assignment<'a>,
        skel_of_trig: &'a Vec<Term>,
        head_for_ass: &'a HashMap<&'a Tag, HashSet<Fact>>,
    ) -> Self {
        Self {
            rule,
            ass,
            skel_of_trig,
            head_for_ass,
        }
    }
}

impl CyclicityStrategy for OverapproximationStrategy<'_> {
    fn is_blocked(&self, rule: &Rule, ass: &Assignment) -> bool {
        if self.rule != rule {
            return false;
        }
        &head_for_assignment(rule, ass) == self.head_for_ass
    }

    fn h<'a>(
        &self,
        facts_by_pred: HashMap<&'a Tag, HashSet<Fact>>,
    ) -> HashMap<&'a Tag, HashSet<Fact>> {
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

    fn is_star_unblockable(&self, rule: &Rule, ass: &Assignment) -> bool {
        // TODO: IF WE ONLY ITERATE OVER EXISTENTIAL RULES THIS IS UNNECCESARY
        if !rule.contains_func() {
            return true;
        }

        let head_for_ass = head_for_assignment(rule, ass);
        let h_star_operapproximation = self.h_star_overapproximation(rule, ass, &head_for_ass);
        // print!("  OVERAPPROX:  ");
        // h_star_operapproximation
        //     .values()
        //     .flatten()
        //     .for_each(|fact| {
        //         print!("  {fact}  ");
        //     });

        // head_for_ass
        //     .iter()
        //     .any(|(pred, facts)| !facts.is_subset(h_star_operapproximation.get(pred).unwrap()))
        let preds_in_head: Vec<&Tag> = rule
            .head()
            .iter()
            .map(|atom| atom.predicate_ref())
            .collect();

        // preds_in_head.iter().for_each(|pred| {
        //     if !h_star_operapproximation.contains_key(pred) {
        //         h_star_overapproximation.insert(pred, HashSet::default());
        //     }
        // });

        let count_preds_body = rule.body().iter().count();
        let unsk_rule = reverse_sk(rule);
        let var_atom_pos_unsk_rule = build_var_index_for_rule(&unsk_rule);

        let possible_ass_for_chase_result = assignments_for_facts(
            count_preds_body,
            preds_in_head,
            &h_star_operapproximation,
            &h_star_operapproximation,
            &var_atom_pos_unsk_rule,
        );

        let frontier_vars = rule.frontier_variables();
        !possible_ass_for_chase_result.iter().any(|pos_ass| {
            frontier_vars
                .iter()
                .all(|var| ass.get(var).unwrap() == pos_ass.get(var).unwrap())
        })
    }

    fn h_star_overapproximation(
        &'a self,
        rule: &Rule,
        ass: &Assignment,
        head_for_ass: &HashMap<&'a Tag, HashSet<Fact>>,
    ) -> HashMap<&'a Tag, HashSet<Fact>> {
        let backtrack_of_trigger = backtrack_trigger(&self.ex_rule_set, rule, ass);

        let mut skeleton_of_trigger = skeleton_of_trigger_backtrack(&backtrack_of_trigger);
        let star_const = Term::from("__STAR__");
        skeleton_of_trigger.push(star_const);

        let preds_and_lens = predicates_ref_and_lens(self.rule_set);
        let preds = preds_and_lens.iter().map(|(pred, _)| *pred).collect();

        let possible_facts_for_preds_and_skeleton_consts =
            possible_facts(preds_and_lens, &skeleton_of_trigger);

        let overapprox_strat =
            OverapproximationStrategy::new(rule, ass, &skeleton_of_trigger, head_for_ass);

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
    fn is_blocked(&self, rule: &Rule, ass: &Assignment) -> bool {
        let no_cyclic_terms_in_ass = !ass.values().any(|term| term.is_cyclic(&mut Vec::default()));
        // print!("  {no_cyclic_terms_in_ass}  ");
        let is_star_unblockable = self.is_star_unblockable(rule, ass);
        // print!("  {is_star_unblockable}  ");
        let ass_injective = if rule == self.rule {
            ass_is_injective(ass)
        } else {
            true
        };
        // ass.iter().for_each(|(var, term)| {
        //     print!("  {var} |-> {term}  ");
        // });
        // print!("  star_unblock: {is_star_unblockable}  ");
        // print!(
        //     "  all: {}  ",
        //     !(no_cyclic_terms_in_ass && is_star_unblockable && ass_injective)
        // );

        // print!("  {ass_injective}  ");
        !(no_cyclic_terms_in_ass && is_star_unblockable && ass_injective)
    }
}

fn possible_facts<'a>(
    preds_and_lens: HashSet<(&'a Tag, usize)>,
    skeleton: &Vec<Term>,
) -> HashMap<&'a Tag, HashSet<Fact>> {
    let skeleton_consts = skeleton.iter().filter(|term| !term.is_function()).collect();
    preds_and_lens.into_iter().fold(
        HashMap::<&Tag, HashSet<Fact>>::new(),
        |mut ret_val, (pred, size)| {
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
        },
    )
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

fn skeleton_of_trigger_backtrack(backtrack: &HashMap<&Tag, HashSet<Fact>>) -> Vec<Term> {
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

fn backtrack_trigger<'a>(
    existential_rules: &Vec<&'a Rule>,
    rule: &Rule,
    ass: &Assignment,
) -> HashMap<&'a Tag, HashSet<Fact>> {
    // NOTE: MAYBE MOVE FRONT_VARS INTO DRPC STRUCT TO AVOID RECOMPUTING
    let front_vars = rule.frontier_variables();
    front_vars
        .into_iter()
        .fold(HashMap::<&Tag, HashSet<Fact>>::new(), |ret_val, var| {
            let term = ass.get(var).unwrap();
            let facts_of_ass_var = backtrack_sk_term(term, existential_rules, false);
            union(ret_val, facts_of_ass_var)
        })
}

// fn h_star_func<'a>(rule_set: &Vec<&Rule>, ass: &'a Assignment<'_>) -> HashMap<&'a Term, &'a Term> {
//     todo!();
// }

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

    for rule in det_rules.iter().filter(|rule| rule.contains_func()) {
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
