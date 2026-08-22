use nemo::rule_model::{
    components::{rule::Rule, tag::Tag, term::Term},
    pipeline::transformations::{
        crit_instance::facts_for_predicate_and_constants,
        filter_rules::{RuleSelector, TransformationFilterRules},
        skolem::TransformationSkolemize,
    },
    programs::{ProgramRead, handle::ProgramHandle},
};

use crate::static_checks::cyclicity_checks::{
    Assignment, CoreReasoner, Cyclic, CyclicityStrategy, FactsByPred, ObsolescenceVariableIndices,
    Trigger, VarPerAtomIdxPosIdxPerRule, backtrack_sk_term, body_for_assignment,
    build_obsolescence_variable_indices, build_var_index_for_rules, head_for_assignment,
    predicates_ref, predicates_ref_and_lens, union,
};

use crate::static_checks::collection_traits::InsertAll;
use std::collections::HashSet;

#[derive(Clone, Copy)]
pub enum CyclicityStrategySelector {
    MFC,
    DRPC,
}

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
    rules: &'a Vec<&'a Rule>,
    ex_rules: &'a Vec<&'a Rule>,
    var_per_atom_idx_pos_idx_per_rule: &'a VarPerAtomIdxPosIdxPerRule<'a>,
    obsolescence_variable_indices: &'a ObsolescenceVariableIndices<'a>,
}

impl<'a> DRPCStrategy<'a> {
    fn new(
        rule: &'a Rule,
        rules: &'a Vec<&'a Rule>,
        ex_rules: &'a Vec<&'a Rule>,
        var_per_atom_idx_pos_idx_per_rule: &'a VarPerAtomIdxPosIdxPerRule<'a>,
        obsolescence_variable_indices: &'a ObsolescenceVariableIndices<'a>,
    ) -> Self {
        Self {
            rule,
            rules,
            ex_rules,
            var_per_atom_idx_pos_idx_per_rule,
            obsolescence_variable_indices,
        }
    }

    fn is_star_unblockable(&self, trig: &Trigger) -> bool {
        let head_for_ass = head_for_assignment(trig.rule(), trig.ass());
        let h_star_operapproximation = self.h_star_overapproximation(trig, &head_for_ass);
        let variable_index = self
            .obsolescence_variable_indices
            .get(trig.rule())
            .expect("every reasoning rule must have an obsolescence variable index");

        !trig.is_obsolete(variable_index, &h_star_operapproximation)
    }

    fn h_star_overapproximation(
        &'a self,
        trig: &Trigger,
        head_for_ass: &FactsByPred<'_>,
    ) -> FactsByPred<'a> {
        let backtrack_of_trigger = backtrack_trigger(self.ex_rules, trig);

        let mut skeleton_of_trigger = skeleton_of_trigger_backtrack(&backtrack_of_trigger);
        let star_const = Term::from("__STAR__");
        skeleton_of_trigger.push(star_const);

        let preds_and_lens = predicates_ref_and_lens(self.rules);
        let preds = preds_and_lens.iter().map(|(pred, _)| *pred).collect();

        let possible_facts_for_preds_and_skeleton_consts =
            possible_facts(preds_and_lens, &skeleton_of_trigger);

        let overapprox_strat =
            OverapproximationStrategy::new(trig.rule(), &skeleton_of_trigger, head_for_ass);

        let mut reasoner = CoreReasoner::new(
            &preds,
            self.rules,
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
    let skeleton_consts: Vec<Term> = skeleton
        .iter()
        .filter(|term| !term.is_function())
        .cloned()
        .collect();

    preds_and_lens
        .into_iter()
        .map(|(predicate, arity)| {
            let facts = facts_for_predicate_and_constants(predicate, arity, &skeleton_consts);
            (predicate, facts)
        })
        .collect()
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
            let facts_of_ass_var = backtrack_sk_term(term, existential_rules, 0, false);
            union(ret_val, facts_of_ass_var)
        })
}

fn ass_is_injective(ass: &Assignment) -> bool {
    let terms: Vec<&Term> = ass.values().collect();

    terms
        .iter()
        .enumerate()
        .all(|(index, term)| !terms[..index].contains(term))
}

pub async fn check_cyclicity(handle: &ProgramHandle, strat_sel: CyclicityStrategySelector) -> bool {
    let sk_ex_rules_handle = handle
        .transform(TransformationFilterRules(RuleSelector::Existential))
        .expect("TransformationFilterRules Error")
        .transform(TransformationSkolemize::default())
        .expect("TransformationSkolemize Error");
    let non_ex_rules_handle = handle
        .transform(TransformationFilterRules(RuleSelector::NonExistential))
        .expect("TransformationFilterRules Error");

    let det_rules: Vec<&Rule> = non_ex_rules_handle
        .rules()
        .chain(sk_ex_rules_handle.rules())
        .collect();
    let ex_rules: Vec<&Rule> = sk_ex_rules_handle.rules().collect();

    let preds: HashSet<&Tag> = predicates_ref(&det_rules);

    let var_per_atom_idx_pos_idx_per_rule = build_var_index_for_rules(&det_rules);
    let obsolescence_variable_indices =
        build_obsolescence_variable_indices(&sk_ex_rules_handle, &det_rules);

    for rule in ex_rules.iter() {
        let strat: &dyn CyclicityStrategy = match strat_sel {
            CyclicityStrategySelector::MFC => &MFCStrategy,
            CyclicityStrategySelector::DRPC => &DRPCStrategy::new(
                rule,
                &det_rules,
                &ex_rules,
                &var_per_atom_idx_pos_idx_per_rule,
                &obsolescence_variable_indices,
            ),
        };
        if check_cyclicity_for_rule(
            rule,
            &det_rules,
            &preds,
            &var_per_atom_idx_pos_idx_per_rule,
            strat,
        )
        .await
        {
            return true;
        }
    }
    false
}

pub async fn check_cyclicity_for_rule(
    rule: &Rule,
    rules: &Vec<&Rule>,
    preds: &HashSet<&Tag>,
    var_per_atom_idx_pos_idx_per_rule: &VarPerAtomIdxPosIdxPerRule<'_>,
    strat: &dyn CyclicityStrategy,
) -> bool {
    let unique_ass: Assignment = unique_ass(rule);

    let mfc_set = union(
        head_for_assignment(rule, &unique_ass),
        body_for_assignment(rule, &unique_ass),
    );

    let mut reasoner: CoreReasoner =
        CoreReasoner::new(&preds, rules, &var_per_atom_idx_pos_idx_per_rule, strat);

    reasoner.run_saturating(mfc_set);

    let sk_func_tags_of_rule: Vec<&Tag> = rule
        .head()
        .iter()
        .flat_map(|atom| atom.terms())
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
    rule.positive_variables()
        .fold(Assignment::new(), |mut ass, var| {
            let fresh_const = Term::from(format!("__FR_CONST_{count}__"));
            count += 1;
            ass.insert(var, fresh_const);
            ass
        })
}
