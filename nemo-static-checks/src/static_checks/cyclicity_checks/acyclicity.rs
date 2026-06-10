use nemo::rule_model::{
    components::{
        atom::Atom,
        fact::Fact,
        rule::Rule,
        tag::Tag,
        term::{
            Cyclic, Term,
            function::FunctionTerm,
            primitive::{Primitive, variable::Variable},
        },
    },
    pipeline::transformations::{
        crit_instance::TransformationCriticalInstance, skolem::TransformationSkolemize,
    },
    programs::{ProgramRead, handle::ProgramHandle},
};

use crate::static_checks::collection_traits::InsertAll;
use crate::static_checks::cyclicity_checks::{
    Assignment, CoreReasoner, CyclicityStrategy, NoBlockStrategy, StrategySelector,
    VarPerAtomIdxPosIdxPerRule, assignments_for_facts, backtrack_sk_term, body_for_assignment,
    build_var_index_for_rule, build_var_index_for_rules, predicates_ref, reverse_sk, union,
};
use crate::static_checks::rule_set::RuleSet;

use std::collections::{HashMap, HashSet};

pub fn mfa_handle(handle: ProgramHandle) -> ProgramHandle {
    handle
        .transform(TransformationCriticalInstance::default())
        .expect("TransformationCriticalInstance Error")
        .transform(TransformationSkolemize::default())
        .expect("TransformationSkolemize Error")
}

pub struct MFAStrategy;

impl CyclicityStrategy for MFAStrategy {
    fn is_blocked(&self, _rule: &Rule, _ass: &Assignment) -> bool {
        false
    }
}

pub struct RMFAStrategy<'a> {
    pub existential_rules: &'a Vec<&'a Rule>,
    pub datalog_rules: &'a Vec<&'a Rule>,
    pub datalog_pr: &'a HashSet<&'a Tag>,
    pub var_per_atom_idx_pos_idx_per_rule: &'a VarPerAtomIdxPosIdxPerRule<'a>,
}

impl<'a> RMFAStrategy<'a> {
    fn new(
        existential_rules: &'a Vec<&'a Rule>,
        datalog_rules: &'a Vec<&'a Rule>,
        datalog_pr: &'a HashSet<&'a Tag>,
        var_per_atom_idx_pos_idx_per_rule: &'a VarPerAtomIdxPosIdxPerRule<'a>,
    ) -> Self {
        Self {
            existential_rules,
            datalog_rules,
            datalog_pr,
            var_per_atom_idx_pos_idx_per_rule,
        }
    }
}

impl<'a> CyclicityStrategy for RMFAStrategy<'a> {
    fn is_blocked(&self, rule: &Rule, ass: &Assignment) -> bool {
        let renamed_ass = rename_consts(ass);
        let body_for_renamed_consts = body_for_assignment(rule, &renamed_ass);
        let skolem_terms_in_body = get_skolem_terms(&body_for_renamed_consts);
        let mut special_reasoning_set = special_reasoning_set(
            self.existential_rules,
            body_for_renamed_consts,
            skolem_terms_in_body,
        );

        let no_bl_strat = NoBlockStrategy;
        let mut datalog_reasoner = CoreReasoner::new(
            self.datalog_pr,
            self.datalog_rules,
            self.var_per_atom_idx_pos_idx_per_rule,
            &no_bl_strat,
        );
        let mut facts_after_reasoning = special_reasoning_set.clone();
        if !self.datalog_rules.is_empty() {
            filter_new_facts(&mut special_reasoning_set, self.datalog_pr);
            datalog_reasoner.run_saturating(special_reasoning_set);
            facts_after_reasoning = union(facts_after_reasoning, datalog_reasoner.into_facts());
        }

        let preds_in_head: Vec<&Tag> = rule
            .head()
            .iter()
            .map(|atom| atom.predicate_ref())
            .collect();

        preds_in_head.iter().for_each(|pred| {
            if !facts_after_reasoning.contains_key(pred) {
                facts_after_reasoning.insert(pred, HashSet::default());
            }
        });

        let count_preds_body = rule.body().iter().count();
        let unsk_rule = reverse_sk(rule);
        let var_atom_pos_unsk_rule = build_var_index_for_rule(&unsk_rule);

        let possible_ass_for_chase_result = assignments_for_facts(
            count_preds_body,
            preds_in_head,
            &facts_after_reasoning,
            &facts_after_reasoning,
            &var_atom_pos_unsk_rule,
        );

        let frontier_vars = rule.frontier_variables();
        possible_ass_for_chase_result.iter().any(|ass| {
            frontier_vars
                .iter()
                .all(|var| renamed_ass.get(var).unwrap() == ass.get(var).unwrap())
        })
    }
}

fn convert_set_to_map(facts: Vec<&Fact>) -> HashMap<&Tag, HashSet<Fact>> {
    facts.into_iter().fold(
        HashMap::<&Tag, HashSet<Fact>>::new(),
        |mut ret_val, fact| {
            ret_val
                .entry(fact.predicate())
                .and_modify(|facts_of_pred| {
                    facts_of_pred.insert(fact.clone());
                })
                .or_insert(HashSet::from([fact.clone()]));
            ret_val
        },
    )
}

pub async fn check_acyclicity(handle: ProgramHandle, strat_sel: StrategySelector) -> bool {
    let rules: RuleSet = RuleSet(handle.rules().cloned().collect());
    let rules_ref: Vec<&Rule> = rules.0.iter().collect();

    let new_facts_by_pred: HashMap<&Tag, HashSet<Fact>> =
        convert_set_to_map(handle.facts().collect());

    let datalog_rules: Vec<&Rule> = rules.datalog_rules();
    let existential_rules: Vec<&Rule> = rules.existential_rules();

    let datalog_pr: HashSet<&Tag> = predicates_ref(&datalog_rules);
    let existential_pr: HashSet<&Tag> = predicates_ref(&existential_rules);

    let var_per_atom_idx_pos_idx_per_rule = build_var_index_for_rules(&rules_ref);

    let no_bl_strat = NoBlockStrategy;

    let mut datalog_reasoner: CoreReasoner = CoreReasoner::new(
        &datalog_pr,
        &datalog_rules,
        &var_per_atom_idx_pos_idx_per_rule,
        &no_bl_strat,
    );

    let strat: &dyn CyclicityStrategy = match strat_sel {
        StrategySelector::MFA => &MFAStrategy,
        StrategySelector::RMFA => &RMFAStrategy::new(
            &existential_rules,
            &datalog_rules,
            &datalog_pr,
            &var_per_atom_idx_pos_idx_per_rule,
        ),
        _ => unreachable!(),
    };

    let mut existential_reasoner: CoreReasoner = CoreReasoner::new(
        &existential_pr,
        &existential_rules,
        &var_per_atom_idx_pos_idx_per_rule,
        strat,
    );

    let mut new_datalog_facts_by_pred: HashMap<&Tag, HashSet<Fact>> = new_facts_by_pred
        .iter()
        .filter(|(pred, _)| datalog_pr.contains(*pred))
        .map(|(pred, facts)| (*pred, facts.clone()))
        .collect();
    let mut new_existential_facts_by_pred: HashMap<&Tag, HashSet<Fact>> = new_facts_by_pred
        .iter()
        .filter(|(pred, _)| existential_pr.contains(*pred))
        .map(|(pred, facts)| (*pred, facts.clone()))
        .collect();

    while !new_existential_facts_by_pred.is_empty() {
        new_existential_facts_by_pred = union(
            datalog_reasoner.run_saturating(new_datalog_facts_by_pred),
            new_existential_facts_by_pred,
        );
        filter_new_facts(&mut new_existential_facts_by_pred, &existential_pr);
        new_existential_facts_by_pred =
            existential_reasoner.run_every_rule_once(&new_existential_facts_by_pred);

        new_datalog_facts_by_pred = new_existential_facts_by_pred
            .iter()
            .filter(|(pred, _)| datalog_pr.contains(*pred))
            .map(|(pred, facts)| (*pred, facts.clone()))
            .collect();

        if new_existential_facts_by_pred
            .values()
            .flatten()
            .any(|fact| fact.is_cyclic(&mut Vec::default()))
        {
            return false;
        }
    }
    true
}

fn filter_new_facts(facts_by_pred: &mut HashMap<&Tag, HashSet<Fact>>, preds: &HashSet<&Tag>) {
    facts_by_pred.retain(|pred, _| preds.contains(pred))
}

fn rename_consts_in_term(term: Term, count: &mut u32) -> Term {
    match term {
        Term::Primitive(Primitive::Ground(_)) => {
            *count += 1;
            Term::from(format!("__STAR_{count}__"))
        }
        Term::FunctionTerm(func_term) => {
            let name: String = func_term.tag().name().to_string();
            let inner_terms = func_term
                .into_terms()
                .map(|inner_term| rename_consts_in_term(inner_term, count));
            let new_func_term = FunctionTerm::new(&name, inner_terms);
            Term::from(new_func_term)
        }
        _ => panic!("not possible"),
    }
}

fn rename_consts<'a>(ass: &Assignment<'a>) -> Assignment<'a> {
    let mut count = 0;
    ass.clone()
        .into_iter()
        .map(|(var, term)| (var, rename_consts_in_term(term, &mut count)))
        .collect()
}

// fn get_skolem_terms(facts_by_pred: &HashMap<&Tag, HashSet<Fact>>) -> Vec<FunctionTerm> {
//     facts_by_pred
//         .values()
//         .fold(Vec::<FunctionTerm>::new(), |mut ret_val, facts| {
//             facts.iter().for_each(|fact| {
//                 let skolem_terms_of_fact: Vec<FunctionTerm> = fact
//                     .terms()
//                     .filter_map(|term| {
//                         if let Term::FunctionTerm(sk_term) = term {
//                             Some(sk_term)
//                         } else {
//                             None
//                         }
//                     })
//                     .cloned()
//                     .collect();
//                 ret_val.insert_all(&skolem_terms_of_fact);
//             });
//             ret_val
//         })
// }

fn get_skolem_terms(facts_by_pred: &HashMap<&Tag, HashSet<Fact>>) -> Vec<Term> {
    facts_by_pred
        .values()
        .fold(Vec::<Term>::new(), |mut ret_val, facts| {
            facts.iter().for_each(|fact| {
                let skolem_terms_of_fact: Vec<Term> = fact
                    .terms()
                    .filter_map(|term| {
                        if let Term::FunctionTerm(_) = term {
                            Some(term)
                        } else {
                            None
                        }
                    })
                    .cloned()
                    .collect();
                ret_val.insert_all(&skolem_terms_of_fact);
            });
            ret_val
        })
}

fn special_reasoning_set<'a>(
    ex_rules: &Vec<&'a Rule>,
    facts_by_pred: HashMap<&'a Tag, HashSet<Fact>>,
    skolem_terms: Vec<Term>,
) -> HashMap<&'a Tag, HashSet<Fact>> {
    let involved_facts_in_derivation =
        skolem_terms
            .iter()
            .fold(HashMap::<&Tag, HashSet<Fact>>::new(), |facts, sk_term| {
                let facts_for_sk_term = backtrack_sk_term(sk_term, ex_rules, true);
                union(facts, facts_for_sk_term)
            });
    union(facts_by_pred, involved_facts_in_derivation)
}
