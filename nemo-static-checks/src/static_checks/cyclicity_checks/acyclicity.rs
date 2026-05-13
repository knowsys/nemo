use nemo::rule_model::{
    components::{
        IterableVariables,
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
    VarPerAtomIdxPosIdxPerRule, assignments_for_facts, body_for_assignment,
    build_var_index_for_rule, build_var_index_for_rules, head_for_assignment, predicates_ref,
    union,
};
use crate::static_checks::rule_set::{RuleRefs, RuleSet};

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
            facts_after_reasoning = union(
                facts_after_reasoning,
                datalog_reasoner.facts_by_pred.clone(),
            );
        }

        let preds_in_head: Vec<&Tag> = rule
            .head()
            .iter()
            .map(|atom| atom.predicate_ref())
            .collect();

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

impl RuleSet {
    fn datalog_rules(&self) -> Vec<&Rule> {
        self.0.iter().filter(|rule| !rule.contains_func()).collect()
    }

    fn existential_rules(&self) -> Vec<&Rule> {
        self.0.iter().filter(|rule| rule.contains_func()).collect()
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

fn get_skolem_terms(facts_by_pred: &HashMap<&Tag, HashSet<Fact>>) -> Vec<FunctionTerm> {
    facts_by_pred
        .values()
        .fold(Vec::<FunctionTerm>::new(), |mut ret_val, facts| {
            facts.iter().for_each(|fact| {
                let skolem_terms_of_fact: Vec<FunctionTerm> = fact
                    .terms()
                    .filter_map(|term| {
                        if let Term::FunctionTerm(sk_term) = term {
                            Some(sk_term)
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

fn facts_for_sk_term<'a>(
    sk_term: FunctionTerm,
    ex_rules: &Vec<&'a Rule>,
) -> HashMap<&'a Tag, HashSet<Fact>> {
    let (rule, unassigned_sk_term) = ex_rules
        .iter()
        .find_map(|rule| {
            rule.head_terms().find_map(|term| {
                if let Term::FunctionTerm(f_term) = term
                    && f_term.tag() == sk_term.tag()
                {
                    Some((rule, f_term))
                } else {
                    None
                }
            })
        })
        .unwrap();
    let front_vars = rule.frontier_variables();
    let mut const_count = 0;
    let ass = rule.variables().fold(Assignment::new(), |mut ass, var| {
        if front_vars.contains(var) {
            let index = unassigned_sk_term
                .terms()
                .enumerate()
                .find_map(|(i, term)| {
                    if let Term::Primitive(Primitive::Variable(var_in_sk)) = term
                        && var_in_sk == var
                    {
                        Some(i)
                    } else {
                        None
                    }
                })
                .unwrap();
            let g_term = sk_term.terms().nth(index).unwrap().clone();
            ass.insert(var, g_term);
        } else {
            const_count += 1;
            let fresh_const = Term::from(format!("__FRESH_CONST_{const_count}__"));
            ass.insert(var, fresh_const);
        }
        ass
    });
    let ret_val = union(
        head_for_assignment(rule, &ass),
        body_for_assignment(rule, &ass),
    );
    sk_term
        .into_terms()
        .filter_map(|term| {
            if let Term::FunctionTerm(inner_sk_term) = term {
                Some(inner_sk_term)
            } else {
                None
            }
        })
        .fold(ret_val, |ret_val, inner_sk_term| {
            union(ret_val, facts_for_sk_term(inner_sk_term, ex_rules))
        })
}

fn special_reasoning_set<'a>(
    ex_rules: &Vec<&'a Rule>,
    facts_by_pred: HashMap<&'a Tag, HashSet<Fact>>,
    skolem_terms: Vec<FunctionTerm>,
) -> HashMap<&'a Tag, HashSet<Fact>> {
    let involved_facts_in_derivation =
        skolem_terms
            .into_iter()
            .fold(HashMap::<&Tag, HashSet<Fact>>::new(), |facts, sk_term| {
                let facts_for_sk_term = facts_for_sk_term(sk_term, ex_rules);
                union(facts, facts_for_sk_term)
            });
    union(facts_by_pred, involved_facts_in_derivation)
}

fn reverse_sk_atom<'a>(
    atom: &'a Atom,
    sk_funcs_to_ex_vars: &mut HashMap<&'a FunctionTerm, Variable>,
    ex_var_count: &mut usize,
) -> Atom {
    let subterms: Vec<Term> = atom
        .terms()
        .map(|term| match term {
            Term::Primitive(prim) => Term::Primitive(prim.clone()),
            Term::FunctionTerm(sk_func) => {
                let ex_var: &mut Variable = sk_funcs_to_ex_vars.entry(sk_func).or_insert({
                    let var = Variable::existential(&format!("_NEXV_{}", ex_var_count));
                    *ex_var_count += 1;
                    var
                });
                Term::from(ex_var.clone())
            }
            _ => panic!(),
        })
        .collect();
    let pred: &Tag = atom.predicate_ref();
    Atom::from((pred, subterms))
}

fn reverse_sk(rule: &Rule) -> Rule {
    let body = rule.body().clone();
    let head = rule
        .head()
        .iter()
        .fold(Vec::<Atom>::new(), |mut head, atom| {
            let mut stored_sk_funcs_to_ex_vars: HashMap<&FunctionTerm, Variable> = HashMap::new();
            let mut ex_var_count = 0;
            let new_atom =
                reverse_sk_atom(atom, &mut stored_sk_funcs_to_ex_vars, &mut ex_var_count);
            head.push(new_atom);
            head
        });
    Rule::new(head, body)
}
