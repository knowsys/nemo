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
use crate::static_checks::rule_set::{RuleRefs, RuleSet};

use std::collections::{HashMap, HashSet};

pub fn mfa_handle(handle: ProgramHandle) -> ProgramHandle {
    handle
        .transform(TransformationCriticalInstance::default())
        .expect("TransformationCriticalInstance Error")
        .transform(TransformationSkolemize::default())
        .expect("TransformationSkolemize Error")
}

#[derive(Clone)]
pub enum ChaseVariant<'a, 'b> {
    SkolemMFA,
    SkolemDMFA,
    SkolemRestricted(&'b HashSet<&'a Tag>, &'b Vec<&'a Rule>),
}

impl RuleSet {
    fn datalog_rules(&self) -> Vec<&Rule> {
        self.0.iter().filter(|rule| !rule.contains_func()).collect()
    }

    fn existential_rules(&self) -> Vec<&Rule> {
        self.0.iter().filter(|rule| rule.contains_func()).collect()
    }
}

fn predicates_ref<'a>(rules: &[&'a Rule]) -> HashSet<&'a Tag> {
    rules.iter().fold(HashSet::<&Tag>::new(), |ret_val, rule| {
        ret_val.insert_all_take_ret(rule.predicates_ref())
    })
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

fn union<'a>(
    facts_by_pred: HashMap<&'a Tag, HashSet<Fact>>,
    other_facts_by_pred: HashMap<&'a Tag, HashSet<Fact>>,
) -> HashMap<&'a Tag, HashSet<Fact>> {
    other_facts_by_pred
        .into_iter()
        .fold(facts_by_pred, |mut ret_val, (pred, facts)| {
            ret_val
                .entry(pred)
                .and_modify(|cur_facts| cur_facts.insert_all_take(facts.clone()))
                .or_insert(facts);
            ret_val
        })
}

pub async fn check_acyclicity(handle: ProgramHandle, variant: ChaseVariant<'_, '_>) -> bool {
    let rules: RuleSet = RuleSet(handle.rules().cloned().collect());
    let rules_ref: Vec<&Rule> = rules.0.iter().collect();

    let new_facts_by_pred: HashMap<&Tag, HashSet<Fact>> =
        convert_set_to_map(handle.facts().collect());

    let datalog_rules: Vec<&Rule> = rules.datalog_rules();
    let existential_rules: Vec<&Rule> = rules.existential_rules();

    let datalog_pr: HashSet<&Tag> = predicates_ref(&datalog_rules);
    let existential_pr: HashSet<&Tag> = predicates_ref(&existential_rules);

    let var_per_atom_idx_pos_idx_per_rule = build_var_index_for_rules(&rules_ref);

    let mut datalog_reasoner: DatalogReasoner = DatalogReasoner::new(
        &datalog_pr,
        &datalog_rules,
        &var_per_atom_idx_pos_idx_per_rule,
    );

    let variant_with_data = match variant {
        ChaseVariant::SkolemRestricted(_, _) => {
            ChaseVariant::SkolemRestricted(&datalog_pr, &datalog_rules)
        }
        _ => variant,
    };

    let mut existential_reasoner: ExistentialReasoner = ExistentialReasoner::new(
        &existential_pr,
        &existential_rules,
        &var_per_atom_idx_pos_idx_per_rule,
        &variant_with_data,
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

fn assign_rec(sk_func: &FunctionTerm, ass: &Assignment) -> Term {
    let subterms: Vec<Term> = sk_func
        .terms()
        .map(|term| match term {
            Term::Primitive(Primitive::Variable(var)) => ass.get(var).unwrap().clone(),
            Term::FunctionTerm(sk_func) => assign_rec(sk_func, ass),
            _ => panic!(),
        })
        .collect();
    let pred: &Tag = sk_func.tag();
    Term::FunctionTerm(FunctionTerm::from((pred, subterms)))
}

fn assign(atom: &Atom, ass: &Assignment) -> Fact {
    let subterms: Vec<Term> = atom
        .terms()
        .map(|term| match term {
            Term::Primitive(Primitive::Variable(var)) => ass.get(var).unwrap().clone(),
            Term::FunctionTerm(sk_func) => assign_rec(sk_func, ass),
            _ => panic!(),
        })
        .collect();
    let pred: &Tag = atom.predicate_ref();
    Fact::from((pred, subterms))
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

fn assign_is_blocked_rmfa<'a>(
    rule: &Rule,
    ex_rules: &Vec<&Rule>,
    ass: &Assignment,
    datalog_preds: &HashSet<&'a Tag>,
    datalog_rules: &Vec<&'a Rule>,
    var_per_atom_idx_pos_idx_per_rule: &VarPerAtomIdxPosIdxPerRule,
) -> bool {
    let renamed_ass = rename_consts(ass);
    let body_for_renamed_consts = body_for_assignment(rule, &renamed_ass);
    let skolem_terms_in_body = get_skolem_terms(&body_for_renamed_consts);
    let mut special_reasoning_set =
        special_reasoning_set(ex_rules, body_for_renamed_consts, skolem_terms_in_body);

    let mut datalog_reasoner = DatalogReasoner::new(
        datalog_preds,
        datalog_rules,
        var_per_atom_idx_pos_idx_per_rule,
    );
    let mut facts_after_reasoning = special_reasoning_set.clone();
    if !datalog_rules.is_empty() {
        filter_new_facts(&mut special_reasoning_set, datalog_preds);
        datalog_reasoner.run_saturating(special_reasoning_set);
        facts_after_reasoning = union(
            facts_after_reasoning,
            datalog_reasoner.core.facts_by_pred.clone(),
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

fn assign_is_blocked(
    rule: &Rule,
    ex_rules: &Vec<&Rule>,
    ass: &Assignment,
    var_per_atom_idx_pos_idx_per_rule: &VarPerAtomIdxPosIdxPerRule,
    variant: &ChaseVariant,
) -> bool {
    match variant {
        ChaseVariant::SkolemMFA => false,
        ChaseVariant::SkolemRestricted(datalog_preds, datalog_rules) => assign_is_blocked_rmfa(
            rule,
            ex_rules,
            ass,
            datalog_preds,
            datalog_rules,
            var_per_atom_idx_pos_idx_per_rule,
        ),
        ChaseVariant::SkolemDMFA => panic!("not implemented yet"),
    }
}

fn facts_for_assignment<'a>(
    atoms: Vec<&'a Atom>,
    ass: &Assignment<'_>,
) -> HashMap<&'a Tag, HashSet<Fact>> {
    atoms.iter().fold(
        HashMap::<&Tag, HashSet<Fact>>::new(),
        |mut ret_val, atom| {
            let fact: Fact = assign(atom, ass);
            let pred: &'a Tag = atom.predicate_ref();
            ret_val
                .entry(pred)
                .and_modify(|facts| {
                    facts.insert(fact.clone());
                })
                .or_insert(HashSet::from([fact]));
            ret_val
        },
    )
}

fn body_for_assignment<'a>(rule: &'a Rule, ass: &Assignment) -> HashMap<&'a Tag, HashSet<Fact>> {
    let body: Vec<&Atom> = rule.body_positive_refs();
    facts_for_assignment(body, ass)
}

fn head_for_assignment<'a>(rule: &'a Rule, ass: &Assignment) -> HashMap<&'a Tag, HashSet<Fact>> {
    let head: Vec<&Atom> = rule.head_refs();
    facts_for_assignment(head, ass)
}

fn assignment_for_fact<'a>(
    fact: &Fact,
    atom_idx: usize,
    var_per_atom_idx_pos_idx: &HashMap<(usize, usize, Option<usize>), &'a Variable>,
    cur_ass: Assignment<'a>,
) -> Option<Assignment<'a>> {
    fact.terms()
        .enumerate()
        .try_fold(cur_ass, |mut ret_val, (pos_idx, term)| {
            if let Some(var) = var_per_atom_idx_pos_idx.get(&(atom_idx, pos_idx, None)) {
                if ret_val.contains_key(var) && ret_val.get(var).unwrap() != term {
                    return None;
                }
                ret_val.entry(var).or_insert(term.clone());
            }
            Some(ret_val)
        })
}

fn assignments_for_facts<'a>(
    body_offset: usize,
    preds: Vec<&Tag>,
    new_facts_by_pred: &HashMap<&Tag, HashSet<Fact>>,
    facts_by_pred: &HashMap<&Tag, HashSet<Fact>>,
    var_per_atom_idx_pos_idx_of_rule: &VarPerAtomIdxPosIdx<'a>,
) -> Vec<Assignment<'a>> {
    let mut ret_val: Vec<Assignment> = Vec::<Assignment>::new();

    for (atom_idx, start_pred) in preds
        .iter()
        .enumerate()
        .map(|(atom_idx, pred)| (atom_idx + body_offset, pred))
        .filter(|(_, pred)| {
            new_facts_by_pred.contains_key(*pred)
                && !new_facts_by_pred.get(*pred).unwrap().is_empty()
        })
    {
        let facts_for_start_pred: &HashSet<Fact> = new_facts_by_pred.get(start_pred).unwrap();
        if facts_for_start_pred.is_empty() {
            continue;
        }
        let unfiltered_assignments_for_start_predicate: Vec<Assignment> = facts_for_start_pred
            .iter()
            .filter_map(|fact| {
                assignment_for_fact(
                    fact,
                    atom_idx,
                    var_per_atom_idx_pos_idx_of_rule,
                    Assignment::new(),
                )
            })
            .collect();
        let other_preds: Vec<(usize, &Tag)> = preds
            .iter()
            .enumerate()
            .map(|(i, pred)| (i + body_offset, pred))
            .filter(|(i, _)| *i != atom_idx)
            .map(|(i, pred)| (i, *pred))
            .collect();
        let filtered_assignments_for_start_predicate: Vec<Assignment> = other_preds.iter().fold(
            unfiltered_assignments_for_start_predicate,
            |assigns, (i, pred)| {
                let facts_of_pred: &HashSet<Fact> = facts_by_pred.get(pred).unwrap();
                assigns
                    .into_iter()
                    .fold(Vec::<Assignment>::new(), |mut new_assigns, ass| {
                        facts_of_pred.iter().for_each(|fact| {
                            let new_ass_op: Option<Assignment> = assignment_for_fact(
                                fact,
                                *i,
                                var_per_atom_idx_pos_idx_of_rule,
                                ass.clone(),
                            );
                            if let Some(new_ass) = new_ass_op {
                                new_assigns.push(new_ass);
                            }
                        });
                        new_assigns
                    })
            },
        );

        for ass in filtered_assignments_for_start_predicate.into_iter() {
            if !ret_val.contains(&ass) {
                ret_val.push(ass);
            }
        }
    }

    ret_val
}

struct DatalogReasoner<'a> {
    core: CoreReasoner<'a>,
}

impl<'a> DatalogReasoner<'a> {
    fn new(
        predicates: &HashSet<&'a Tag>,
        rules: &'a Vec<&'a Rule>,
        var_per_atom_idx_pos_idx_per_rule: &'a VarPerAtomIdxPosIdxPerRule<'a>,
    ) -> Self {
        Self {
            core: CoreReasoner::new(predicates, rules, var_per_atom_idx_pos_idx_per_rule),
        }
    }

    fn run_saturating(
        &mut self,
        new_facts_by_pred: HashMap<&'a Tag, HashSet<Fact>>,
    ) -> HashMap<&'a Tag, HashSet<Fact>> {
        self.core.run_saturating(new_facts_by_pred, &|_, _| false)
    }
}

struct ExistentialReasoner<'a, 'b> {
    core: CoreReasoner<'a>,
    variant: &'b ChaseVariant<'a, 'b>,
}

impl<'a, 'b> ExistentialReasoner<'a, 'b> {
    fn new(
        predicates: &HashSet<&'a Tag>,
        rules: &'a Vec<&'a Rule>,
        var_per_atom_idx_pos_idx_per_rule: &'a VarPerAtomIdxPosIdxPerRule<'a>,
        variant: &'b ChaseVariant<'a, 'b>,
    ) -> Self {
        Self {
            core: CoreReasoner::new(predicates, rules, var_per_atom_idx_pos_idx_per_rule),
            variant,
        }
    }

    fn run_every_rule_once(
        &mut self,
        new_facts_by_pred: &HashMap<&'a Tag, HashSet<Fact>>,
    ) -> HashMap<&'a Tag, HashSet<Fact>> {
        self.core
            .run_every_rule_once(new_facts_by_pred, &|rule, ass| {
                assign_is_blocked(
                    rule,
                    self.core.rules,
                    ass,
                    self.core.var_per_atom_idx_pos_idx_per_rule,
                    self.variant,
                )
            })
    }
}

fn build_var_index_for_rule<'a>(rule: &'a Rule) -> VarPerAtomIdxPosIdx<'a> {
    let mut ret_val: VarPerAtomIdxPosIdx<'a> = VarPerAtomIdxPosIdx::new();
    rule.body_positive_refs()
        .iter()
        .copied()
        .chain(rule.head().iter())
        .enumerate()
        .for_each(|(i, atom)| {
            atom.terms()
                .enumerate()
                .filter_map(|(j, term)| match term {
                    Term::Primitive(Primitive::Variable(var)) => Some(vec![(j, None, var)]),
                    Term::FunctionTerm(f_term) => Some(f_term.terms().enumerate().fold(
                        Vec::<(usize, Option<usize>, &Variable)>::new(),
                        |mut poses, (l, inner_term)| {
                            let var = match inner_term {
                                Term::Primitive(Primitive::Variable(var)) => var,
                                _ => panic!("not possible"),
                            };
                            poses.push((j, Some(l), var));
                            poses
                        },
                    )),
                    _ => None,
                })
                .flatten()
                .for_each(|(j, op_l, var)| {
                    ret_val.insert((i, j, op_l), var);
                })
        });
    ret_val
}

fn build_var_index_for_rules<'a>(rules: &[&'a Rule]) -> VarPerAtomIdxPosIdxPerRule<'a> {
    rules.iter().fold(
        VarPerAtomIdxPosIdxPerRule::new(),
        |mut var_atom_pos_rule, rule| {
            var_atom_pos_rule.insert(rule, build_var_index_for_rule(rule));
            var_atom_pos_rule
        },
    )
}

struct CoreReasoner<'a> {
    facts_by_pred: HashMap<&'a Tag, HashSet<Fact>>,
    rules: &'a Vec<&'a Rule>,
    var_per_atom_idx_pos_idx_per_rule: &'a VarPerAtomIdxPosIdxPerRule<'a>,
}

impl<'a> CoreReasoner<'a> {
    fn new(
        predicates: &HashSet<&'a Tag>,
        rules: &'a Vec<&'a Rule>,
        var_per_atom_idx_pos_idx_per_rule: &'a VarPerAtomIdxPosIdxPerRule<'a>,
    ) -> Self {
        let facts_by_pred: HashMap<&Tag, HashSet<Fact>> = predicates
            .iter()
            .map(|pred| (*pred, HashSet::default()))
            .collect();
        Self {
            facts_by_pred,
            rules,
            var_per_atom_idx_pos_idx_per_rule,
        }
    }

    fn run_saturating(
        &mut self,
        mut new_facts_by_pred: HashMap<&'a Tag, HashSet<Fact>>,
        is_blocked: &impl Fn(&Rule, &Assignment) -> bool,
    ) -> HashMap<&'a Tag, HashSet<Fact>> {
        let mut ret_val = HashMap::<&Tag, HashSet<Fact>>::default();

        loop {
            let con_facts_by_pred: HashMap<&Tag, HashSet<Fact>> =
                self.run_every_rule_once(&new_facts_by_pred, is_blocked);
            if con_facts_by_pred.is_empty() {
                break;
            }
            con_facts_by_pred.iter().for_each(|(pred, con_facts)| {
                ret_val
                    .entry(pred)
                    .and_modify(|facts| facts.insert_all_take(con_facts.clone()))
                    .or_insert(con_facts.clone());
            });
            new_facts_by_pred = con_facts_by_pred;
        }

        ret_val
    }

    fn run_every_rule_once(
        &mut self,
        new_facts_by_pred: &HashMap<&'a Tag, HashSet<Fact>>,
        is_blocked: &impl Fn(&Rule, &Assignment) -> bool,
    ) -> HashMap<&'a Tag, HashSet<Fact>> {
        self.update_facts(new_facts_by_pred);

        self.rules.iter().fold(
            HashMap::<&Tag, HashSet<Fact>>::new(),
            |mut ret_val, rule| {
                let con_facts_by_pred: HashMap<&Tag, HashSet<Fact>> =
                    self.run_rule(rule, new_facts_by_pred, is_blocked);
                con_facts_by_pred.into_iter().for_each(|(pred, con_facts)| {
                    ret_val
                        .entry(pred)
                        .and_modify(|facts| facts.insert_all_take(con_facts.clone()))
                        .or_insert(con_facts);
                });
                ret_val
            },
        )
    }

    fn run_rule(
        &self,
        rule: &'a Rule,
        new_facts_by_pred: &HashMap<&Tag, HashSet<Fact>>,
        is_blocked: &impl Fn(&Rule, &Assignment) -> bool,
    ) -> HashMap<&'a Tag, HashSet<Fact>> {
        let preds_of_body: Vec<&Tag> = rule
            .body_positive_refs()
            .iter()
            .map(|atom| atom.predicate_ref())
            .collect();
        let mut assignments: Vec<Assignment> = assignments_for_facts(
            0,
            preds_of_body,
            new_facts_by_pred,
            &self.facts_by_pred,
            self.var_per_atom_idx_pos_idx_per_rule.get(rule).unwrap(),
        );

        assignments.retain(|ass| !is_blocked(rule, ass));

        assignments
            .iter_mut()
            .flat_map(|ass| head_for_assignment(rule, ass))
            .filter_map(|(pred, mut facts)| {
                facts.retain(|fact| !self.facts_by_pred.get(pred).unwrap().contains(fact));
                match !facts.is_empty() {
                    true => Some((pred, facts)),
                    false => None,
                }
            })
            .collect()
    }

    fn update_facts(&mut self, new_facts_by_pred: &HashMap<&'a Tag, HashSet<Fact>>) {
        new_facts_by_pred.iter().for_each(|(pred, new_facts)| {
            self.facts_by_pred
                .get_mut(pred)
                .unwrap()
                .insert_all(new_facts)
        });
    }
}

type Assignment<'a> = HashMap<&'a Variable, Term>;
type VarPerAtomIdxPosIdx<'a> = HashMap<(usize, usize, Option<usize>), &'a Variable>;
type VarPerAtomIdxPosIdxPerRule<'a> = HashMap<&'a Rule, VarPerAtomIdxPosIdx<'a>>;
