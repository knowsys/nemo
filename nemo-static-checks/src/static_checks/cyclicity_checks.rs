//! Functionality
use crate::static_checks::collection_traits::InsertAll;
use crate::static_checks::rule_set::{RuleRefs, RuleSet};
use nemo::rule_model::components::{
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
};

use std::collections::{HashMap, HashSet};

pub mod acyclicity;
pub mod cyclicity;

fn backtrack_sk_term<'a>(
    in_term: &Term,
    ex_rules: &Vec<&'a Rule>,
    with_body: bool,
) -> HashMap<&'a Tag, HashSet<Fact>> {
    let sk_term;
    if let Term::FunctionTerm(f_term) = in_term {
        sk_term = f_term;
    } else {
        return HashMap::default();
    }
    let (rule, unassigned_sk_term): (&Rule, &FunctionTerm) = ex_rules
        .iter()
        .find_map(|rule| {
            rule.head_terms().find_map(|term| {
                if let Term::FunctionTerm(f_term) = term
                    && f_term.tag() == sk_term.tag()
                {
                    Some((*rule, f_term))
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
    let ret_val = if with_body {
        union(
            head_for_assignment(rule, &ass),
            body_for_assignment(rule, &ass),
        )
    } else {
        head_for_assignment(rule, &ass)
    };
    sk_term
        .terms()
        // .filter_map(|term| {
        //     if let Term::FunctionTerm(inner_sk_term) = term {
        //         Some(inner_sk_term)
        //     } else {
        //         None
        //     }
        // })
        .fold(ret_val, |ret_val, inner_sk_term| {
            union(
                ret_val,
                backtrack_sk_term(inner_sk_term, ex_rules, with_body),
            )
        })
}

pub fn union<'a>(
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

pub fn body_for_assignment<'a>(
    rule: &'a Rule,
    ass: &Assignment,
) -> HashMap<&'a Tag, HashSet<Fact>> {
    let body: Vec<&Atom> = rule.body_positive_refs();
    facts_for_assignment(body, ass)
}

pub fn head_for_assignment<'a>(
    rule: &'a Rule,
    ass: &Assignment,
) -> HashMap<&'a Tag, HashSet<Fact>> {
    let head: Vec<&Atom> = rule.head_refs();
    facts_for_assignment(head, ass)
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

#[derive(Clone, Copy)]
pub enum StrategySelector {
    MFA,
    RMFA,
    MFC,
    DRPC,
}

pub trait CyclicityStrategy {
    fn is_blocked(&self, rule: &Rule, ass: &Assignment) -> bool;

    #[inline]
    fn h<'a>(
        &self,
        facts_by_pred: HashMap<&'a Tag, HashSet<Fact>>,
    ) -> HashMap<&'a Tag, HashSet<Fact>> {
        facts_by_pred
    }
}

pub struct NoBlockStrategy;

impl CyclicityStrategy for NoBlockStrategy {
    fn is_blocked(&self, _rule: &Rule, _ass: &Assignment) -> bool {
        false
    }
}

fn predicates_ref<'a>(rule_set: &[&'a Rule]) -> HashSet<&'a Tag> {
    rule_set
        .iter()
        .fold(HashSet::<&Tag>::new(), |ret_val, rule| {
            ret_val.insert_all_take_ret(rule.predicates_ref())
        })
}

fn predicates_ref_and_lens<'a>(rule_set: &[&'a Rule]) -> HashSet<(&'a Tag, usize)> {
    rule_set
        .iter()
        .fold(HashSet::<(&Tag, usize)>::new(), |ret_val, rule| {
            ret_val.insert_all_take_ret(rule.predicates_ref_and_lens())
        })
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

pub struct CoreReasoner<'a, 's> {
    facts_by_pred: HashMap<&'a Tag, HashSet<Fact>>,
    rules: &'a Vec<&'a Rule>,
    var_per_atom_idx_pos_idx_per_rule: &'a VarPerAtomIdxPosIdxPerRule<'a>,
    strat: &'s dyn CyclicityStrategy,
}

impl<'a, 's> CoreReasoner<'a, 's> {
    pub fn facts(&self) -> &HashMap<&Tag, HashSet<Fact>> {
        &self.facts_by_pred
    }

    pub fn into_facts(self) -> HashMap<&'a Tag, HashSet<Fact>> {
        self.facts_by_pred
    }

    fn new(
        predicates: &HashSet<&'a Tag>,
        rules: &'a Vec<&'a Rule>,
        var_per_atom_idx_pos_idx_per_rule: &'a VarPerAtomIdxPosIdxPerRule<'a>,
        strat: &'s dyn CyclicityStrategy,
    ) -> Self {
        let facts_by_pred: HashMap<&Tag, HashSet<Fact>> = predicates
            .iter()
            .map(|pred| (*pred, HashSet::default()))
            .collect();
        Self {
            facts_by_pred,
            rules,
            var_per_atom_idx_pos_idx_per_rule,
            strat,
        }
    }

    fn run_saturating(
        &mut self,
        mut new_facts_by_pred: HashMap<&'a Tag, HashSet<Fact>>,
    ) -> HashMap<&'a Tag, HashSet<Fact>> {
        let mut ret_val = HashMap::<&Tag, HashSet<Fact>>::default();

        loop {
            let con_facts_by_pred: HashMap<&Tag, HashSet<Fact>> =
                self.run_every_rule_once(&new_facts_by_pred);
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
    ) -> HashMap<&'a Tag, HashSet<Fact>> {
        self.update_facts(new_facts_by_pred);

        self.rules.iter().fold(
            HashMap::<&Tag, HashSet<Fact>>::new(),
            |mut ret_val, rule| {
                let con_facts_by_pred: HashMap<&Tag, HashSet<Fact>> =
                    self.run_rule(rule, new_facts_by_pred);
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

        assignments.retain(|ass| !self.strat.is_blocked(rule, ass));

        assignments
            .iter_mut()
            .flat_map(|ass| self.strat.h(head_for_assignment(rule, ass)))
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

impl RuleSet {
    fn datalog_rules(&self) -> Vec<&Rule> {
        self.0.iter().filter(|rule| !rule.contains_func()).collect()
    }

    fn existential_rules(&self) -> Vec<&Rule> {
        self.0.iter().filter(|rule| rule.contains_func()).collect()
    }
}

pub type Assignment<'a> = HashMap<&'a Variable, Term>;
pub type VarPerAtomIdxPosIdx<'a> = HashMap<(usize, usize, Option<usize>), &'a Variable>;
pub type VarPerAtomIdxPosIdxPerRule<'a> = HashMap<&'a Rule, VarPerAtomIdxPosIdx<'a>>;
