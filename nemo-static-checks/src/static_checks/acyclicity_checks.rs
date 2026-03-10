use nemo::{
    execution::{DefaultExecutionEngine, ExecutionEngine},
    io::{ImportManager, resource_providers::ResourceProviders},
    rule_model::{
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
        programs::{ProgramRead, handle::ProgramHandle, program::Program},
    },
};

use crate::static_checks::collection_traits::InsertAll;
use crate::static_checks::rule_properties::RuleProperties;
use crate::static_checks::rule_set::{RuleRefs, RuleSet};

use std::collections::{HashMap, HashSet};

use super::rule_set::{ExistentialVariables, SpecialVariables};

pub fn mfa_handle(handle: ProgramHandle) -> ProgramHandle {
    handle
        .transform(TransformationCriticalInstance::default())
        .expect("TransformationCriticalInstance Error")
        .transform(TransformationSkolemize::default())
        .expect("TransformationSkolemize Error")
}

#[derive(Clone)]
pub enum ChaseVariant {
    SkolemMFA,
    SkolemDMFA,
    SkolemRestricted,
}

enum EngineType {
    Datalog,
    Existential,
}

impl RuleSet {
    fn critical_instance(&self) -> HashSet<Fact> {
        let star_constant: Term = Term::from("*");
        let predicates_and_lens: HashSet<(&Tag, usize)> = self
            .0
            .iter()
            .flat_map(|rule| rule.predicates_ref_and_lens())
            .collect();
        predicates_and_lens
            .into_iter()
            .map(|(pred, len)| {
                let terms: Vec<Term> = vec![star_constant.clone(); len];
                Fact::from((pred, terms))
            })
            .collect()
    }

    fn initialize_dummy_import_manager(&self) -> ImportManager {
        ImportManager::new(ResourceProviders::empty())
    }

    // fn initialize_program(&self, rules: &Vec<&Rule>, facts: &HashSet<Fact>) -> Program {
    //     let mut program_builder: ProgramBuilder = ProgramBuilder::default();
    //     rules.iter().for_each(|rule| {
    //         program_builder.add_rule((*rule).clone());
    //     });
    //     facts.iter().for_each(|fact| {
    //         program_builder.add_fact(fact.clone());
    //     });
    //     program_builder.finalize()
    // }

    // async fn initialize_execution_engine(
    //     &self,
    //     rules: &Vec<&Rule>,
    //     facts: &HashSet<Fact>,
    // ) -> DefaultExecutionEngine {
    //     let dummy_import_manager: ImportManager = self.initialize_dummy_import_manager();
    //     let program: Program = self.initialize_program(rules, facts);
    //     ExecutionEngine::initialize(program, dummy_import_manager)
    //         .await
    //         .expect("Error while creating datalog program")
    // }

    fn datalog_rules(&self) -> Vec<&Rule> {
        self.0.iter().filter(|rule| !rule.contains_func()).collect()
        // self.0.iter().filter(|rule| rule.is_datalog()).collect()
    }

    fn existential_rules(&self) -> Vec<&Rule> {
        self.0.iter().filter(|rule| rule.contains_func()).collect()
    }

    fn predicates_of_datalog_rules<'a>(&self, datalog_rules: &Vec<&'a Rule>) -> HashSet<&'a Tag> {
        self.predicates_of_rules(datalog_rules)
    }

    fn predicates_of_existential_rules<'a>(
        &self,
        existential_rules: &Vec<&'a Rule>,
    ) -> HashSet<&'a Tag> {
        self.predicates_of_rules(existential_rules)
    }

    fn predicates_of_rules<'a>(&self, rules: &Vec<&'a Rule>) -> HashSet<&'a Tag> {
        rules.iter().fold(HashSet::<&Tag>::new(), |preds, rule| {
            let preds_of_rule: Vec<&Tag> = rule.predicates_ref();
            preds.insert_all_take_ret(preds_of_rule)
        })
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

pub async fn check_acyclicity(handle: ProgramHandle, variant: ChaseVariant) -> bool {
    let rules: RuleSet = RuleSet(handle.rules().cloned().collect());

    let mut new_facts_by_pred: HashMap<&Tag, HashSet<Fact>> =
        convert_set_to_map(handle.facts().collect());

    let datalog_rules: Vec<&Rule> = rules.datalog_rules();
    // println!("{}", datalog_rules.is_empty());
    let existential_rules: Vec<&Rule> = rules.existential_rules();

    let datalog_pr: HashSet<&Tag> = predicates_ref(&datalog_rules);
    // println!("{}", datalog_pr.is_empty());
    let existential_pr: HashSet<&Tag> = predicates_ref(&existential_rules);
    // let intersection_pr: HashSet<&Tag> =
    //     datalog_pr.intersection(&existential_pr).copied().collect();

    let mut datalog_reasoner: Reasoner = Reasoner::new(&datalog_pr, datalog_rules, &variant);
    let mut existential_reasoner: Reasoner =
        Reasoner::new(&existential_pr, existential_rules, &variant);

    let mut new_datalog_facts_by_pred: HashMap<&Tag, HashSet<Fact>> = new_facts_by_pred
        .iter()
        .filter(|(pred, _)| datalog_pr.contains(*pred))
        .map(|(pred, facts)| (*pred, facts.clone()))
        .collect();
    // println!("{}", new_datalog_facts_by_pred.is_empty());
    let mut new_existential_facts_by_pred: HashMap<&Tag, HashSet<Fact>> = new_facts_by_pred
        .iter()
        .filter(|(pred, _)| existential_pr.contains(*pred))
        .map(|(pred, facts)| (*pred, facts.clone()))
        .collect();

    let mut count = 1;

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
            .any(|fact| {
                // new_existential_facts_by_pred.iter().for_each(|(_, facts)| {
                //     facts.iter().for_each(|fact| {
                //         print!("{}: {}", count, fact);
                //     })
                // });
                // println!("count");
                fact.is_cyclic(&mut Vec::default())
            })
        {
            return false;
        }
        // if count == 2 {
        // new_existential_facts_by_pred.iter().for_each(|(_, facts)| {
        //     facts.iter().for_each(|fact| {
        //         print!("{}: {}", count, fact);
        //     })
        // });
        //     return true;
        // }
        // if count == 2 {
        //     return true;
        // }
        count += 1;
        // cur_facts.insert_all(&new_facts);
    }
    // println!("{}", count);
    true
}

fn filter_new_facts(facts_by_pred: &mut HashMap<&Tag, HashSet<Fact>>, preds: &HashSet<&Tag>) {
    facts_by_pred.retain(|pred, _| preds.contains(pred))
}

struct Reasoner<'a, 'b> {
    facts_by_pred: HashMap<&'a Tag, HashSet<Fact>>,
    rules: Vec<&'a Rule>,
    var_per_atom_idx_pos_idx_per_rule: VarPerAtomIdxPosIdxPerRule<'a>,
    // body_pos_per_var_per_rule: BodyPosPerVarPerRule<'a>,
    variant: &'b ChaseVariant,
}

// struct DatalogReasoner<'a> {
//     facts_by_pred: HashMap<&'a Tag, HashSet<Fact>>,
//     rules: Vec<&'a Rule>,
//     var_per_atom_idx_pos_idx_per_rule: Var
// }

// trait Reasoner {
//     fn run_every_rule_once(&mut self);
//     fn run_every_rule(&mut self);
// }

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

// fn rename_consts<'a>(ass: &Assignment<'a>) -> Assignment<'a> {
//     todo!();
//     // TODO: IMPLEMENT
// }

fn assign_is_blocked_rmfa(rule: &Rule, ass: &Assignment) -> bool {
    return false;
    // let renam= rename_consts(ass);
    // let body_for_renamed_consts = body_for_assignment(renamed_ass);
    // let skolem_terms_in_body = get_skolem_terms(&body_for_renamed_consts);
    // let special_reasoning_set =
    //     special_reasoning_set(body_for_renamed_consts, skolem_terms_in_body);
    // let datalog_reasoner = Reasoner::new(special_reasoning_set, )
    // TODO: IMPLEMENT
    todo!();
}

fn assign_is_blocked(rule: &Rule, ass: &Assignment, variant: &ChaseVariant) -> bool {
    match variant {
        ChaseVariant::SkolemMFA => false,
        ChaseVariant::SkolemRestricted => assign_is_blocked_rmfa(rule, ass),
        ChaseVariant::SkolemDMFA => panic!("not implemented yet"),
    }
}

impl<'a, 'b> Reasoner<'a, 'b> {
    fn assignment_for_fact(
        &self,
        fact: &Fact,
        atom_idx: usize,
        var_per_atom_idx_pos_idx: &HashMap<(usize, usize), &'a Variable>,
        cur_ass: Assignment<'a>,
    ) -> Option<Assignment<'a>> {
        fact.terms()
            .enumerate()
            .try_fold(cur_ass, |mut ret_val, (pos_idx, term)| {
                let var: &Variable = var_per_atom_idx_pos_idx.get(&(atom_idx, pos_idx)).unwrap();
                if ret_val.contains_key(var) && ret_val.get(var).unwrap() != term {
                    return None;
                }
                ret_val.entry(var).or_insert(term.clone());
                Some(ret_val)
            })
    }

    fn assignments_for_rule(
        &self,
        rule: &'a Rule,
        new_facts_by_pred: &HashMap<&Tag, HashSet<Fact>>,
    ) -> Vec<Assignment<'a>> {
        // print!("{}", rule);
        let mut ret_val: Vec<Assignment> = Vec::<Assignment>::new();
        let preds_of_body: Vec<&Tag> = rule
            .body_positive_refs()
            .iter()
            .map(|atom| atom.predicate_ref())
            .collect();
        // print!("vec: ");
        // preds_of_body.iter().for_each(|pred| {
        //     print!("{}", pred);
        // });
        let var_per_atom_idx_pos_idx_of_rule: &HashMap<(usize, usize), &Variable> =
            self.var_per_atom_idx_pos_idx_per_rule.get(rule).unwrap();
        // var_per_atom_idx_pos_idx_of_rule
        //     .iter()
        //     .for_each(|((atom_idx, pos_idx), var)| {
        //         print!("({}, {}): {}", atom_idx, pos_idx, var);
        //     });
        for (atom_idx, start_pred) in preds_of_body.iter().enumerate().filter(|(_, pred)| {
            new_facts_by_pred.contains_key(*pred)
                && !new_facts_by_pred.get(*pred).unwrap().is_empty()
        }) {
            let facts_for_start_pred: &HashSet<Fact> = new_facts_by_pred.get(start_pred).unwrap();
            if facts_for_start_pred.is_empty() {
                continue;
            }
            let unfiltered_assignments_for_start_predicate: Vec<Assignment> = facts_for_start_pred
                .iter()
                .filter_map(|fact| {
                    self.assignment_for_fact(
                        fact,
                        atom_idx,
                        var_per_atom_idx_pos_idx_of_rule,
                        Assignment::new(),
                    )
                })
                .collect();
            let other_preds: Vec<(usize, &Tag)> = preds_of_body
                .iter()
                .enumerate()
                .filter(|(i, _)| *i != atom_idx)
                .map(|(i, pred)| (i, *pred))
                .collect();
            // println!("other_preds: {:#?}", other_preds);
            // println!(
            //     "unfiltered_ass: \n{:#?}",
            //     unfiltered_assignments_for_start_predicate
            // );
            let filtered_assignments_for_start_predicate: Vec<Assignment> =
                other_preds.iter().fold(
                    unfiltered_assignments_for_start_predicate,
                    |assigns, (i, pred)| {
                        let facts_of_pred: &HashSet<Fact> = self.facts_by_pred.get(pred).unwrap();
                        // println!("{:#?}", facts_of_pred);
                        assigns.into_iter().fold(
                            Vec::<Assignment>::new(),
                            |mut new_assigns, ass| {
                                facts_of_pred.iter().for_each(|fact| {
                                    // println!("cur_ass: {:#?}", ass);
                                    let new_ass_op: Option<Assignment> = self.assignment_for_fact(
                                        fact,
                                        *i,
                                        var_per_atom_idx_pos_idx_of_rule,
                                        ass.clone(),
                                    );
                                    // println!("{:#?}", new_ass_op);
                                    if let Some(new_ass) = new_ass_op {
                                        // println!("new_ass: {:#?}", new_ass);
                                        new_assigns.push(new_ass);
                                    }
                                });
                                new_assigns
                            },
                        )
                    },
                );
            // println!(
            //     "filtered_asss: {:#?}",
            //     filtered_assignments_for_start_predicate
            // );
            filtered_assignments_for_start_predicate
                .into_iter()
                .for_each(|ass| {
                    ret_val.push(ass);
                });
            // ret_val.insert_all_take(filtered_assignments_for_start_predicate);
        }
        // let frontier_vars: HashSet<&Variable> = rule.frontier_variables();
        // let ex_vars: HashSet<&Variable> = rule.existential_variables();
        // ret_val.iter_mut().for_each(|ass| {
        //     self.extend_assign_for_ex_vars(rule_idx, ass, &frontier_vars, &ex_vars)
        // });
        ret_val
    }

    fn head_for_assignment(
        &self,
        rule: &'a Rule,
        ass: &Assignment,
    ) -> HashMap<&'a Tag, HashSet<Fact>> {
        let head: Vec<&Atom> = rule.head_refs();
        head.iter().fold(
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

    fn new(predicates: &HashSet<&'a Tag>, rules: Vec<&'a Rule>, variant: &'b ChaseVariant) -> Self {
        let facts_by_pred: HashMap<&Tag, HashSet<Fact>> = predicates
            .iter()
            .map(|pred| (*pred, HashSet::default()))
            .collect();
        let var_per_atom_idx_pos_idx_per_rule: VarPerAtomIdxPosIdxPerRule = rules.iter().fold(
            VarPerAtomIdxPosIdxPerRule::new(),
            |mut var_atom_pos_rule, rule| {
                rule.body_positive_refs()
                    .iter()
                    .enumerate()
                    .for_each(|(i, atom)| {
                        atom.terms()
                            .enumerate()
                            .filter_map(|(j, term)| match term {
                                Term::Primitive(Primitive::Variable(var)) => Some((j, var)),
                                _ => None,
                            })
                            .for_each(|(j, var)| {
                                var_atom_pos_rule
                                    .entry(rule)
                                    .and_modify(|map| {
                                        map.insert((i, j), var);
                                    })
                                    .or_insert(HashMap::from([((i, j), var)]));
                            })
                    });
                var_atom_pos_rule
            },
        );
        Self {
            facts_by_pred,
            rules,
            var_per_atom_idx_pos_idx_per_rule,
            variant,
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

        let ret_val = self.rules.iter().fold(
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
        );
        self.update_facts(&ret_val);
        ret_val
    }

    fn run_rule(
        &self,
        rule: &'a Rule,
        new_facts_by_pred: &HashMap<&Tag, HashSet<Fact>>,
    ) -> HashMap<&'a Tag, HashSet<Fact>> {
        let mut assignments: Vec<Assignment> = self.assignments_for_rule(rule, new_facts_by_pred);

        // TODO: FILTER ASSIGNMENTS FOR RMFA / DMFA CHECKS
        assignments.retain(|ass| !assign_is_blocked(rule, ass, self.variant));

        assignments
            .iter_mut()
            .flat_map(|ass| self.head_for_assignment(rule, ass))
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
type VarPerAtomIdxPosIdxPerRule<'a> = HashMap<&'a Rule, HashMap<(usize, usize), &'a Variable>>;
