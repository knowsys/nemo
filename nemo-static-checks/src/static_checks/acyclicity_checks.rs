use nemo::{
    execution::{DefaultExecutionEngine, ExecutionEngine},
    io::{resource_providers::ResourceProviders, ImportManager},
    rule_model::{
        components::{
            atom::Atom,
            fact::Fact,
            rule::Rule,
            tag::Tag,
            term::{
                primitive::{variable::Variable, Primitive},
                Term,
            },
        },
        program::{Program, ProgramBuilder},
    },
};

use crate::static_checks::collection_traits::InsertAll;
use crate::static_checks::rule_properties::RuleProperties;
use crate::static_checks::rule_set::{RuleRefs, RuleSet};

use std::collections::{HashMap, HashSet};

use super::rule_set::{ExistentialVariables, SpecialVariables};

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

    fn initialize_program(&self, rules: &Vec<&Rule>, facts: &HashSet<Fact>) -> Program {
        let mut program_builder: ProgramBuilder = ProgramBuilder::default();
        rules.iter().for_each(|rule| {
            program_builder.add_rule((*rule).clone());
        });
        facts.iter().for_each(|fact| {
            program_builder.add_fact(fact.clone());
        });
        program_builder.finalize()
    }

    fn initialize_execution_engine(
        &self,
        rules: &Vec<&Rule>,
        facts: &HashSet<Fact>,
    ) -> DefaultExecutionEngine {
        let dummy_import_manager: ImportManager = self.initialize_dummy_import_manager();
        let program: Program = self.initialize_program(rules, facts);
        ExecutionEngine::initialize(program, dummy_import_manager)
            .expect("Error while creating datalog program")
    }

    fn datalog_rules(&self) -> Vec<&Rule> {
        self.0.iter().filter(|rule| rule.is_datalog()).collect()
    }

    fn existential_rules(&self) -> Vec<&Rule> {
        self.0.iter().filter(|rule| !rule.is_datalog()).collect()
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

    pub fn check_acyclicity(&self, variant: ChaseVariant) -> bool {
        let mut cur_facts: HashSet<Fact> = self.critical_instance();
        // println!("{:#?}", new_facts);
        let datalog_rules: Vec<&Rule> = self.datalog_rules();
        // println!("{:?}", datalog_rules);
        let existential_rules: Vec<&Rule> = self.existential_rules();
        // println!("{:?}", existential_rules);
        let datalog_predicates: HashSet<&Tag> = self.predicates_of_datalog_rules(&datalog_rules);
        let existential_predicates: HashSet<&Tag> =
            self.predicates_of_existential_rules(&existential_rules);
        let mut datalog_execution_engine: DefaultExecutionEngine;
        let mut existential_reasoner: ExistentialReasoner = ExistentialReasoner::new(
            &existential_predicates,
            &HashSet::default(),
            existential_rules,
            variant,
        );
        let mut new_facts: HashSet<Fact> = cur_facts
            .iter()
            .filter(|fact| existential_predicates.contains(fact.predicate()))
            .cloned()
            .collect();
        while !new_facts.is_empty() {
            println!("here");
            datalog_execution_engine = self.initialize_execution_engine(&datalog_rules, &cur_facts);
            datalog_execution_engine
                .execute()
                .expect("Error while executing DatalogEngine");
            datalog_predicates.iter().for_each(|pred| {
                let facts_of_pred: Vec<Fact> = datalog_execution_engine
                    .predicate_rows(pred)
                    .expect("some error with preds")
                    .unwrap()
                    .map(|values| Fact::from((*pred, values.into_iter().map(Term::from).collect())))
                    .filter(|fact| !cur_facts.contains(fact))
                    .collect();
                facts_of_pred.into_iter().for_each(|fact| {
                    if !new_facts.contains(&fact)
                        && existential_predicates.contains(fact.predicate())
                    {
                        new_facts.insert(fact.clone());
                    }
                    cur_facts.insert(fact);
                })
            });
            new_facts = existential_reasoner.run_every_rule_once(new_facts);
            if new_facts.iter().any(|fact| fact.is_cyclic()) {
                return false;
            }
            cur_facts.insert_all(&new_facts);
        }
        true
    }
}

struct ExistentialReasoner<'a> {
    facts_by_pred: HashMap<&'a Tag, HashSet<Fact>>,
    rules: Vec<&'a Rule>,
    var_per_atom_idx_pos_idx_per_rule: VarPerAtomIdxPosIdxPerRule<'a>,
    // body_pos_per_var_per_rule: BodyPosPerVarPerRule<'a>,
    variant: ChaseVariant,
}

impl<'a> ExistentialReasoner<'a> {
    // TODO: MAYBE REWRITE
    fn assignment_for_fact(
        &self,
        fact: &Fact,
        atom_idx: usize,
        var_per_atom_idx_pos_idx: &HashMap<(usize, usize), &'a Variable>,
        cur_ass: Assignment<'a>,
    ) -> Option<Assignment> {
        fact.subterms()
            .enumerate()
            .try_fold(cur_ass, |mut ret_val, (pos_idx, term)| {
                let var: &Variable = var_per_atom_idx_pos_idx.get(&(atom_idx, pos_idx)).unwrap();
                // println!("{}, var: {:#?}", pos_idx, var);
                if ret_val.contains_key(var) && ret_val.get(var).unwrap() != term {
                    return None;
                }
                ret_val.entry(var).or_insert(term.clone());
                Some(ret_val)
            })
    }

    fn assignments_for_rule(
        &'a self,
        rule_idx: usize,
        rule: &'a Rule,
        new_facts_by_pred: &HashMap<&Tag, HashSet<Fact>>,
    ) -> Vec<Assignment<'a>> {
        let mut ret_val: Vec<Assignment> = Vec::<Assignment>::new();
        let preds_of_body: Vec<&Tag> = rule
            .body_positive_refs()
            .iter()
            .map(|atom| atom.predicate_ref())
            .collect();
        let var_per_atom_idx_pos_idx_of_rule: &HashMap<(usize, usize), &Variable> =
            self.var_per_atom_idx_pos_idx_per_rule.get(rule).unwrap();
        for (atom_idx, start_pred) in preds_of_body
            .iter()
            .filter(|pred| !new_facts_by_pred.get(*pred).unwrap().is_empty())
            .enumerate()
        {
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
            println!("other_preds: {:#?}", other_preds);
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
                                    println!("cur_ass: {:#?}", ass);
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
            ret_val.insert_all_take(filtered_assignments_for_start_predicate);
        }
        let frontier_vars: HashSet<&Variable> = rule.frontier_variables();
        let ex_vars: HashSet<&Variable> = rule.existential_variables();
        ret_val.iter_mut().for_each(|ass| {
            self.extend_assign_for_ex_vars(rule_idx, ass, &frontier_vars, &ex_vars)
        });
        ret_val
    }

    fn convert_set_to_map(&self, filtered_facts: HashSet<Fact>) -> HashMap<&Tag, HashSet<Fact>> {
        self.facts_by_pred.keys().fold(
            HashMap::<&Tag, HashSet<Fact>>::new(),
            |mut ret_val, pred| {
                let facts_of_pred: HashSet<Fact> = filtered_facts
                    .iter()
                    .filter(|fact| fact.predicate() == *pred)
                    .cloned()
                    .collect();
                ret_val.insert(pred, facts_of_pred);
                ret_val
            },
        )
    }

    fn extend_assign_for_ex_vars(
        &self,
        rule_idx: usize,
        ass: &mut Assignment<'a>,
        frontier_vars: &HashSet<&Variable>,
        ex_vars: &HashSet<&'a Variable>,
    ) {
        // println!("ass: \n{:#?}", ass);
        let frontier_terms: Vec<&Term> = frontier_vars
            .iter()
            .map(|var| ass.get(var).unwrap())
            .collect();
        let frontier_term_string: String = frontier_terms
            .iter()
            .map(|term| format!("{term}"))
            .collect::<Vec<String>>()
            .join(",");
        let cleaned_string: String = frontier_term_string.replace('"', "");
        ex_vars.iter().enumerate().for_each(|(var_idx, var)| {
            let new_term_name: String = format!("f_{}_{}({})", rule_idx, var_idx, cleaned_string);
            let new_term: Term = Term::from(new_term_name);
            ass.insert(var, new_term);
        })
    }

    fn filter_new_facts(&self, new_facts: HashSet<Fact>) -> HashSet<Fact> {
        new_facts
            .into_iter()
            .filter(|fact| {
                !self
                    .facts_by_pred
                    .get(fact.predicate())
                    .unwrap()
                    .contains(fact)
            })
            .collect()
    }

    // NOTE: PANIC MAYBE NOT RIGHT -> CAN HEAD CONTAIN CONSTANTS?
    fn head_for_assignment(&self, rule: &Rule, ass: &Assignment) -> HashSet<Fact> {
        let head: Vec<&Atom> = rule.head_refs();
        head.iter()
            .fold(HashSet::<Fact>::new(), |mut ret_val, atom| {
                let subterms: Vec<Term> = atom
                    .arguments()
                    .map(|term| match term {
                        Term::Primitive(Primitive::Variable(var)) => var,
                        _ => panic!("can only be a var"),
                    })
                    .map(|var| ass.get(var).unwrap().clone())
                    .collect();
                let fact: Fact = Fact::from((atom.predicate_ref(), subterms));
                ret_val.insert(fact);
                ret_val
            })
    }

    fn new(
        predicates: &HashSet<&'a Tag>,
        facts: &HashSet<Fact>,
        rules: Vec<&'a Rule>,
        variant: ChaseVariant,
    ) -> Self {
        // let facts_by_pred: HashMap<&Tag, HashSet<Fact>> = predicates.into_iter().fold(
        //     HashMap::<&Tag, HashSet<Fact>>::new(),
        //     |mut ret_val, pred| {
        //         let facts_of_pred: HashSet<Fact> = facts
        //             .iter()
        //             .filter(|fact| fact.predicate() == pred)
        //             .cloned()
        //             .collect();
        //         ret_val.insert(pred, facts_of_pred);
        //         ret_val
        //     },
        // );
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
                        atom.arguments()
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

    fn run_every_rule_once(&mut self, new_facts: HashSet<Fact>) -> HashSet<Fact> {
        // println!("{:#?}", new_facts);
        let filtered_facts: HashSet<Fact> = self.filter_new_facts(new_facts);
        self.update_facts(filtered_facts.clone());
        let filtered_facts_by_pred: HashMap<&Tag, HashSet<Fact>> =
            self.convert_set_to_map(filtered_facts);
        self.rules
            .iter()
            .enumerate()
            .fold(HashSet::<Fact>::new(), |ret_val, (rule_idx, rule)| {
                let concluded_facts: HashSet<Fact> =
                    self.run_rule(rule_idx, rule, &filtered_facts_by_pred);
                ret_val.insert_all_take_ret(concluded_facts)
            })
    }

    fn run_rule(
        &self,
        rule_idx: usize,
        rule: &Rule,
        filtered_facts_by_pred: &HashMap<&Tag, HashSet<Fact>>,
    ) -> HashSet<Fact> {
        let assignments: Vec<Assignment> =
            self.assignments_for_rule(rule_idx, rule, filtered_facts_by_pred);

        // TODO: FILTER ASSIGNMENTS FOR RMFA / DMFA CHECKS

        assignments
            .iter()
            .flat_map(|ass| self.head_for_assignment(rule, ass))
            .filter(|fact| {
                let tag: &Tag = fact.predicate();
                !self.facts_by_pred.get(tag).unwrap().contains(fact)
            })
            .collect()
    }

    fn update_facts(&mut self, new_facts: HashSet<Fact>) {
        new_facts.into_iter().for_each(|fact| {
            let tag: &Tag = fact.predicate();
            self.facts_by_pred.get_mut(tag).unwrap().insert(fact);
        });
    }
}

type Assignment<'a> = HashMap<&'a Variable, Term>;
type VarPerAtomIdxPosIdxPerRule<'a> = HashMap<&'a Rule, HashMap<(usize, usize), &'a Variable>>;
