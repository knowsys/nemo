use nemo::{
    execution::{DefaultExecutionEngine, ExecutionEngine},
    io::{resource_providers::ResourceProviders, ImportManager},
    rule_model::{
        components::{
            atom::Atom,
            fact::Fact,
            rule::Rule,
            tag::Tag,
            term::{primitive::variable::Variable, Term},
        },
        program::{Program, ProgramBuilder},
    },
};

use crate::static_checks::collection_traits::InsertAll;
use crate::static_checks::positions::Positions;
use crate::static_checks::rule_properties::RuleProperties;
use crate::static_checks::rule_set::{RuleRefs, RuleSet};

use std::collections::{HashMap, HashSet};

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
        let mut new_facts: HashSet<Fact> = self.critical_instance();
        // println!("{:#?}", new_facts);
        let mut cur_facts: HashSet<Fact> = new_facts.clone();
        let datalog_rules: Vec<&Rule> = self.datalog_rules();
        // println!("{:?}", datalog_rules);
        let existential_rules: Vec<&Rule> = self.existential_rules();
        // println!("{:?}", existential_rules);
        let datalog_predicates: HashSet<&Tag> = self.predicates_of_datalog_rules(&datalog_rules);
        let existential_predicates: HashSet<&Tag> =
            self.predicates_of_existential_rules(&existential_rules);
        let mut datalog_execution_engine: DefaultExecutionEngine;
        let mut existential_reasoner: ExistentialReasoner = ExistentialReasoner::new(
            existential_predicates,
            &new_facts,
            existential_rules,
            variant,
        );
        while !new_facts.is_empty() {
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
                cur_facts.insert_all_take(facts_of_pred);
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
    body_pos_per_var_per_rule: BodyPosPerVarPerRule<'a>,
    variant: ChaseVariant,
}

impl<'a> ExistentialReasoner<'a> {
    fn assignment_for_fact(&self, fact: &Fact) -> Option<Assignment> {
        fact.subterms()
            .try_fold(Assignment::new(), |ret_val, term| {
                todo!("");
            })
    }

    fn assignments_for_rule(
        &self,
        rule: &Rule,
        new_facts_by_pred: &HashMap<&Tag, HashSet<Fact>>,
    ) -> Vec<Assignment> {
        let preds_of_body: Vec<&Tag> = rule
            .body_positive_refs()
            .iter()
            .map(|atom| atom.predicate_ref())
            .collect();
        for (i, start_pred) in preds_of_body
            .into_iter()
            .filter(|pred| !new_facts_by_pred.get(pred).unwrap().is_empty())
            .enumerate()
        {
            let facts_for_start_pred: &HashSet<Fact> = new_facts_by_pred.get(start_pred).unwrap();
            if facts_for_start_pred.is_empty() {
                continue;
            }
            let unfiltered_assignments_for_start_predicate: Vec<Assignment> = facts_for_start_pred
                .iter()
                .filter_map(|fact| self.assignment_for_fact(fact))
                .collect();
        }
        // preds_of_body
        //     .into_iter()
        //     .filter(|pred| !new_facts_by_pred.get(pred).unwrap().is_empty())
        //     .enumerate()
        //     .for_each(|(i, start_pred)| {
        //         let facts_for_start_pred: &HashSet<Fact> =
        //             new_facts_by_pred.get(start_pred).unwrap();
        //         let unfiltered_assignments_for_start_predicate: Vec<Assignment> =
        //             facts_for_start_pred
        //                 .iter()
        //                 .map(|fact| self.assignment_for_fact(fact))
        //                 .collect();
        //     });
        todo!("IMPLEMENT");
        // TODO: IMPLEMENT
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

    fn filter_new_facts(&self, new_facts: HashSet<Fact>) -> HashSet<Fact> {
        new_facts
            .into_iter()
            .filter(|fact| self.facts_by_pred.contains_key(fact.predicate()))
            .collect()
    }

    fn head_for_assignment(&self, rule: &Rule, ass: &Assignment) -> HashSet<Fact> {
        todo!("IMPLEMENT");
        // TODO: IMPLEMENT
    }

    fn new(
        predicates: HashSet<&'a Tag>,
        facts: &HashSet<Fact>,
        rules: Vec<&'a Rule>,
        variant: ChaseVariant,
    ) -> Self {
        let facts_by_pred: HashMap<&Tag, HashSet<Fact>> = predicates.into_iter().fold(
            HashMap::<&Tag, HashSet<Fact>>::new(),
            |mut ret_val, pred| {
                let facts_of_pred: HashSet<Fact> = facts
                    .iter()
                    .filter(|fact| fact.predicate() == pred)
                    .cloned()
                    .collect();
                ret_val.insert(pred, facts_of_pred);
                ret_val
            },
        );
        let body_pos_per_var_per_rule: BodyPosPerVarPerRule =
            rules
                .iter()
                .fold(BodyPosPerVarPerRule::new(), |mut ret_val, rule| {
                    let body_pos_per_var_of_rule: HashMap<&Variable, Positions> =
                        rule.positive_variables_iter().fold(
                            HashMap::<&Variable, Positions>::new(),
                            |pos_per_var_of_rule, var| {
                                todo!("");
                            },
                        );
                    ret_val.insert(rule, body_pos_per_var_of_rule);
                    ret_val
                });
        Self {
            facts_by_pred,
            rules,
            body_pos_per_var_per_rule,
            variant,
        }
    }

    fn run_every_rule_once(&mut self, new_facts: HashSet<Fact>) -> HashSet<Fact> {
        let filtered_facts: HashSet<Fact> = self.filter_new_facts(new_facts);
        self.update_facts(filtered_facts.clone());
        let filtered_facts_by_pred: HashMap<&Tag, HashSet<Fact>> =
            self.convert_set_to_map(filtered_facts);
        self.rules
            .iter()
            .fold(HashSet::<Fact>::new(), |ret_val, rule| {
                let concluded_facts: HashSet<Fact> = self.run_rule(rule, &filtered_facts_by_pred);
                ret_val.insert_all_take_ret(concluded_facts)
            })
    }

    fn run_rule(
        &self,
        rule: &Rule,
        filtered_facts_by_pred: &HashMap<&Tag, HashSet<Fact>>,
    ) -> HashSet<Fact> {
        let assignments: Vec<Assignment> = self.assignments_for_rule(rule, filtered_facts_by_pred);

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
            // self.facts_by_pred
            //     .entry(tag)
            //     .and_modify(|facts| {
            //         facts.insert(fact.clone());
            //     })
            //     .or_insert(HashSet::from([fact]));
        });
    }
}

type Assignment = HashMap<Variable, Term>;
type BodyPosPerVarPerRule<'a> = HashMap<&'a Rule, HashMap<&'a Variable, Positions<'a>>>;
