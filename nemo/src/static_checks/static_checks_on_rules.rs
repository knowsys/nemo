use crate::rule_model::components::{
    atom::Atom,
    literal::Literal,
    rule::Rule,
    tag::Tag,
    term::primitive::{variable::Variable, Primitive},
    IterablePrimitives,
};
use crate::static_checks::static_checks_on_rule::RuleProperties;
use std::collections::{HashMap, HashSet};

struct RuleSet(Vec<Rule>);

impl RuleSet {
    fn rules(&self) -> &Vec<Rule> {
        &self.0
    }

    fn affected_positions(&self) -> Positions {
        let initial_affected_positions: Positions = self.initial_affected_positions();
        todo!("IMPLEMENT");
        // TODO: IMPLEMENT
    }

    fn initial_affected_positions(&self) -> Positions {
        let mut initial_affected_positions: Positions = Positions::new();
        for rule in self.rules().iter() {
            initial_affected_positions.union(&rule.initial_affected_positions());
        }
        initial_affected_positions
    }
}

#[derive(Debug)]
pub struct Positions(HashMap<Tag, HashSet<usize>>);

impl Positions {
    pub fn new() -> Self {
        Positions(HashMap::<Tag, HashSet<usize>>::new())
    }

    pub fn positions(&self) -> &HashMap<Tag, HashSet<usize>> {
        &self.0
    }

    fn positions_mut(&mut self) -> &mut HashMap<Tag, HashSet<usize>> {
        &mut self.0
    }

    pub fn get(&self, predicate: &Tag) -> Option<&HashSet<usize>> {
        self.positions().get(predicate)
    }

    fn get_mut(&mut self, predicate: &Tag) -> Option<&mut HashSet<usize>> {
        self.positions_mut().get_mut(predicate)
    }

    pub fn contains_key(&self, predicate: &Tag) -> bool {
        self.positions().contains_key(predicate)
    }

    pub fn get_predicate_and_unwrap(&self, predicate: &Tag) -> &HashSet<usize> {
        self.positions().get(predicate).unwrap()
    }

    pub fn get_predicate_and_unwrap_mut(&mut self, predicate: &Tag) -> &mut HashSet<usize> {
        self.positions_mut().get_mut(predicate).unwrap()
    }

    pub fn insert(
        &mut self,
        predicate: Tag,
        positions_in_predicate: HashSet<usize>,
    ) -> Option<HashSet<usize>> {
        self.positions_mut()
            .insert(predicate, positions_in_predicate)
    }

    pub fn contains_only_affected_variable_position(&self, affected_positions: &Positions) -> bool {
        self.positions().iter().all(|pred| {
            affected_positions.contains_key(pred.0)
                && self.get_predicate_and_unwrap(pred.0).iter().all(|pos| {
                    affected_positions
                        .get_predicate_and_unwrap(pred.0)
                        .contains(pos)
                })
        })
    }

    pub fn union(&mut self, positions: &Positions) {
        for pred in positions.positions().keys() {
            if !self.contains_key(pred) {
                self.insert(
                    pred.clone(),
                    positions.get_predicate_and_unwrap(pred).clone(),
                );
            }
            self.insert(pred.clone(), self.new_positions_of_pred(pred, positions));
        }
    }

    fn new_positions_of_pred(&self, pred: &Tag, positions: &Positions) -> HashSet<usize> {
        self.get_predicate_and_unwrap(pred)
            .union(positions.get_predicate_and_unwrap(pred))
            .copied()
            .collect()
    }
}

pub trait RulesProperties {
    fn is_joinless(&self) -> bool;
    fn is_linear(&self) -> bool;
    fn is_guarded(&self) -> bool;
    fn is_sticky(&self) -> bool;
    fn is_domain_restricted(&self) -> bool;
    fn is_frontier_one(&self) -> bool;
    fn is_datalog(&self) -> bool;
    fn is_monadic(&self) -> bool;
    fn is_frontier_guarded(&self) -> bool;
    fn is_weakly_guarded(&self) -> bool;
    fn is_weakly_frontier_guarded(&self) -> bool;
    fn is_jointly_guarded(&self) -> bool;
    fn is_jointly_frontier_guarded(&self) -> bool;
    fn is_weakly_acyclic(&self) -> bool;
    fn is_jointly_acyclic(&self) -> bool;
    fn is_weakly_sticky(&self) -> bool;
    fn is_glut_guarded(&self) -> bool;
    fn is_glut_frontier_guarded(&self) -> bool;
    fn is_shy(&self) -> bool;
    fn is_mfa(&self) -> bool;
    fn is_dmfa(&self) -> bool;
    fn is_rmfa(&self) -> bool;
    fn is_mfc(&self) -> bool;
    fn is_dmfc(&self) -> bool;
    fn is_drpc(&self) -> bool;
    fn is_rpc(&self) -> bool;
}

impl RulesProperties for RuleSet {
    fn is_joinless(&self) -> bool {
        self.0.iter().all(|rule| rule.is_joinless())
    }

    fn is_linear(&self) -> bool {
        self.0.iter().all(|rule| rule.is_linear())
    }

    fn is_guarded(&self) -> bool {
        self.0.iter().all(|rule| rule.is_guarded())
    }

    fn is_sticky(&self) -> bool {
        todo!("IMPLEMENT");
        // TODO: IMPLEMENT
    }

    fn is_domain_restricted(&self) -> bool {
        self.0.iter().all(|rule| rule.is_domain_restricted())
    }

    fn is_frontier_one(&self) -> bool {
        self.0.iter().all(|rule| rule.is_frontier_one())
    }

    fn is_datalog(&self) -> bool {
        self.0.iter().all(|rule| rule.is_datalog())
    }

    fn is_monadic(&self) -> bool {
        self.0.iter().all(|rule| rule.is_monadic())
    }

    fn is_frontier_guarded(&self) -> bool {
        self.0.iter().all(|rule| rule.is_frontier_guarded())
    }

    fn is_weakly_guarded(&self) -> bool {
        let affected_positions: Positions = self.affected_positions();
        self.0
            .iter()
            .all(|rule| rule.is_weakly_guarded(&affected_positions))
    }

    fn is_weakly_frontier_guarded(&self) -> bool {
        let affected_positions: Positions = self.affected_positions();
        self.0
            .iter()
            .all(|rule| rule.is_weakly_frontier_guarded(&affected_positions))
    }

    fn is_jointly_guarded(&self) -> bool {
        todo!("IMPLEMENT");
        // TODO: IMPLEMENT
    }

    fn is_jointly_frontier_guarded(&self) -> bool {
        todo!("IMPLEMENT");
        // TODO: IMPLEMENT
    }
    fn is_weakly_acyclic(&self) -> bool {
        todo!("IMPLEMENT");
        // TODO: IMPLEMENT
    }
    fn is_jointly_acyclic(&self) -> bool {
        todo!("IMPLEMENT");
        // TODO: IMPLEMENT
    }
    fn is_weakly_sticky(&self) -> bool {
        todo!("IMPLEMENT");
        // TODO: IMPLEMENT
    }
    fn is_glut_guarded(&self) -> bool {
        todo!("IMPLEMENT");
        // TODO: IMPLEMENT
    }
    fn is_glut_frontier_guarded(&self) -> bool {
        todo!("IMPLEMENT");
        // TODO: IMPLEMENT
    }
    fn is_shy(&self) -> bool {
        todo!("IMPLEMENT");
        // TODO: IMPLEMENT
    }
    fn is_mfa(&self) -> bool {
        todo!("IMPLEMENT");
        // TODO: IMPLEMENT
    }
    fn is_dmfa(&self) -> bool {
        todo!("IMPLEMENT");
        // TODO: IMPLEMENT
    }
    fn is_rmfa(&self) -> bool {
        todo!("IMPLEMENT");
        // TODO: IMPLEMENT
    }
    fn is_mfc(&self) -> bool {
        todo!("IMPLEMENT");
        // TODO: IMPLEMENT
    }
    fn is_dmfc(&self) -> bool {
        todo!("IMPLEMENT");
        // TODO: IMPLEMENT
    }
    fn is_drpc(&self) -> bool {
        todo!("IMPLEMENT");
        // TODO: IMPLEMENT
    }
    fn is_rpc(&self) -> bool {
        todo!("IMPLEMENT");
        // TODO: IMPLEMENT
    }
}
