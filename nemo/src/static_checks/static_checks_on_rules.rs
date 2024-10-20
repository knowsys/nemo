use crate::rule_model::components::{
    atom::Atom,
    literal::Literal,
    rule::Rule,
    tag::Tag,
    term::primitive::{variable::Variable, Primitive},
    IterablePrimitives,
};
use crate::static_checks::static_checks_on_rule::RuleProperties;
use std::collections::{hash_map::Keys, hash_set::Union, HashMap, HashSet};
use std::hash::RandomState;

struct RuleSet(Vec<Rule>);

impl RuleSet {
    fn rules(&self) -> &Vec<Rule> {
        &self.0
    }

    fn affected_positions(&self) -> Positions {
        let mut affected_positions: Positions = self.initial_affected_positions();
        let mut new_in_last_iteration: Positions = affected_positions.clone();
        while !new_in_last_iteration.is_empty() {
            let mut new_found_affected_positions: Positions =
                new_in_last_iteration.conclude_affected_positions();
            new_found_affected_positions.subtract(&affected_positions);
            affected_positions.union(&new_found_affected_positions);
            new_in_last_iteration = new_found_affected_positions;
        }
        affected_positions
    }

    fn initial_affected_positions(&self) -> Positions {
        let mut initial_affected_positions: Positions = Positions::new();
        for rule in self.rules().iter() {
            initial_affected_positions.union(&rule.initial_affected_positions());
        }
        initial_affected_positions
    }
}

#[derive(Debug, Clone)]
pub struct Positions(HashMap<Tag, HashSet<usize>>);

impl Default for Positions {
    fn default() -> Self {
        Positions::new()
    }
}

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

    fn keys(&self) -> Keys<Tag, HashSet<usize>> {
        self.positions().keys()
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

    fn is_empty(&self) -> bool {
        self.positions().is_empty()
    }

    fn pred_is_empty(&self, pred: &Tag) -> bool {
        self.get_predicate_and_unwrap(pred).is_empty()
    }

    fn pred_contains_index(&self, pred: &Tag, index: &usize) -> bool {
        self.get_predicate_and_unwrap(pred).contains(index)
    }

    fn pred_remove_index(&mut self, pred: &Tag, index: &usize) -> bool {
        self.get_predicate_and_unwrap_mut(pred).remove(index)
    }

    fn remove(&mut self, pred: &Tag) -> Option<HashSet<usize>> {
        self.positions_mut().remove(pred)
    }

    pub fn subsumes(&self, positions: &Positions) -> bool {
        positions.keys().all(|pred| {
            self.contains_key(pred)
                && positions
                    .get_predicate_and_unwrap(pred)
                    .iter()
                    .all(|index| self.pred_contains_index(pred, index))
        })
    }

    fn conclude_affected_positions(&self) -> Positions {
        todo!("IMPLEMENT");
        // TODO: IMPLEMENT
    }

    // TODO: LOOK IF FUNCTION WILL WORK DUE TO USE OF CLONE
    // TODO: SHORTEN FUNCTION
    fn subtract(&mut self, positions: &Positions) {
        for pred in self.clone().keys() {
            if !positions.contains_key(pred) {
                continue;
            }
            self.subtract_indexes_of_pred(pred, positions);
        }
    }

    // TODO: LOOK IF FUNCTION WILL WORK DUE TO USE OF CLONE
    fn subtract_indexes_of_pred(&mut self, pred: &Tag, positions: &Positions) {
        for index in self.clone().get_predicate_and_unwrap(pred) {
            if !positions.pred_contains_index(pred, index) {
                continue;
            }
            self.pred_remove_index(pred, index);
        }
        if self.pred_is_empty(pred) {
            self.remove(pred);
        }
    }

    pub fn union(&mut self, positions: &Positions) {
        for pred in positions.keys() {
            if !self.contains_key(pred) {
                self.insert(
                    pred.clone(),
                    positions.get_predicate_and_unwrap(pred).clone(),
                );
            }
            self.union_indexes_of_pred(pred, positions);
            // self.insert(pred.clone(), self.union_indexes_of_pred(pred, positions));
        }
    }

    // TODO: RETURN UNION TYPE
    fn union_indexes_of_pred(&self, pred: &Tag, positions: &Positions) {
        self.get_predicate_and_unwrap(pred)
            .union(positions.get_predicate_and_unwrap(pred));
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
