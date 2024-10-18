use crate::rule_model::components::{
    literal::Literal,
    rule::Rule,
    tag::Tag,
    term::primitive::{variable::Variable, Primitive},
    IterablePrimitives,
};
// use crate::model::{
//     rule_model::{Tag, Literal, Rule, Term, Variable},
//     PrimitiveTerm,
// };
use crate::static_checks::static_checks_on_rule::RuleProperties;
use std::collections::{HashMap, HashSet};

struct RuleSet(Vec<Rule>);

impl RuleSet {
    fn rules(&self) -> &Vec<Rule> {
        &self.0
    }

    fn affected_positions(&self) -> Positions {
        todo!("IMPLEMENT");
        // TODO: IMPLEMENT
    }
}

pub struct Positions(pub HashMap<Tag, HashSet<usize>>);

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

    fn contains_predicate(&self, predicate: &Tag) -> bool {
        self.positions().contains_key(predicate)
    }

    fn get_and_unwrap_predicate(&self, predicate: &Tag) -> &HashSet<usize> {
        self.positions().get(predicate).unwrap()
    }

    pub fn get(&self, predicate: &Tag) -> Option<&HashSet<usize>> {
        self.positions().get(predicate)
    }

    pub fn contains_key(&self, predicate: &Tag) -> bool {
        self.positions().contains_key(predicate)
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
            affected_positions.contains_predicate(pred.0)
                && self.get_and_unwrap_predicate(pred.0).iter().all(|pos| {
                    affected_positions
                        .get_and_unwrap_predicate(pred.0)
                        .contains(pos)
                })
        })
    }

    pub fn union_positions_of_variable_in_literal(
        &self,
        variable: &Variable,
        literal: &Literal,
    ) -> HashSet<usize> {
        let predicate: Tag = literal.predicate();
        let new_positions: HashSet<usize> = variable.get_positions_in_literal(literal);
        let old_positions: &HashSet<usize> = self.get(&predicate).unwrap_or(&new_positions);
        new_positions.union(old_positions).cloned().collect()
    }
}

impl Variable {
    fn get_positions_in_literal(&self, literal: &Literal) -> HashSet<usize> {
        let mut positions_in_literal: HashSet<usize> = HashSet::<usize>::new();
        for (position, term) in literal.primitive_terms().enumerate() {
            if let Primitive::Variable(variable) = term {
                if variable == self {
                    positions_in_literal.insert(position);
                }
            }
        }
        positions_in_literal
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
