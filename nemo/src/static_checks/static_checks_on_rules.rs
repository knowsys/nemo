use crate::model::{
    rule_model::{Atom, Literal, Rule, Term, Variable},
    PrimitiveTerm,
};
use std::collections::HashSet;

pub trait RuleProperties {
    fn is_joinless(&self) -> bool;
    fn is_linear(&self) -> bool;
    fn is_guarded(&self) -> bool;
    // fn is_sticky(&self) -> bool;
    fn is_domain_restricted(&self) -> bool;
    /*
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
    */
    // TODO: MORE CHECKS
}

impl RuleProperties for Rule {
    /* fn is_joinless(self: &Self) -> bool {
        let mut variables: HashSet<Variable> = HashSet::new();
        for literal in self.body().iter() {
            let terms: &Vec<Term> = literal.atom().terms();
            for term in terms.iter() {
                if let Term::Primitive(PrimitiveTerm::Variable(variable)) = term {
                    if !variables.insert(variable.clone()) {
                        return false;
                    }
                }
            }
        }
        true
    } */

    fn is_joinless(&self) -> bool {
        let mut variables: HashSet<Variable> = HashSet::new();
        for literal in self.body().iter() {
            let terms: &Vec<Term> = literal.atom().terms();
            for term in terms.iter() {
                if let Term::Primitive(PrimitiveTerm::Variable(variable)) = term {
                    if !variables.insert(variable.clone()) {
                        return false;
                    }
                }
            }
        }
        true
    }

    fn is_linear(&self) -> bool {
        1 == self.body().len()
    }

    fn is_guarded(&self) -> bool {
        let all_body_variables_from_positive_literals: HashSet<Variable> = self.safe_variables();
        for literal in self.body().iter() {
            let variables_of_literal: HashSet<Variable> = literal
                .variables()
                .map(|literal| (*literal).clone())
                .collect();
            if all_body_variables_from_positive_literals == variables_of_literal {
                return true;
            }
        }
        false
    }

    // fn is_sticky(&self) -> bool {}

    fn is_domain_restricted(&self) -> bool {
        let all_body_variables_from_positive_literals: HashSet<Variable> = self.safe_variables();
        // let all_head_variables: HashSet<Variable> = self.
    }
}
