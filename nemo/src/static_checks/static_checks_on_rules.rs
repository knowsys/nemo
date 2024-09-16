use crate::model::{
    rule_model::{Atom, Literal, Rule, Term, Variable},
    PrimitiveTerm,
};
use std::collections::HashSet;

pub trait RuleProperties {
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
        let mut variables: HashSet<Variable> = HashSet::<Variable>::new();
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
        1 >= self.body().len()
    }

    fn is_guarded(&self) -> bool {
        let all_body_variables_from_positive_literals: HashSet<Variable> = self.safe_variables();
        for literal in self.body().iter() {
            let variables_of_literal: HashSet<Variable> = literal.variables().cloned().collect();
            if all_body_variables_from_positive_literals == variables_of_literal {
                return true;
            }
        }
        false
    }

    fn is_sticky(&self) -> bool {
        todo!("IMPLEMENT");
        // TODO: IMPLEMENT
    }

    fn is_domain_restricted(&self) -> bool {
        // let all_body_variables_from_positive_literals: HashSet<Variable> = self.safe_variables();
        // for atom in self.head().iter() {
        //     let variables_of_atom: HashSet<Variable> = atom
        //         .primitive_terms()
        //         .map(|term| (*term).clone())
        //         .filter(|term| PrimitiveTerm::Variable(variable) == term)
        //         .collect();
        // }
        todo!("CORRECT FUNCTION");
        // TODO: CORRECT FUNCTION
    }

    fn is_frontier_one(&self) -> bool {
        1 >= self.frontier_variables().len()
    }

    fn is_datalog(&self) -> bool {
        let atoms_of_head: &Vec<Atom> = self.head();
        for atom in atoms_of_head.iter() {
            let existential_variables_of_atom: Vec<Variable> =
                atom.existential_variables().cloned().collect();
            if !existential_variables_of_atom.is_empty() {
                return false;
            }
        }
        true
    }

    fn is_monadic(&self) -> bool {
        let atoms_of_head: &Vec<Atom> = self.head();
        for atom in atoms_of_head.iter() {
            if atom.terms().len() != 1 {
                return false;
            }
        }
        true
    }

    fn is_frontier_guarded(&self) -> bool {
        let frontier_variables: HashSet<Variable> = self.frontier_variables();
        for literal in self.body().iter() {
            let variables_of_literal: HashSet<Variable> = literal.variables().cloned().collect();
            if frontier_variables == variables_of_literal {
                return true;
            }
        }
        false
    }

    fn is_weakly_guarded(&self) -> bool {
        todo!("IMPLEMENT");
        // TODO: IMPLEMENT
    }

    fn is_weakly_frontier_guarded(&self) -> bool {
        todo!("IMPLEMENT");
        // TODO: IMPLEMENT
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

fn disjoint(a: &HashSet<Variable>, b: &HashSet<Variable>) -> bool {
    for variable in a.iter() {
        if b.contains(variable) {
            return false;
        }
    }
    true
}
