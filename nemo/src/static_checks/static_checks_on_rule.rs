use crate::model::{
    rule_model::{Atom, Identifier, Literal, Rule, Term, Variable},
    PrimitiveTerm,
};
use crate::static_checks::static_checks_on_rules::Positions;
use std::collections::{HashMap, HashSet};

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
    fn is_weakly_guarded(&self, affected_positions: &Positions) -> bool;
    fn is_weakly_frontier_guarded(&self, affected_positions: &Positions) -> bool;
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
        self.is_guarded_for_variables(self.safe_variables())
    }

    fn is_sticky(&self) -> bool {
        todo!("IMPLEMENT");
        // TODO: IMPLEMENT
    }

    fn is_domain_restricted(&self) -> bool {
        let all_body_variables_from_positive_literals: HashSet<Variable> = self.safe_variables();
        for atom in self.head().iter() {
            let universal_variables_of_atom: HashSet<Variable> = atom
                .primitive_terms()
                .filter_map(|term| {
                    if let PrimitiveTerm::Variable(Variable::Universal(universal_variable)) = term {
                        Some(Variable::Universal(universal_variable.clone()))
                    } else {
                        None
                    }
                })
                .collect();
            if !universal_variables_of_atom.is_empty()
                && universal_variables_of_atom != all_body_variables_from_positive_literals
            {
                return false;
            }
        }
        true
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
        self.is_guarded_for_variables(self.frontier_variables())
    }

    fn is_weakly_guarded(&self, affected_positions: &Positions) -> bool {
        self.is_guarded_for_variables(self.affected_universal_variables(affected_positions))
    }

    fn is_weakly_frontier_guarded(&self, affected_positions: &Positions) -> bool {
        self.is_guarded_for_variables(self.affected_frontier_variables(affected_positions))
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

impl Rule {
    fn safe_variables_atom(atoms: &[Atom]) -> HashSet<Variable> {
        let mut result: HashSet<Variable> = HashSet::<Variable>::new();
        for atom in atoms.iter() {
            for term in atom.terms() {
                if let Term::Primitive(PrimitiveTerm::Variable(Variable::Universal(name))) = term {
                    result.insert(Variable::Universal(name.clone()));
                }
            }
        }
        result
    }

    fn safe_variables_of_head(&self) -> HashSet<Variable> {
        Self::safe_variables_atom(&self.head)
    }

    fn frontier_variables(&self) -> HashSet<Variable> {
        let safe_body_variables: HashSet<Variable> = self.safe_variables();
        let safe_head_variables: HashSet<Variable> = self.safe_variables_of_head();
        let mut frontier_variables: HashSet<Variable> = HashSet::<Variable>::new();
        for variable in safe_body_variables.iter() {
            if safe_head_variables.contains(variable) {
                frontier_variables.insert(variable.clone());
            }
        }
        frontier_variables
    }

    fn affected_variables(
        &self,
        variables: HashSet<Variable>,
        affected_positions: &Positions,
    ) -> HashSet<Variable> {
        let mut affected_variables: HashSet<Variable> = HashSet::<Variable>::new();
        for variable in variables.iter() {
            if variable.is_affected(&self, affected_positions) {
                affected_variables.insert(variable.clone());
            }
        }
        affected_variables
    }

    fn affected_universal_variables(&self, affected_positions: &Positions) -> HashSet<Variable> {
        self.affected_variables(self.safe_variables(), affected_positions)
    }

    fn affected_frontier_variables(&self, affected_positions: &Positions) -> HashSet<Variable> {
        self.affected_variables(self.frontier_variables(), affected_positions)
    }

    fn is_guarded_for_variables(&self, variables: HashSet<Variable>) -> bool {
        for literal in self.body().iter() {
            let variables_of_literal: HashSet<Variable> = literal.variables().cloned().collect();
            if variables == variables_of_literal {
                return true;
            }
        }
        false
    }

    fn get_positions_of_variable_in_body(&self, variable: &Variable) -> Positions {
        // let mut positions_of_variable_in_body: Positions = Positions::new();
        // for literal in self.body().iter() {
        //     let predicate: Identifier = literal.predicate();
        //     let variables_of_literal: Vec<Variable> = literal.variables().cloned().collect();
        //     if variables_of_literal.contains(variable) {
        //         let position_of_variable_in_literal: usize =
        //             literal.get_position_of_variable(variable);
        //         if !positions_of_variable_in_body.contains_key(&predicate) {
        //             positions_of_variable_in_body
        //                 .insert(predicate.clone(), HashSet::<usize>::new());
        //         }
        //         positions_of_variable_in_body
        //             .get(&predicate)
        //             .unwrap()
        //             .insert(position_of_variable_in_literal);
        //     }
        // }
        todo!("IMPLEMENT");
        // TODO: IMPLEMENT
    }
}

impl Literal {
    fn get_position_of_variable(&self, variable: &Variable) -> usize {
        todo!("IMPLEMENT");
        // TODO: IMPLEMENT
    }
}

impl Variable {
    fn is_affected(&self, rule: &Rule, affected_positions: &Positions) -> bool {
        let positions_of_variable_in_body: Positions = rule.get_positions_of_variable_in_body(self);
        positions_of_variable_in_body.contains_only_affected_variable_position(affected_positions)
    }
}
