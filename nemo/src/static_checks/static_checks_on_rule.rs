use crate::rule_model::components::{
    atom::Atom,
    literal::Literal,
    rule::Rule,
    tag::Tag,
    term::primitive::{variable::Variable, Primitive},
    IterablePrimitives, IterableVariables,
};
use crate::static_checks::static_checks_on_rules::Positions;
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
    // fn is_joinless(&self) -> bool {
    //     let mut variables: HashSet<Variable> = HashSet::<Variable>::new();
    //     for literal in self.body().iter() {
    //         let terms: &Vec<Term> = literal.atom().terms();
    //         for term in terms.iter() {
    //             if let Term::Primitive(PrimitiveTerm::Variable(variable)) = term {
    //                 if !variables.insert(variable.clone()) {
    //                     return false;
    //                 }
    //             }
    //         }
    //     }
    //     true
    // }

    fn is_joinless(&self) -> bool {
        let mut variables: HashSet<Variable> = HashSet::<Variable>::new();
        for literal in self.body().iter() {
            for term in literal.primitive_terms() {
                if let Primitive::Variable(variable) = term {
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
        self.is_guarded_for_variables(self.safe_variables_dereferenced())
    }

    fn is_sticky(&self) -> bool {
        todo!("IMPLEMENT");
        // TODO: IMPLEMENT
    }

    fn is_domain_restricted(&self) -> bool {
        let all_body_variables_from_positive_literals: HashSet<Variable> =
            self.safe_variables_dereferenced();
        for atom in self.head().iter() {
            let universal_variables_of_atom: HashSet<Variable> = atom.universal_variables();
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
            let existential_variables_of_atom: HashSet<Variable> = atom.existential_variables();
            if !existential_variables_of_atom.is_empty() {
                return false;
            }
        }
        true
    }

    // fn is_monadic(&self) -> bool {
    //     let atoms_of_head: &Vec<Atom> = self.head();
    //     for atom in atoms_of_head.iter() {
    //         if atom.terms().len() != 1 {
    //             return false;
    //         }
    //     }
    //     true
    // }

    fn is_monadic(&self) -> bool {
        let atoms_of_head: &Vec<Atom> = self.head();
        for atom in atoms_of_head.iter() {
            if atom.primitive_terms().count() != 1 {
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
    fn safe_variables_dereferenced(&self) -> HashSet<Variable> {
        self.safe_variables()
            .iter()
            .map(|var| (*var).clone())
            .collect()
    }

    // fn safe_variables_atom(atoms: &[Atom]) -> HashSet<Variable> {
    //     let mut result: HashSet<Variable> = HashSet::<Variable>::new();
    //     for atom in atoms.iter() {
    //         for term in atom.terms() {
    //             if let Term::Primitive(PrimitiveTerm::Variable(Variable::Universal(name))) = term {
    //                 result.insert(Variable::Universal(name.clone()));
    //             }
    //         }
    //     }
    //     result
    // }

    fn safe_variables_atom(atoms: &[Atom]) -> HashSet<Variable> {
        let mut result: HashSet<Variable> = HashSet::<Variable>::new();
        for atom in atoms.iter() {
            for term in atom.primitive_terms() {
                if let Primitive::Variable(Variable::Universal(name)) = term {
                    result.insert(Variable::Universal(name.clone()));
                }
            }
        }
        result
    }

    fn safe_variables_of_head(&self) -> HashSet<Variable> {
        Self::safe_variables_atom(self.head())
    }

    fn frontier_variables(&self) -> HashSet<Variable> {
        let safe_body_variables: HashSet<Variable> = self.safe_variables_dereferenced();
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
            if variable.is_affected(self, affected_positions) {
                affected_variables.insert(variable.clone());
            }
        }
        affected_variables
    }

    fn affected_universal_variables(&self, affected_positions: &Positions) -> HashSet<Variable> {
        self.affected_variables(self.safe_variables_dereferenced(), affected_positions)
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

    // TODO: SHORTEN FUNCTION
    pub fn initial_affected_positions(&self) -> Positions {
        let mut initial_affected_positions: Positions = Positions::new();
        for atom in self.head().iter() {
            initial_affected_positions.union(
                &atom
                    .existential_variables()
                    .iter()
                    .map(|var| var.get_positions_in_literal(&Literal::Positive(atom.clone())))
                    .fold(Positions::new(), |mut pos_start, pos| {
                        pos_start.union(&pos);
                        pos_start
                    }),
            );
        }
        initial_affected_positions
    }
}

impl Variable {
    fn is_affected(&self, rule: &Rule, affected_positions: &Positions) -> bool {
        let positions_of_variable_in_body: Positions = self.get_positions_in_literals(rule.body());
        positions_of_variable_in_body.contains_only_affected_variable_position(affected_positions)
    }

    fn get_positions_in_literals(&self, literals: &[Literal]) -> Positions {
        let mut positions_of_variable_in_body: Positions = Positions::new();
        for literal in literals.iter() {
            positions_of_variable_in_body.union(&self.get_positions_in_literal(literal));
        }
        positions_of_variable_in_body
    }

    // TODO: SHORTEN FUNCTION
    pub fn get_positions_in_literal(&self, literal: &Literal) -> Positions {
        let mut positions_in_literal: Positions = Positions::new();
        let predicate: Tag = literal.predicate();
        positions_in_literal.insert(predicate.clone(), HashSet::<usize>::new());
        for (position, term) in literal.primitive_terms().enumerate() {
            if let Primitive::Variable(variable) = term {
                if variable == self {
                    positions_in_literal
                        .get_predicate_and_unwrap_mut(&predicate)
                        .insert(position);
                }
            }
        }
        if positions_in_literal
            .get_predicate_and_unwrap(&predicate)
            .is_empty()
        {
            return Positions::new();
        }
        positions_in_literal
    }
}

impl Atom {
    pub fn existential_variables(&self) -> HashSet<Variable> {
        self.variables()
            .filter_map(|var| {
                if let Variable::Existential(ex) = var {
                    Some(Variable::Existential(ex.clone()))
                } else {
                    None
                }
            })
            .collect()
    }

    fn universal_variables(&self) -> HashSet<Variable> {
        self.variables()
            .filter_map(|var| {
                if let Variable::Universal(un) = var {
                    Some(Variable::Universal(un.clone()))
                } else {
                    None
                }
            })
            .collect()
    }
}

impl Literal {
    pub fn predicate(&self) -> Tag {
        let atom: Atom = match self.atom() {
            Some(at) => at,
            None => panic!("not possible to get an operation here"),
        };
        atom.predicate()
    }

    fn atom(&self) -> Option<Atom> {
        match self {
            Literal::Positive(atom) => Some(atom.clone()),
            Literal::Negative(atom) => Some(atom.clone()),
            _ => None,
        }
    }
}
