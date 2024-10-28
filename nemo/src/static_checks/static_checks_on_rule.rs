use crate::rule_model::components::{
    atom::Atom,
    literal::Literal,
    rule::Rule,
    tag::Tag,
    term::primitive::{variable::Variable, Primitive},
    IterablePrimitives, IterableVariables,
};
use crate::static_checks::positions::Positions;
use std::collections::HashSet;

#[warn(dead_code)]
pub trait RuleProperties {
    fn is_joinless(&self) -> bool;
    fn is_linear(&self) -> bool;
    fn is_guarded(&self) -> bool;
    fn is_sticky(&self, marking: &Positions) -> bool;
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
    fn is_joinless(&self) -> bool {
        self.variables()
            .all(|var| !var.is_join_variable_in_rule(self))
    }

    fn is_linear(&self) -> bool {
        1 >= self.body().len()
    }

    fn is_guarded(&self) -> bool {
        self.is_guarded_for_variables(self.positive_variables())
    }

    // TODO: SHORTEN FUNCTION
    fn is_sticky(&self, marking: &Positions) -> bool {
        self.body().iter().all(|literal| {
            literal.variables().all(|var_1| {
                if var_1.is_join_variable_in_rule(self)
                    || var_1.appears_at_positions_in_atom(marking, &literal.atom())
                {
                    self.head().iter().all(|atom| {
                        atom.variables()
                            .filter(|var_2| *var_2 == var_1)
                            .any(|var_2| var_2.appears_at_positions_in_atom(marking, atom))
                    })
                } else {
                    true
                }
            })
        })
    }

    fn is_domain_restricted(&self) -> bool {
        let positive_body_variables: HashSet<&Variable> = self.positive_variables();
        self.head().iter().all(|atom| {
            let universal_variables_of_atom: HashSet<&Variable> = atom.universal_variables();
            universal_variables_of_atom.is_empty()
                || universal_variables_of_atom == positive_body_variables
        })
    }

    fn is_frontier_one(&self) -> bool {
        1 >= self.frontier_variables().len()
    }

    fn is_datalog(&self) -> bool {
        self.head()
            .iter()
            .all(|atom| atom.existential_variables().is_empty())
    }

    fn is_monadic(&self) -> bool {
        self.head()
            .iter()
            .all(|atom| 1 == atom.primitive_terms().count())
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

// NOTE: MAYBE CREATE A TRAIT???
impl Rule {
    fn affected_frontier_variables(&self, affected_positions: &Positions) -> HashSet<&Variable> {
        self.affected_variables(self.frontier_variables(), affected_positions)
    }

    fn affected_universal_variables(&self, affected_positions: &Positions) -> HashSet<&Variable> {
        self.affected_variables(self.positive_variables(), affected_positions)
    }

    fn affected_variables<'a>(
        &self,
        variables: HashSet<&'a Variable>,
        affected_positions: &Positions,
    ) -> HashSet<&'a Variable> {
        variables.iter().fold(
            HashSet::<&Variable>::new(),
            |mut affected_variables, variable| {
                if variable.is_affected(self, affected_positions) {
                    affected_variables.insert(variable);
                }
                affected_variables
            },
        )
    }

    fn frontier_variables(&self) -> HashSet<&Variable> {
        let positive_body_variables: HashSet<&Variable> = self.positive_variables();
        let universal_head_variables: HashSet<&Variable> = self.universal_head_variables();
        positive_body_variables.iter().fold(
            HashSet::<&Variable>::new(),
            |mut frontier_variables, variable| {
                if universal_head_variables.contains(variable) {
                    frontier_variables.insert(variable);
                }
                frontier_variables
            },
        )
    }

    pub fn initial_affected_positions(&self) -> Positions {
        self.head()
            .iter()
            .fold(Positions::new(), |mut initial_affected_positions, atom| {
                initial_affected_positions.union(&atom.positions_of_existential_variables());
                initial_affected_positions
            })
    }

    fn is_guarded_for_variables(&self, variables: HashSet<&Variable>) -> bool {
        self.body().iter().any(|literal| {
            let variables_of_literal: HashSet<&Variable> = literal.variables().collect();
            variables_of_literal == variables
        })
    }

    fn universal_head_variables(&self) -> HashSet<&Variable> {
        self.head().iter().fold(
            HashSet::<&Variable>::new(),
            |universal_head_variables: HashSet<&Variable>, atom| {
                universal_head_variables
                    .union(&atom.universal_variables())
                    .cloned()
                    .collect()
            },
        )
    }
}

// NOTE: MAYBE CREATE A TRAIT???
impl Variable {
    fn appears_at_positions_in_atom(&self, positions: &Positions, atom: &Atom) -> bool {
        let positions_in_atom: Positions = self.get_positions_in_atom(atom);
        !positions_in_atom.is_disjoint(positions)
    }

    // TODO: SHORTEN FUNCTION
    pub fn get_positions_in_atom(&self, atom: &Atom) -> Positions {
        atom.variables().enumerate().fold(
            Positions::new(),
            |mut positions_in_atom, (index, var)| {
                if var == self {
                    positions_in_atom
                        .entry(atom.predicate().clone())
                        .and_modify(|indeces| {
                            indeces.insert(index);
                        })
                        .or_insert(HashSet::from([index]));
                }
                positions_in_atom
            },
        )
    }

    fn get_positions_in_atoms(&self, atoms: &[Atom]) -> Positions {
        atoms.iter().fold(
            Positions::new(),
            |mut positions_in_atoms: Positions, atom| {
                positions_in_atoms.union(&self.get_positions_in_atom(atom));
                positions_in_atoms
            },
        )
    }

    fn get_positions_in_literal(&self, literal: &Literal) -> Positions {
        self.get_positions_in_atom(&literal.atom())
    }

    fn get_positions_in_literals(&self, literals: &[Literal]) -> Positions {
        let atoms: Vec<Atom> = literals.iter().map(|literal| literal.atom()).collect();
        self.get_positions_in_atoms(&atoms)
    }

    fn is_affected(&self, rule: &Rule, affected_positions: &Positions) -> bool {
        let positions_of_variable_in_body: Positions = self.get_positions_in_literals(rule.body());
        affected_positions.is_superset(&positions_of_variable_in_body)
    }

    fn is_join_variable_in_rule(&self, rule: &Rule) -> bool {
        let positions_of_variable_in_body: Positions = self.get_positions_in_literals(rule.body());
        2 <= positions_of_variable_in_body.len()
    }
}

impl Atom {
    pub fn existential_variables(&self) -> HashSet<&Variable> {
        self.variables()
            .filter(|var| var.is_existential())
            .collect()
    }

    fn positions_of_existential_variables(&self) -> Positions {
        self.existential_variables()
            .iter()
            .map(|var| var.get_positions_in_atom(self))
            .fold(Positions::new(), |mut pos_start, pos| {
                pos_start.union(&pos);
                pos_start
            })
    }

    fn universal_variables(&self) -> HashSet<&Variable> {
        self.variables().filter(|var| var.is_universal()).collect()
    }
}

impl Literal {
    /// Return the predicate of this literal.
    pub fn predicate(&self) -> Tag {
        self.atom().predicate()
    }

    fn atom(&self) -> Atom {
        match self {
            Literal::Positive(atom) => atom.clone(),
            Literal::Negative(atom) => atom.clone(),
            _ => todo!("HANDLING OPERATION CASE"),
        }
    }
}
