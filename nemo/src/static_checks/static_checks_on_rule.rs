use crate::rule_model::components::{
    atom::Atom, literal::Literal, rule::Rule, tag::Tag, term::primitive::variable::Variable,
    IterablePrimitives, IterableVariables,
};
use crate::static_checks::positions::Positions;
use std::collections::HashSet;

#[warn(dead_code)]
pub trait RuleProperties {
    fn is_joinless(&self) -> bool;
    fn is_linear(&self) -> bool;
    fn is_guarded(&self) -> bool;
    // NOTE: IS_STICKY FUNCTION FOR SINGLE RULES PROPABLY NOT NEEDED
    // fn is_sticky(&self, marking: &Positions) -> bool;
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
        self.existential_variables().is_empty()
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

    // TODO: SHORTEN FUNCTION
    /// Returns the new found affected positions in the head based on the last iteration positions.
    pub fn conclude_affected_positions(&self, last_iteration_positions: &Positions) -> Positions {
        self.positive_variables()
            .iter()
            .filter(|var| {
                var.appears_at_positions_in_atoms(
                    last_iteration_positions,
                    self.body_positive().collect(),
                )
            })
            .fold(Positions::new(), |new_aff_pos_in_rule, var| {
                new_aff_pos_in_rule.union(&var.get_positions_in_atoms(self.head()))
            })
    }

    // TODO: SHORTEN FUNCTION
    /// Returns the new found marked positions in the head based on the last iteration positions or
    /// returns None if the marking condition for the last iteration positions fails.
    pub fn conclude_marked_positions(
        &self,
        last_iteration_positions: &Positions,
    ) -> Option<Positions> {
        self.positive_variables()
            .iter()
            .filter(|var| {
                var.appears_at_positions_in_atoms(
                    last_iteration_positions,
                    self.body_positive().collect(),
                )
            })
            .try_fold(Positions::new(), |new_mar_pos_in_rule, var| {
                if self
                    .head()
                    .iter()
                    .any(|atom| !atom.variables().collect::<Vec<&Variable>>().contains(var))
                {
                    return None;
                }
                Some(new_mar_pos_in_rule.union(&var.get_positions_in_atoms(self.head())))
            })
    }

    fn existential_variables(&self) -> HashSet<&Variable> {
        self.variables()
            .filter(|var| var.is_existential())
            .collect()
    }

    fn frontier_variables(&self) -> HashSet<&Variable> {
        let positive_body_variables: HashSet<&Variable> = self.positive_variables();
        let universal_head_variables: HashSet<&Variable> = self.universal_head_variables();
        positive_body_variables
            .intersection(&universal_head_variables)
            .cloned()
            .collect()
    }

    fn join_variables(&self) -> HashSet<&Variable> {
        self.variables()
            .filter(|var| var.is_join_variable_in_rule(self))
            .collect()
    }

    // NOTE: only use variables of positive body atoms or use all body atoms?
    /// Returns the initial marked positions of a rule. A position of a rule is initial marked if
    /// an join variable appears on it.
    pub fn positions_of_join_variables(&self) -> Positions {
        let join_variables: HashSet<&Variable> = self.join_variables();
        join_variables
            .iter()
            .fold(Positions::new(), |pos_of_join_vars, var| {
                pos_of_join_vars.union(&var.get_positions_in_literals(self.body()))
            })
    }

    /// Returns the initial affected positions of the rule. A position of a rule is initial
    /// affected if an existential variable appears on it.
    pub fn positions_of_existential_variables(&self) -> Positions {
        let existential_variables: HashSet<&Variable> = self.existential_variables();
        existential_variables
            .iter()
            .fold(Positions::new(), |pos_of_ex_vars, var| {
                pos_of_ex_vars.union(&var.get_positions_in_atoms(self.head()))
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
    /// Returns whether the variable appears in an atom at the given positions.
    pub fn appears_at_positions_in_atom(&self, positions: &Positions, atom: &Atom) -> bool {
        let positions_in_atom: Positions = self.get_positions_in_atom(atom);
        !positions_in_atom.is_disjoint(positions)
    }

    /// Returns whether the variable appears in the atoms at the given positions.
    pub fn appears_at_positions_in_atoms(
        &self,
        positions: &Positions,
        atoms: HashSet<&Atom>,
    ) -> bool {
        atoms
            .iter()
            .any(|atom| self.appears_at_positions_in_atom(positions, atom))
    }

    // TODO: SHORTEN FUNCTION
    /// Returns the positions of the variable in the atom.
    pub fn get_positions_in_atom(&self, atom: &Atom) -> Positions {
        atom.variables()
            .filter(|var| self == *var)
            .enumerate()
            .fold(Positions::new(), |mut positions_in_atom, (index, _)| {
                positions_in_atom
                    .entry(atom.predicate().clone())
                    .and_modify(|indeces| {
                        indeces.insert(index);
                    })
                    .or_insert(HashSet::from([index]));
                positions_in_atom
            })
    }

    /// Returns the positions where the variable appears in the given atoms.
    pub fn get_positions_in_atoms(&self, atoms: &[Atom]) -> Positions {
        atoms
            .iter()
            .fold(Positions::new(), |positions_in_atoms: Positions, atom| {
                positions_in_atoms.union(&self.get_positions_in_atom(atom))
            })
    }

    #[warn(dead_code)]
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
    /// Return all existential variables that appear in this atom.
    pub fn existential_variables(&self) -> HashSet<&Variable> {
        self.variables()
            .filter(|var| var.is_existential())
            .collect()
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

    /// Returns the atom of the literal.
    pub fn atom(&self) -> Atom {
        match self {
            Literal::Positive(atom) => atom.clone(),
            Literal::Negative(atom) => atom.clone(),
            _ => todo!("HANDLING OPERATION CASE"),
        }
    }
}
