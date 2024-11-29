use crate::rule_model::components::{
    atom::Atom, literal::Literal, rule::Rule, tag::Tag, term::primitive::variable::Variable,
    IterablePrimitives, IterableVariables,
};
use crate::static_checks::positions::{ExtendedPositions, Positions};
use std::collections::{HashMap, HashSet};

#[warn(dead_code)]
pub trait RuleProperties {
    fn is_joinless(&self) -> bool;
    fn is_linear(&self) -> bool;
    fn is_guarded(&self) -> bool;
    fn is_domain_restricted(&self) -> bool;
    fn is_frontier_one(&self) -> bool;
    fn is_datalog(&self) -> bool;
    fn is_monadic(&self) -> bool;
    fn is_frontier_guarded(&self) -> bool;
    fn is_weakly_guarded(&self, affected_positions: &Positions) -> bool;
    fn is_weakly_frontier_guarded(&self, affected_positions: &Positions) -> bool;
    fn is_jointly_guarded(&self, attacked_pos_by_vars: &HashMap<&Variable, Positions>) -> bool;
    fn is_jointly_frontier_guarded(
        &self,
        attacked_pos_by_vars: &HashMap<&Variable, Positions>,
    ) -> bool;
    fn is_weakly_acyclic(&self) -> bool;
    fn is_jointly_acyclic(&self) -> bool;
    fn is_weakly_sticky(&self) -> bool;
    fn is_glut_guarded(&self) -> bool;
    fn is_glut_frontier_guarded(&self) -> bool;
    fn is_shy(&self, attacked_pos_by_vars: &HashMap<&Variable, Positions>) -> bool;
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
        self.join_variables().is_empty()
    }

    fn is_linear(&self) -> bool {
        1 >= self.body().len()
    }

    fn is_guarded(&self) -> bool {
        let positive_variables: HashSet<&Variable> = self.positive_variables();
        self.is_guarded_for_variables(positive_variables)
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
        let frontier_variables: HashSet<&Variable> = self.frontier_variables();
        self.is_guarded_for_variables(frontier_variables)
    }

    fn is_weakly_guarded(&self, affected_positions: &Positions) -> bool {
        let affected_universal_variables: HashSet<&Variable> =
            self.affected_universal_variables(affected_positions);
        self.is_guarded_for_variables(affected_universal_variables)
    }

    fn is_weakly_frontier_guarded(&self, affected_positions: &Positions) -> bool {
        let affected_frontier_variables: HashSet<&Variable> =
            self.affected_frontier_variables(affected_positions);
        self.is_guarded_for_variables(affected_frontier_variables)
    }

    fn is_jointly_guarded(&self, attacked_pos_by_vars: &HashMap<&Variable, Positions>) -> bool {
        let attacked_universal_variables: HashSet<&Variable> =
            self.attacked_universal_variables(attacked_pos_by_vars);
        self.is_guarded_for_variables(attacked_universal_variables)
    }

    fn is_jointly_frontier_guarded(
        &self,
        attacked_pos_by_vars: &HashMap<&Variable, Positions>,
    ) -> bool {
        let attacked_frontier_variables: HashSet<&Variable> =
            self.attacked_frontier_variables(attacked_pos_by_vars);
        self.is_guarded_for_variables(attacked_frontier_variables)
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

    // TODO: SHORTEN FUNCTION
    fn is_shy(&self, attacked_pos_by_vars: &HashMap<&Variable, Positions>) -> bool {
        self.join_variables()
            .iter()
            .all(|var| !var.is_attacked(self, attacked_pos_by_vars))
            && self
                .pairs_of_frontier_variables()
                .iter()
                .all(|[var1, var2]| {
                    attacked_pos_by_vars.values().all(|ex_var_pos| {
                        !var1.is_attacked_by_positions_in_rule(self, ex_var_pos)
                            || !var2.is_attacked_by_positions_in_rule(self, ex_var_pos)
                    })
                })
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
    // TODO: ATOM SHOULD NOT HAVE LIFETIME 'A
    fn all_positions_of_atoms<'a>(&self, atoms: &[&'a Atom]) -> Positions<'a> {
        atoms.iter().fold(Positions::new(), |all_pos, atom| {
            let positions: Positions = Positions::from(HashMap::from([(
                atom.predicate_ref(),
                (0..atom.len()).collect(),
            )]));
            all_pos.union(positions)
        })
    }

    fn all_positions_of_head(&self) -> Positions {
        let head_atoms: Vec<&Atom> = self.head_refs();
        self.all_positions_of_atoms(&head_atoms)
    }

    fn all_positions_of_positive_body(&self) -> Positions {
        let positive_body_atoms: Vec<&Atom> = self.body_positive_refs();
        self.all_positions_of_atoms(&positive_body_atoms)
    }

    /// Returns all positions of the Rule.
    pub fn all_positive_positions(&self) -> Positions {
        let all_positive_body_positions: Positions = self.all_positions_of_positive_body();
        let all_head_positions: Positions = self.all_positions_of_head();
        all_positive_body_positions.union(all_head_positions)
    }

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
        variables
            .iter()
            .filter(|var| var.is_affected(self, affected_positions))
            .copied()
            .collect()
    }

    fn attacked_frontier_variables(
        &self,
        attacked_pos_by_vars: &HashMap<&Variable, Positions>,
    ) -> HashSet<&Variable> {
        self.attacked_variables(self.frontier_variables(), attacked_pos_by_vars)
    }

    /// Returns the attacked universal variables of the rule.
    pub fn attacked_universal_variables(
        &self,
        attacked_pos_by_vars: &HashMap<&Variable, Positions>,
    ) -> HashSet<&Variable> {
        self.attacked_variables(self.positive_variables(), attacked_pos_by_vars)
    }

    fn attacked_variables<'a>(
        &self,
        variables: HashSet<&'a Variable>,
        attacked_pos_by_vars: &HashMap<&Variable, Positions>,
    ) -> HashSet<&'a Variable> {
        variables
            .iter()
            .filter(|var| var.is_attacked(self, attacked_pos_by_vars))
            .copied()
            .collect()
    }

    /// Returns the positive body atoms.
    pub fn body_positive_refs(&self) -> Vec<&Atom> {
        self.body_positive().collect()
    }

    // TODO: SHORTEN FUNCTION
    /// Returns the new found affected positions in the head based on the last iteration positions.
    pub fn conclude_affected_positions(&self, last_iteration_positions: &Positions) -> Positions {
        self.positive_variables()
            .iter()
            .filter(|var| {
                let positive_body_atoms: Vec<&Atom> = self.body_positive_refs();
                var.appears_at_positions_in_atoms(last_iteration_positions, &positive_body_atoms)
            })
            .fold(Positions::new(), |new_aff_pos_in_rule, var| {
                let pos_of_var_in_head: Positions = var.get_positions_in_head(self);
                new_aff_pos_in_rule.union(pos_of_var_in_head)
            })
    }

    // TODO: SHORTEN FUNCTION
    /// Returns the new found attacked positions in the head based on the whole attacked positions.
    pub fn conclude_attacked_positions(
        &self,
        currently_attacked_positions: &Positions,
    ) -> Positions {
        self.positive_variables()
            .iter()
            .filter(|var| var.is_attacked_by_positions_in_rule(self, currently_attacked_positions))
            .fold(Positions::new(), |new_att_pos_in_rule, var| {
                let pos_of_var_in_head: Positions = var.get_positions_in_head(self);
                new_att_pos_in_rule.union(pos_of_var_in_head)
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
                let positive_body_atoms: Vec<&Atom> = self.body_positive_refs();
                var.appears_at_positions_in_atoms(last_iteration_positions, &positive_body_atoms)
            })
            .try_fold(Positions::new(), |new_mar_pos_in_rule, var| {
                if self
                    .head()
                    .iter()
                    .any(|atom| !atom.variables().collect::<Vec<&Variable>>().contains(var))
                {
                    return None;
                }
                let pos_of_var_in_head: Positions = var.get_positions_in_head(self);
                Some(new_mar_pos_in_rule.union(pos_of_var_in_head))
            })
    }

    /// Returns all existential variables of the given rule.
    pub fn existential_variables(&self) -> HashSet<&Variable> {
        self.variables()
            .filter(|var| var.is_existential())
            .collect()
    }

    fn frontier_variables(&self) -> HashSet<&Variable> {
        let positive_body_variables: HashSet<&Variable> = self.positive_variables();
        let universal_head_variables: HashSet<&Variable> = self.universal_head_variables();
        positive_body_variables
            .intersection(&universal_head_variables)
            .copied()
            .collect()
    }

    /// Returns the head atoms as a reference.
    pub fn head_refs(&self) -> Vec<&Atom> {
        self.head().iter().collect()
    }

    fn join_variables(&self) -> HashSet<&Variable> {
        self.positive_variables()
            .iter()
            .filter(|var| var.is_join_variable_in_rule(self))
            .cloned()
            .collect()
    }

    fn pairs_of_frontier_variables(&self) -> HashSet<[&Variable; 2]> {
        self.frontier_variables().iter().fold(
            HashSet::<[&Variable; 2]>::new(),
            |mut pairs, var1| {
                self.frontier_variables()
                    .iter()
                    .filter(|var2| var1 != *var2)
                    .for_each(|var2| {
                        pairs.insert([var1, var2]);
                    });
                pairs
            },
        )
    }

    /// Returns the initial marked positions of a rule. A position of a rule is initial marked if
    /// an join variable appears on it.
    pub fn positions_of_join_variables(&self) -> Positions {
        self.join_variables()
            .iter()
            .fold(Positions::new(), |pos_of_join_vars, var| {
                let pos_of_var_in_body: Positions = var.get_positions_in_positive_body(self);
                pos_of_join_vars.union(pos_of_var_in_body)
            })
    }

    /// Returns the extended positions of all existential variables in the rule.
    pub fn extended_positions_of_existential_variables(&self) -> ExtendedPositions {
        let pos_of_ex_vars: Positions = self.positions_of_existential_variables();
        ExtendedPositions::from(pos_of_ex_vars)
    }

    /// Returns the positions of all existential variables in the rule.
    /// Therefore, it returns also the initial affected positions of the rule. A position of a rule is initial
    /// affected if an existential variable appears on it.
    pub fn positions_of_existential_variables(&self) -> Positions {
        self.existential_variables()
            .iter()
            .fold(Positions::new(), |pos_of_ex_vars, var| {
                let pos_of_var_in_head: Positions = var.get_positions_in_head(self);
                pos_of_ex_vars.union(pos_of_var_in_head)
            })
    }

    fn is_guarded_for_variables(&self, variables: HashSet<&Variable>) -> bool {
        self.body_positive_refs().iter().any(|atom| {
            let vars_of_atom: HashSet<&Variable> = atom.variables_refs();
            vars_of_atom == variables
        })
    }

    fn universal_head_variables(&self) -> HashSet<&Variable> {
        self.head().iter().fold(
            HashSet::<&Variable>::new(),
            |universal_head_variables: HashSet<&Variable>, atom| {
                let un_vars_of_atom: HashSet<&Variable> = atom.universal_variables();
                universal_head_variables
                    .union(&un_vars_of_atom)
                    .copied()
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
    pub fn appears_at_positions_in_atoms(&self, positions: &Positions, atoms: &Vec<&Atom>) -> bool {
        atoms
            .iter()
            .any(|atom| self.appears_at_positions_in_atom(positions, atom))
    }

    /// Returns the extended positions of the variable in the positive body.
    pub fn get_extended_positions_in_positive_body<'a>(
        &self,
        rule: &'a Rule,
    ) -> ExtendedPositions<'a> {
        let body_pos_of_var: Positions = self.get_positions_in_positive_body(rule);
        ExtendedPositions::from(body_pos_of_var)
    }

    /// Returns the extended positions of the variable in the head.
    pub fn get_extended_positions_in_head<'a>(&self, rule: &'a Rule) -> ExtendedPositions<'a> {
        let head_pos_of_var: Positions = self.get_positions_in_head(rule);
        ExtendedPositions::from(head_pos_of_var)
    }

    /// Returns the positions of the variable in the positive body.
    pub fn get_positions_in_positive_body<'a>(&self, rule: &'a Rule) -> Positions<'a> {
        let positive_body_atoms: Vec<&Atom> = rule.body_positive_refs();
        self.get_positions_in_atoms(&positive_body_atoms)
    }

    /// Returns the positions of the variablen in the head.
    pub fn get_positions_in_head<'a>(&self, rule: &'a Rule) -> Positions<'a> {
        let head_atoms: Vec<&Atom> = rule.head_refs();
        self.get_positions_in_atoms(&head_atoms)
    }

    /// Returns the positions of the variable in the atom.
    pub fn get_positions_in_atom<'a>(&self, atom: &'a Atom) -> Positions<'a> {
        atom.variables()
            .filter(|var| self == *var)
            .enumerate()
            .fold(Positions::new(), |mut positions_in_atom, (index, _)| {
                positions_in_atom
                    .entry(atom.predicate_ref())
                    .and_modify(|indeces| {
                        indeces.insert(index);
                    })
                    .or_insert(HashSet::from([index]));
                positions_in_atom
            })
    }

    /// Returns the positions where the variable appears in the given atoms.
    pub fn get_positions_in_atoms<'a>(&self, atoms: &[&'a Atom]) -> Positions<'a> {
        atoms
            .iter()
            .fold(Positions::new(), |positions_in_atoms, atom| {
                let positions_in_atom: Positions = self.get_positions_in_atom(atom);
                positions_in_atoms.union(positions_in_atom)
            })
    }

    fn is_affected(&self, rule: &Rule, affected_positions: &Positions) -> bool {
        let positions_of_variable_in_body: Positions = self.get_positions_in_positive_body(rule);
        affected_positions.is_superset(&positions_of_variable_in_body)
    }

    fn is_attacked(
        &self,
        rule: &Rule,
        attacked_pos_by_vars: &HashMap<&Variable, Positions>,
    ) -> bool {
        let positions_of_variable_in_body: Positions = self.get_positions_in_positive_body(rule);
        attacked_pos_by_vars
            .values()
            .any(|att_pos| att_pos.is_superset(&positions_of_variable_in_body))
    }

    fn is_attacked_by_positions_in_rule(
        &self,
        rule: &Rule,
        attacked_pos_of_var: &Positions,
    ) -> bool {
        let positions_of_variable_in_body: Positions = self.get_positions_in_positive_body(rule);
        attacked_pos_of_var.is_superset(&positions_of_variable_in_body)
    }

    /// Returns whether the variable is attacked by the given variable.
    pub fn is_attacked_by_variable(
        &self,
        attacking_var: &Variable,
        rule: &Rule,
        attacked_pos_by_vars: &HashMap<&Variable, Positions>,
    ) -> bool {
        let attacked_pos_by_var: &Positions = attacked_pos_by_vars.get(attacking_var).unwrap();
        self.is_attacked_by_positions_in_rule(rule, attacked_pos_by_var)
    }

    fn is_join_variable_in_rule(&self, rule: &Rule) -> bool {
        let positions_of_variable_in_body: Positions = self.get_positions_in_positive_body(rule);
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

    fn variables_refs(&self) -> HashSet<&Variable> {
        self.variables().collect()
    }
}

impl Literal {
    /// Return the predicate of this literal.
    pub fn predicate(&self) -> Tag {
        self.atom().predicate()
    }

    /// Returns the atom of the literal.
    pub fn atom(&self) -> &Atom {
        match self {
            Literal::Positive(atom) => atom,
            Literal::Negative(atom) => atom,
            _ => todo!("HANDLING OPERATION CASE"),
        }
    }
}
