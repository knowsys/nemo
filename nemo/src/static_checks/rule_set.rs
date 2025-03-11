//! Functionality that provides methods in relation with Rule-Types.
use crate::rule_model::components::{
    atom::Atom, rule::Rule, term::primitive::variable::Variable, IterableVariables,
};
use crate::static_checks::collection_traits::{Disjoint, InsertAll, Superset};
use crate::static_checks::positions::{Position, Positions, PositionsByRuleAndVariables};

use std::collections::{HashMap, HashSet};

/// Type to relate an (existential) Variable to a Rule. Therefore a type to identificate an
/// (existential) Variable.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct RuleAndVariable<'a>(pub &'a Rule, pub &'a Variable);

impl<'a> Ord for RuleAndVariable<'a> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.1.cmp(other.1)
    }
}

impl<'a> PartialOrd for RuleAndVariable<'a> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.1.partial_cmp(other.1)
    }
}

// TODO: USE A REFERENCE TO RULE
/// Type to wrap a 'set' (Vec) of Rule(s).
#[derive(Debug)]
pub struct RuleSet(pub Vec<Rule>);

/// Type to relate 2 Variable(s) together.
#[derive(Debug, Eq, Hash, PartialEq)]
pub struct RuleAndVariablePair<'a>(pub [RuleAndVariable<'a>; 2]);

// NOTE: KEEP?
/// This Trait provides methods to get all (HashSet<Position> / Positions) of some type.
pub trait AllPositivePositions<'a> {
    /// Returns all HashSet<Position> of some type.
    fn all_positive_positions_as_set(&'a self) -> HashSet<Position<'a>>;
    /// Returns all Positions of some type.
    fn all_positive_positions(&'a self) -> Positions<'a>;
}

impl<'a> AllPositivePositions<'a> for RuleSet {
    fn all_positive_positions_as_set(&'a self) -> HashSet<Position<'a>> {
        let all_positive_positions: Positions = self.all_positive_positions();
        HashSet::<Position>::from(all_positive_positions)
    }

    fn all_positive_positions(&'a self) -> Positions<'a> {
        self.0.iter().fold(Positions::new(), |all_pos, rule| {
            let all_pos_of_rule: Positions = rule.all_positive_positions();
            all_pos.insert_all_take_ret(all_pos_of_rule)
        })
    }
}

impl<'a> AllPositivePositions<'a> for Rule {
    fn all_positive_positions_as_set(&'a self) -> HashSet<Position<'a>> {
        let all_positive_positions: Positions = self.all_positive_positions();
        HashSet::<Position>::from(all_positive_positions)
    }

    fn all_positive_positions(&'a self) -> Positions<'a> {
        let all_positive_body_positions: Positions = self.all_positions_of_positive_body();
        let all_head_positions: Positions = self.all_positions_of_head();
        all_positive_body_positions.insert_all_take_ret(all_head_positions)
    }
}

// NOTE: KEEP IT?
/// This Trait provides a method to get all existential variables combined with its rule of a RuleSet.
pub trait ExistentialRuleAndVariables {
    /// Returns all existential variables combined with its rule of a RuleSet.
    fn existential_rule_and_variables(&self) -> HashSet<RuleAndVariable>;
}

impl ExistentialRuleAndVariables for RuleSet {
    fn existential_rule_and_variables(&self) -> HashSet<RuleAndVariable> {
        self.0
            .iter()
            .fold(HashSet::<RuleAndVariable>::new(), |ex_vars, rule| {
                let ex_rule_and_vars: HashSet<RuleAndVariable> =
                    rule.existential_rule_and_variables();
                ex_vars.union(&ex_rule_and_vars).copied().collect()
            })
    }
}

impl<'a> ExistentialRuleAndVariables for Rule {
    fn existential_rule_and_variables(&self) -> HashSet<RuleAndVariable> {
        let ex_vars_of_rule: HashSet<&Variable> = self.existential_variables();
        ex_vars_of_rule
            .into_iter()
            .map(|var| RuleAndVariable(self, var))
            .collect()
    }
}

// NOTE: KEEP?
/// This Trait provides a method to get all the existential Variables of some type.
pub trait ExistentialVariables {
    /// Returns all the existential Variables of some type.
    fn existential_variables(&self) -> HashSet<&Variable>;
}

impl ExistentialVariables for RuleSet {
    fn existential_variables(&self) -> HashSet<&Variable> {
        self.0
            .iter()
            .fold(HashSet::<&Variable>::new(), |ex_vars, rule| {
                let ex_vars_of_rule: HashSet<&Variable> = rule.existential_variables();
                ex_vars.union(&ex_vars_of_rule).copied().collect()
            })
    }
}

impl ExistentialVariables for Rule {
    fn existential_variables(&self) -> HashSet<&Variable> {
        self.variables()
            .filter(|var| var.is_existential())
            .collect()
    }
}

impl ExistentialVariables for Atom {
    fn existential_variables(&self) -> HashSet<&Variable> {
        self.variables()
            .filter(|var| var.is_existential())
            .collect()
    }
}

/// This Trait provides a method to get the frontier Variables of a Rule.
pub trait FrontierVariables {
    /// Returns the frontier Variables of a Rule.
    fn frontier_variables(&self) -> HashSet<&Variable>;
}

impl FrontierVariables for Rule {
    fn frontier_variables(&self) -> HashSet<&Variable> {
        let positive_body_variables: HashSet<&Variable> = self.positive_variables();
        let universal_head_variables: HashSet<&Variable> = self.universal_head_variables();
        positive_body_variables
            .intersection(&universal_head_variables)
            .copied()
            .collect()
    }
}

/// This Trait provides a method to get the join Variables of a Rule.
pub trait JoinVariables {
    /// Returns the join Variables of a Rule.
    fn join_variables(&self) -> HashSet<&Variable>;
}

impl JoinVariables for Rule {
    fn join_variables(&self) -> HashSet<&Variable> {
        let positive_variables: Vec<&Variable> = self.positive_variables_as_vec();
        positive_variables
            .iter()
            .filter(|var| positive_variables.iter().filter(|var1| var1 == var).count() > 1)
            .copied()
            .collect()
    }
}

/// This Impl-Block contains a method for an atom to get its universal variables.
impl Atom {
    /// This method returns the universal Variables of an Atom.
    pub fn universal_variables(&self) -> HashSet<&Variable> {
        self.variables().filter(|var| var.is_universal()).collect()
    }
}

/// This Impl-Block contains a method for an atom to get its variables as a reference.
impl Atom {
    /// Returns all the Variables of an Atom.
    pub fn variables_refs(&self) -> HashSet<&Variable> {
        self.variables().collect()
    }
}

/// This Impl-Block contains methods for a rule to get its (positive body / head) atoms.
impl Rule {
    /// Returns the positive body Atom(s) as reference(s) of a Rule.
    pub fn body_positive_refs(&self) -> Vec<&Atom> {
        self.body_positive().collect()
    }

    fn head_refs(&self) -> Vec<&Atom> {
        self.head().iter().collect()
    }
}

/// This Impl-Block contains methods for a rule to get all of the positions of its (positive body / head) variables.
impl<'a> Rule {
    fn all_positions_of_atoms(&self, atoms: &[&'a Atom]) -> Positions<'a> {
        atoms.iter().fold(Positions::new(), |all_pos, atom| {
            let positions: Positions = Positions(HashMap::from([(
                atom.predicate_ref(),
                (0..atom.len()).collect(),
            )]));
            all_pos.insert_all_take_ret(positions)
        })
    }

    fn all_positions_of_head(&'a self) -> Positions<'a> {
        let head_atoms: Vec<&Atom> = self.head_refs();
        self.all_positions_of_atoms(&head_atoms)
    }

    /// Returns all the positions of the head as a set.
    pub fn all_positions_of_head_as_set(&'a self) -> HashSet<Position<'a>> {
        let all_pos_of_head: Positions<'a> = self.all_positions_of_head();
        HashSet::<Position>::from(all_pos_of_head)
    }

    fn all_positions_of_positive_body(&'a self) -> Positions<'a> {
        let positive_body_atoms: Vec<&Atom> = self.body_positive_refs();
        self.all_positions_of_atoms(&positive_body_atoms)
    }

    /// Returns all the positions of the positive body as a set.
    pub fn all_positions_of_positive_body_as_set(&'a self) -> HashSet<Position<'a>> {
        let all_pos_of_positive_body: Positions<'a> = self.all_positions_of_positive_body();
        HashSet::<Position>::from(all_pos_of_positive_body)
    }
}

/// This Impl-Block contains methods for a rule to get the positions (set | compressed) of its
/// existential variables.
impl Rule {
    /// Returns the positions of the existential Variables of the rule as a set.
    pub fn positions_of_existential_variables_as_set(&self) -> HashSet<Position> {
        let pos_of_ex_vars: Positions = self.positions_of_existential_variables();
        HashSet::<Position>::from(pos_of_ex_vars)
    }

    /// Returns the Positions of the existential Variables of a rule.
    pub fn positions_of_existential_variables(&self) -> Positions {
        self.existential_rule_and_variables().into_iter().fold(
            Positions::new(),
            |pos_of_ex_var, rule_and_var| {
                let pos_of_ex_var_in_head: Positions = rule_and_var.positions_in_head();
                pos_of_ex_var.insert_all_take_ret(pos_of_ex_var_in_head)
            },
        )
    }
}

/// This Impl-Block contains methods for a rule to get its frontier variable pairs.
impl Rule {
    /// Returns all pairs of frontier Variables of a Rule excluding the reflexive pairs.
    pub fn frontier_rule_and_variable_pairs(&self) -> HashSet<RuleAndVariablePair> {
        let frontier_variables: HashSet<&Variable> = self.frontier_variables();
        frontier_variables
            .iter()
            .fold(HashSet::<RuleAndVariablePair>::new(), |mut pairs, var1| {
                frontier_variables
                    .iter()
                    .filter(|var2| var1 != *var2)
                    .for_each(|var2| {
                        pairs.insert(RuleAndVariablePair([
                            RuleAndVariable(self, var1),
                            RuleAndVariable(self, var2),
                        ]));
                    });
                pairs
            })
    }
}

/// This Impl-Block contains a method to check if the rule is guarded for a set of variables.
impl Rule {
    /// Checks whether the Rule is gaurded for some Variables.
    pub fn is_guarded_for_variables(&self, variables: HashSet<&Variable>) -> bool {
        if variables.is_empty() {
            return true;
        }
        self.body_positive_refs().iter().any(|atom| {
            let vars_of_atom: HashSet<&Variable> = atom.variables_refs();
            vars_of_atom.is_superset(&variables)
        })
    }
}

/// This Impl-Block contains a method for a rule to get the universal variables of its head.
impl Rule {
    fn universal_head_variables(&self) -> HashSet<&Variable> {
        self.head().iter().fold(
            HashSet::<&Variable>::new(),
            |universal_head_variables: HashSet<&Variable>, atom| {
                let un_vars_of_atom: HashSet<&Variable> = atom.universal_variables();
                universal_head_variables.insert_all_take_ret(un_vars_of_atom)
            },
        )
    }
}

/// This Impl-Block contains methods for a rule to get its affected (frontier | universal)
/// variables.
impl<'a> Rule {
    /// Returns the affected frontier Variables of a Rule.
    pub fn affected_frontier_variables(
        &self,
        affected_positions: &Positions,
    ) -> HashSet<&Variable> {
        self.affected_variables(self.frontier_variables(), affected_positions)
    }

    /// Returns the affected universal Variables of a Rule.
    pub fn affected_universal_variables(
        &self,
        affected_positions: &Positions,
    ) -> HashSet<&Variable> {
        self.affected_variables(self.positive_variables(), affected_positions)
    }

    /// Returns the affected Variables out of some Variables of a Rule.
    fn affected_variables(
        &self,
        variables: HashSet<&'a Variable>,
        affected_positions: &Positions,
    ) -> HashSet<&'a Variable> {
        variables
            .iter()
            .filter(|var| RuleAndVariable(self, var).is_affected(affected_positions))
            .copied()
            .collect()
    }
}

/// This Impl-Block contains methods for a rule to get its attacked (frontier | universal)
/// variables.
impl<'a> Rule {
    /// Returns the attacked frontier Variables of some Rule based on the attacked positions by RuleIdxVariable(s) (PositionsByRuleIdxVariables).
    pub fn attacked_frontier_variables(
        &self,
        attacked_pos_by_rule_and_vars: &PositionsByRuleAndVariables,
    ) -> HashSet<&Variable> {
        let frontier_variables: HashSet<&Variable> = self.frontier_variables();
        self.attacked_variables(frontier_variables, attacked_pos_by_rule_and_vars)
    }

    /// Returns the attacked Variables of some Rule based on the attacked positions by RuleIdxVariable(s) (PositionsByRuleIdxVariables).
    pub fn attacked_universal_variables(
        &self,
        attacked_pos_by_rule_and_vars: &PositionsByRuleAndVariables,
    ) -> HashSet<&Variable> {
        let universal_variables: HashSet<&Variable> = self.positive_variables();
        self.attacked_variables(universal_variables, attacked_pos_by_rule_and_vars)
    }

    /// Returns the attacked Variables out of some Variables of some Rule based on the attacked positions by RuleIdxVariable(s) (PositionsByRuleIdxVariables).
    fn attacked_variables(
        &self,
        variables: HashSet<&'a Variable>,
        attacked_pos_by_rule_and_vars: &PositionsByRuleAndVariables,
    ) -> HashSet<&'a Variable> {
        variables
            .iter()
            .filter(|var| RuleAndVariable(self, var).is_attacked(attacked_pos_by_rule_and_vars))
            .copied()
            .collect()
    }
}

/// This Impl-Block contains methods for a rule to get its attacked (frontier | universal) glut
/// variables.
impl Rule {
    /// Returns the attacked frontier glut Variables of a Rule.
    pub fn attacked_frontier_glut_variables(
        &self,
        attacked_pos_by_cycle_rule_and_vars: &PositionsByRuleAndVariables,
    ) -> HashSet<&Variable> {
        let frontier_variables: HashSet<&Variable> = self.frontier_variables();
        self.attacked_variables(frontier_variables, attacked_pos_by_cycle_rule_and_vars)
    }

    /// Returns the attacked glut Variables of a Rule.
    pub fn attacked_universal_glut_variables(
        &self,
        attacked_pos_by_cycle_rule_and_vars: &PositionsByRuleAndVariables,
    ) -> HashSet<&Variable> {
        let universal_variables: HashSet<&Variable> = self.positive_variables();
        self.attacked_variables(universal_variables, attacked_pos_by_cycle_rule_and_vars)
    }
}

/// This Impl-Block contains methods for a variable to check if it is attacked with different input
/// parameters.
impl RuleAndVariable<'_> {
    /// Returns the attacked frontier Variables of a Rule.
    pub fn is_attacked(&self, attacked_pos_by_rule_and_vars: &PositionsByRuleAndVariables) -> bool {
        let positions_of_variable_in_body: Positions = self.positions_in_positive_body();
        attacked_pos_by_rule_and_vars
            .0
            .values()
            .any(|att_pos| att_pos.is_superset(&positions_of_variable_in_body))
    }

    /// Returns the attacked Variables of a Rule.
    pub fn is_attacked_by_positions(&self, attacked_pos_of_rule_and_var: &Positions) -> bool {
        let positions_of_variable_in_body: Positions = self.positions_in_positive_body();
        attacked_pos_of_rule_and_var.is_superset(&positions_of_variable_in_body)
    }

    /// Returns the attacked Variables out of some Variables of a Rule.
    pub fn is_attacked_by_rule_and_variable(
        &self,
        attacking_rule_and_var: &RuleAndVariable,
        attacked_pos_by_rule_and_vars: &PositionsByRuleAndVariables,
    ) -> bool {
        let attacked_pos_of_rule_and_var: &Positions = attacked_pos_by_rule_and_vars
            .0
            .get(attacking_rule_and_var)
            .unwrap();
        self.is_attacked_by_positions(attacked_pos_of_rule_and_var)
    }
}

/// This Impl-Block contains a method for a variable to check if it is affected.
impl RuleAndVariable<'_> {
    /// Checks whether a Variable is attacked in some Rule based on the
    /// affected Positions.
    fn is_affected(&self, affected_positions: &Positions) -> bool {
        let positions_of_variable_in_body: Positions = self.positions_in_positive_body();
        affected_positions.is_superset(&positions_of_variable_in_body)
    }
}

/// This Impl-Block contains methods for a variable to determine if it appears in a special way in
/// a rule.
impl RuleAndVariable<'_> {
    /// Checks if some Variable appears at multiple atoms in the positive body of some rule.
    pub fn appears_in_multiple_positive_body_atoms(&self) -> bool {
        self.0
            .body_positive_refs()
            .iter()
            .filter(|atom| self.1.appears_in_atom(atom))
            .count()
            > 1
    }

    fn appears_without_other_in_positive_body_atom(&self, other: &RuleAndVariable) -> bool {
        self.0
            .body_positive_refs()
            .iter()
            .filter(|atom| self.1.appears_in_atom(atom) && !other.1.appears_in_atom(atom))
            .count()
            > 0
    }
}

/// This Impl-Block contains methods for a variable to get its positions in some part of a rule.
impl<'a> RuleAndVariable<'a> {
    /// Returns the HashSet<Position> of a Variable in the head of some Rule.
    pub fn positions_in_positive_body_as_set(self) -> HashSet<Position<'a>> {
        let body_pos_of_var: Positions = self.positions_in_positive_body();
        HashSet::<Position>::from(body_pos_of_var)
    }

    /// Returns the HashSet<Position> of a Variable in the positive body of some Rule.
    pub fn positions_in_head_as_set(self) -> HashSet<Position<'a>> {
        let head_pos_of_var: Positions = self.positions_in_head();
        HashSet::<Position>::from(head_pos_of_var)
    }

    /// Returns the Positions of a Variable in the head of some Rule.
    fn positions_in_positive_body(self) -> Positions<'a> {
        let positive_body_atoms: Vec<&Atom> = self.0.body_positive_refs();
        self.1.positions_in_atoms(&positive_body_atoms)
    }

    /// Returns the Positions of a Variable in the positive body of some Rule.
    pub fn positions_in_head(self) -> Positions<'a> {
        let head_atoms: Vec<&Atom> = self.0.head_refs();
        self.1.positions_in_atoms(&head_atoms)
    }
}

/// This Impl-Block contains methods for a variable to get its positions in an atom or some atoms.
impl<'a> Variable {
    /// Returns the Positions where the Variable appears in the Atom.
    fn positions_in_atom(&self, atom: &'a Atom) -> Positions<'a> {
        atom.variables()
            .enumerate()
            .filter(|(_, var)| self == *var)
            .fold(Positions::new(), |mut positions_in_atom, (index, _)| {
                positions_in_atom
                    .0
                    .entry(atom.predicate_ref())
                    .and_modify(|indeces| {
                        indeces.insert(index);
                    })
                    .or_insert(HashSet::from([index]));
                positions_in_atom
            })
    }

    /// Returns the Positions where the Variable appears in the Atom(s).
    fn positions_in_atoms(&self, atoms: &[&'a Atom]) -> Positions<'a> {
        atoms
            .iter()
            .fold(Positions::new(), |positions_in_atoms, atom| {
                let positions_in_atom: Positions = self.positions_in_atom(atom);
                positions_in_atoms.insert_all_take_ret(positions_in_atom)
            })
    }
}

/// This Impl-Block contains methods for a variable to check its appearance on positions of an atom
/// or some atoms.
impl Variable {
    /// Checks if some Variable appears at some Positions in some Atom.
    fn appears_at_some_positions_in_atom(&self, positions: &Positions, atom: &Atom) -> bool {
        let positions_in_atom: Positions = self.positions_in_atom(atom);
        !positions_in_atom.is_disjoint(positions)
    }

    /// Checks if some Variable appears at some Positions in some Atoms.
    pub fn appears_at_some_positions_in_atoms(
        &self,
        positions: &Positions,
        atoms: &[&Atom],
    ) -> bool {
        atoms
            .iter()
            .any(|atom| self.appears_at_some_positions_in_atom(positions, atom))
    }

    /// Checks if some Variable appears only at Positions in some Atom.
    fn appears_only_at_positions_in_atom(&self, positions: &Positions, atom: &Atom) -> bool {
        let positions_in_atom: Positions = self.positions_in_atom(atom);
        positions.is_superset(&positions_in_atom)
    }

    /// Checks if some Variable appears only at Positions in some Atoms.
    pub fn appears_only_at_positions_in_atoms(
        &self,
        positions: &Positions,
        atoms: &[&Atom],
    ) -> bool {
        atoms
            .iter()
            .all(|atom| self.appears_only_at_positions_in_atom(positions, atom))
    }
}

/// Thisk Impl-Block contains methods for a variable to check its appearance in an atom or some
/// atoms.
impl Variable {
    /// Checks if some Variable appears in some Atom.
    fn appears_in_atom(&self, atom: &Atom) -> bool {
        atom.variables_refs().contains(self)
    }

    /// Checks if some Variable appears in some Atoms.
    fn appears_in_atoms(&self, atoms: &[&Atom]) -> bool {
        atoms.iter().any(|atom| self.appears_in_atom(atom))
    }
}

/// This Impl-Block contains a method for a variable pair to check if its variables appear in
/// different positive body atoms of a rule.
impl RuleAndVariablePair<'_> {
    // TODO: REEVALUATE FUNCTION BECAUSE '2 FRONTIER VARIABLE(S) APPEAR IN DIFFERENT ATOM(S)' IS
    // NOT A CLEAR DEFINITION
    /// Checks if some Variables of a VariablePair appear at different atoms in the positive body
    /// of some rule.
    pub fn appear_in_different_positive_body_atoms(&self) -> bool {
        let found_var1_alone: bool =
            self.0[0].appears_without_other_in_positive_body_atom(&self.0[1]);
        let found_var2_alone: bool =
            self.0[1].appears_without_other_in_positive_body_atom(&self.0[0]);
        found_var1_alone && found_var2_alone
    }
}
