//! Functionality that provides methods in relation with Rule-Types.
use crate::rule_model::components::{
    atom::Atom, rule::Rule, term::primitive::variable::Variable, IterableVariables,
};
use crate::static_checks::collection_traits::{Disjoint, InsertAll, Superset};
use crate::static_checks::positions::{Position, Positions, PositionsByRuleIdxVariables};

use std::collections::{HashMap, HashSet};

/// Type to identify a Rule in some RuleSet.
type RuleIdx = usize;

// TODO: NEEDED?
/// Type to relate an Rule to its index in some RuleSet.
pub type RuleIdxRule<'a> = (RuleIdx, &'a Rule);

// TODO: RATHER RULEVARIABLE AS TYPE -> WOULD MAKE SOME THINGS SIMPLER?
/// Type to relate an (existential) Variable to a Rule. Therefore a type to identificate an
/// (existential) Variable.
pub type RuleIdxVariable<'a> = (RuleIdx, &'a Variable);

/// Type to wrap a 'set' (Vec) of Rule(s).
#[derive(Debug)]
pub struct RuleSet(pub Vec<Rule>);

/// Type to relate 2 Variable(s) together.
#[derive(Debug, Eq, Hash, PartialEq)]
pub struct VariablePair<'a>(pub [&'a Variable; 2]);

// NOTE: KEEP?
/// This Trait provides methods to get all (HashSet<Position> / Positions) of some type.
pub trait AllPositivePositions<'a> {
    /// Returns all HashSet<Position> of some type.
    fn all_positive_extended_positions(&'a self) -> HashSet<Position<'a>>;
    /// Returns all Positions of some type.
    fn all_positive_positions(&'a self) -> Positions<'a>;
}

impl<'a> AllPositivePositions<'a> for RuleSet {
    fn all_positive_extended_positions(&'a self) -> HashSet<Position<'a>> {
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
    fn all_positive_extended_positions(&'a self) -> HashSet<Position<'a>> {
        let all_positive_positions: Positions = self.all_positive_positions();
        HashSet::<Position>::from(all_positive_positions)
    }

    fn all_positive_positions(&'a self) -> Positions<'a> {
        let all_positive_body_positions: Positions = self.all_positions_of_positive_body();
        let all_head_positions: Positions = self.all_positions_of_head();
        all_positive_body_positions.insert_all_take_ret(all_head_positions)
    }
}

// NOTE: KEEP IT IF RULEIDXRULE DOES NOT GET ELIMINATED?
/// This Trait provides a method to get all existential RuleIdxVariables of a RuleSet.
pub trait ExistentialRuleIdxVariables {
    /// Returns all existential RuleIdxVariables of a RuleSet.
    fn existential_rule_idx_variables(&self) -> HashSet<RuleIdxVariable>;
}

impl ExistentialRuleIdxVariables for RuleSet {
    // TODO: USE EXISTENTIALRULEIDXVARIABLES FOR RULEIDXRULE
    fn existential_rule_idx_variables(&self) -> HashSet<RuleIdxVariable> {
        self.0.iter().enumerate().fold(
            HashSet::<RuleIdxVariable>::new(),
            |ex_vars, (index, rule)| {
                // let idx_ex_vars: RuleIdxVariables = r_idx_rule.existential_rule_idx_variables();
                let ex_vars_of_rule: HashSet<&Variable> = rule.existential_variables();
                let idx_ex_vars: HashSet<RuleIdxVariable> = ex_vars_of_rule
                    .into_iter()
                    .map(|var| (index, var))
                    .collect();
                ex_vars.union(&idx_ex_vars).copied().collect()
            },
        )
    }
}

// impl<'a> ExistentialRuleIdxVariables for RuleIdxRule<'a> {
//     fn existential_rule_idx_variables(&self) -> RuleIdxVariables<'a> {
//         let ex_vars_of_rule: Variables = self.1.existential_variables();
//         ex_vars_of_rule.iter().map(|var| (self.0, *var)).collect()
//     }
// }

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

    fn all_positions_of_positive_body(&'a self) -> Positions<'a> {
        let positive_body_atoms: Vec<&Atom> = self.body_positive_refs();
        self.all_positions_of_atoms(&positive_body_atoms)
    }
}

/// This Impl-Block contains methods for a rule to get the (extended / normal) positions of its
/// existential variables.
impl Rule {
    /// Returns the HashSet<Position> of the existential Variables of the rule.
    pub fn extended_positions_of_existential_variables(&self) -> HashSet<Position> {
        let pos_of_ex_vars: Positions = self.positions_of_existential_variables();
        HashSet::<Position>::from(pos_of_ex_vars)
    }

    /// Returns the Positions of the existential Variables of a rule.
    pub fn positions_of_existential_variables(&self) -> Positions {
        self.existential_variables()
            .iter()
            .fold(Positions::new(), |pos_of_ex_vars, var| {
                let pos_of_var_in_head: Positions = var.positions_in_head(self);
                pos_of_ex_vars.insert_all_take_ret(pos_of_var_in_head)
            })
    }
}

/// This Impl-Block contains methods for a rule to get its frontier variable pairs.
impl Rule {
    /// Returns all pairs of frontier Variables of a Rule excluding the reflexive pairs.
    pub fn frontier_variable_pairs(&self) -> HashSet<VariablePair> {
        let frontier_variables: HashSet<&Variable> = self.frontier_variables();
        frontier_variables
            .iter()
            .fold(HashSet::<VariablePair>::new(), |mut pairs, var1| {
                frontier_variables
                    .iter()
                    .filter(|var2| var1 != *var2)
                    .for_each(|var2| {
                        pairs.insert(VariablePair([var1, var2]));
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
            .filter(|var| var.is_affected(self, affected_positions))
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
        attacked_pos_by_rule_idx_vars: &PositionsByRuleIdxVariables,
    ) -> HashSet<&Variable> {
        let frontier_variables: HashSet<&Variable> = self.frontier_variables();
        self.attacked_variables(frontier_variables, attacked_pos_by_rule_idx_vars)
    }

    /// Returns the attacked Variables of some Rule based on the attacked positions by RuleIdxVariable(s) (PositionsByRuleIdxVariables).
    pub fn attacked_universal_variables(
        &self,
        attacked_pos_by_rule_idx_vars: &PositionsByRuleIdxVariables,
    ) -> HashSet<&Variable> {
        let universal_variables: HashSet<&Variable> = self.positive_variables();
        self.attacked_variables(universal_variables, attacked_pos_by_rule_idx_vars)
    }

    /// Returns the attacked Variables out of some Variables of some Rule based on the attacked positions by RuleIdxVariable(s) (PositionsByRuleIdxVariables).
    fn attacked_variables(
        &self,
        variables: HashSet<&'a Variable>,
        attacked_pos_by_rule_idx_vars: &PositionsByRuleIdxVariables,
    ) -> HashSet<&'a Variable> {
        variables
            .iter()
            .filter(|var| var.is_attacked(self, attacked_pos_by_rule_idx_vars))
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
        attacked_pos_by_cycle_rule_idx_vars: &PositionsByRuleIdxVariables,
    ) -> HashSet<&Variable> {
        let frontier_variables: HashSet<&Variable> = self.frontier_variables();
        self.attacked_variables(frontier_variables, attacked_pos_by_cycle_rule_idx_vars)
    }

    /// Returns the attacked glut Variables of a Rule.
    pub fn attacked_universal_glut_variables(
        &self,
        attacked_pos_by_cycle_rule_idx_vars: &PositionsByRuleIdxVariables,
    ) -> HashSet<&Variable> {
        let universal_variables: HashSet<&Variable> = self.positive_variables();
        self.attacked_variables(universal_variables, attacked_pos_by_cycle_rule_idx_vars)
    }
}

/// This Impl-Block contains methods for a variable to determine if it appears in a special way in
/// a rule.
impl Variable {
    /// Checks if some Variable appears at multiple atoms in the positive body of some rule.
    pub fn appears_in_multiple_positive_body_atoms(&self, rule: &Rule) -> bool {
        rule.body_positive_refs()
            .iter()
            .filter(|atom| self.appears_in_atom(atom))
            .count()
            > 1
    }

    fn appears_without_other_in_positive_body_atom(&self, other: &Variable, rule: &Rule) -> bool {
        rule.body_positive_refs()
            .iter()
            .filter(|atom| self.appears_in_atom(atom) && !other.appears_in_atom(atom))
            .count()
            > 0
    }
}

/// This Impl-Block contains methods for a variable to get its positions in some part of a rule.
impl<'a> Variable {
    /// Returns the HashSet<Position> of a Variable in the head of some Rule.
    pub fn extended_positions_in_positive_body(&self, rule: &'a Rule) -> HashSet<Position<'a>> {
        let body_pos_of_var: Positions = self.positions_in_positive_body(rule);
        HashSet::<Position>::from(body_pos_of_var)
    }

    /// Returns the HashSet<Position> of a Variable in the positive body of some Rule.
    pub fn extended_positions_in_head(&self, rule: &'a Rule) -> HashSet<Position<'a>> {
        let head_pos_of_var: Positions = self.positions_in_head(rule);
        HashSet::<Position>::from(head_pos_of_var)
    }

    /// Returns the Positions of a Variable in the head of some Rule.
    fn positions_in_positive_body(&self, rule: &'a Rule) -> Positions<'a> {
        let positive_body_atoms: Vec<&Atom> = rule.body_positive_refs();
        self.positions_in_atoms(&positive_body_atoms)
    }

    /// Returns the Positions of a Variable in the positive body of some Rule.
    pub fn positions_in_head(&self, rule: &'a Rule) -> Positions<'a> {
        let head_atoms: Vec<&Atom> = rule.head_refs();
        self.positions_in_atoms(&head_atoms)
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

/// This Impl-Block contains methods for a variable to check if it is attacked with different input
/// parameters.
impl Variable {
    /// Returns the attacked frontier Variables of a Rule.
    pub fn is_attacked(
        &self,
        rule: &Rule,
        attacked_pos_by_rule_idx_vars: &PositionsByRuleIdxVariables,
    ) -> bool {
        let positions_of_variable_in_body: Positions = self.positions_in_positive_body(rule);
        attacked_pos_by_rule_idx_vars
            .0
            .values()
            .any(|att_pos| att_pos.is_superset(&positions_of_variable_in_body))
    }

    /// Returns the attacked Variables of a Rule.
    pub fn is_attacked_by_positions_in_rule(
        &self,
        rule: &Rule,
        attacked_pos_of_var: &Positions,
    ) -> bool {
        let positions_of_variable_in_body: Positions = self.positions_in_positive_body(rule);
        attacked_pos_of_var.is_superset(&positions_of_variable_in_body)
    }

    /// Returns the attacked Variables out of some Variables of a Rule.
    pub fn is_attacked_by_variable(
        &self,
        attacking_rule_idx_var: &RuleIdxVariable,
        rule: &Rule,
        attacked_pos_by_rule_idx_vars: &PositionsByRuleIdxVariables,
    ) -> bool {
        let attacked_pos_by_var: &Positions = attacked_pos_by_rule_idx_vars
            .0
            .get(attacking_rule_idx_var)
            .unwrap();
        self.is_attacked_by_positions_in_rule(rule, attacked_pos_by_var)
    }
}

/// This Impl-Block contains a method for a variable to check if it is affected.
impl Variable {
    /// Checks whether a Variable is attacked in some Rule based on the
    /// affected Positions.
    fn is_affected(&self, rule: &Rule, affected_positions: &Positions) -> bool {
        let positions_of_variable_in_body: Positions = self.positions_in_positive_body(rule);
        affected_positions.is_superset(&positions_of_variable_in_body)
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
impl VariablePair<'_> {
    // TODO: REEVALUATE FUNCTION BECAUSE '2 FRONTIER VARIABLE(S) APPEAR IN DIFFERENT ATOM(S)' IS
    // NOT A CLEAR DEFINITION
    /// Checks if some Variables of a VariablePair appear at different atoms in the positive body
    /// of some rule.
    pub fn appear_in_different_positive_body_atoms(&self, rule: &Rule) -> bool {
        let found_var1_alone: bool =
            self.0[0].appears_without_other_in_positive_body_atom(self.0[1], rule);
        let found_var2_alone: bool =
            self.0[1].appears_without_other_in_positive_body_atom(self.0[0], rule);
        found_var1_alone && found_var2_alone
    }
}
