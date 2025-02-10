//! Functionality that provides methods in relation with Rule-Types.
use crate::rule_model::components::{
    atom::Atom, rule::Rule, term::primitive::variable::Variable, IterableVariables,
};
use crate::static_checks::collection_traits::{Disjoint, InsertAll, Superset};
use crate::static_checks::positions::{
    AffectedPositionsBuilder, AttackedPositionsBuilder, AttackingType, ExtendedPositions,
    FromPositions, MarkedPositionsBuilder, MarkingType, Positions, PositionsByRuleIdxVariables,
};

use std::collections::{HashMap, HashSet};

/// Type to identify a Rule in some RuleSet.
pub type RuleIdx = usize;

/// Type to relate an Rule to its index in some RuleSet.
pub type RuleIdxRule<'a> = (RuleIdx, &'a Rule);

/// Type to relate an (existential) Variable to a Rule. Therefore a type to identificate an
/// (existential) Variable.
pub type RuleIdxVariable<'a> = (RuleIdx, &'a Variable);

/// Type to compress a set of RuleIdxVariable(s).
pub type RuleIdxVariables<'a> = HashSet<RuleIdxVariable<'a>>;

/// Type to compress a set of Rule(s).
pub type RuleSet = Vec<Rule>;

/// Type to compress a set of Variable(s).
pub type Variables<'a> = HashSet<&'a Variable>;

/// Type to relate 2 Variable(s) together.
pub type VariablePair<'a> = [&'a Variable; 2];

/// Type to compress a set of VariablePair(s).
pub type VariablePairs<'a> = HashSet<VariablePair<'a>>;

/// This Trait provides a method to check whether a Variable is attacked in some Rule based on the
/// affected Positions.
pub trait Affected {
    /// Checks whether a Variable is attacked in some Rule based on the
    /// affected Positions.
    fn is_affected(&self, rule: &Rule, affected_positions: &Positions) -> bool;
}

impl Affected for Variable {
    fn is_affected(&self, rule: &Rule, affected_positions: &Positions) -> bool {
        let positions_of_variable_in_body: Positions = self.positions_in_positive_body(rule);
        affected_positions.is_superset(&positions_of_variable_in_body)
    }
}

/// This Trait provides methods to get the (frontier / universal (all)) affected Variables of a
/// Rule.
pub trait AffectedVariables<'a> {
    /// Returns the affected frontier Variables of a Rule.
    fn affected_frontier_variables(&self, affected_positions: &Positions) -> Variables;
    /// Returns the affected universal Variables of a Rule.
    fn affected_universal_variables(&self, affected_positions: &Positions) -> Variables;
    /// Returns the affected Variables out of some Variables of a Rule.
    fn affected_variables(
        &self,
        variables: Variables<'a>,
        affected_positions: &Positions,
    ) -> Variables<'a>;
}

impl<'a> AffectedVariables<'a> for Rule {
    fn affected_frontier_variables(&self, affected_positions: &Positions) -> Variables {
        self.affected_variables(self.frontier_variables(), affected_positions)
    }

    fn affected_universal_variables(&self, affected_positions: &Positions) -> Variables {
        self.affected_variables(self.positive_variables(), affected_positions)
    }

    fn affected_variables(
        &self,
        variables: Variables<'a>,
        affected_positions: &Positions,
    ) -> Variables<'a> {
        variables
            .iter()
            .filter(|var| var.is_affected(self, affected_positions))
            .copied()
            .collect()
    }
}

/// This Trait gives checks for appearance of some Variable in some Atom(s).
pub trait AtomAppearance {
    /// Checks if some Variable appears in some Atom.
    fn appears_in_atom(&self, atom: &Atom) -> bool;
    /// Checks if some Variable appears in some Atoms.
    fn appears_in_atoms(&self, atoms: &[&Atom]) -> bool;
}

impl AtomAppearance for Variable {
    fn appears_in_atom(&self, atom: &Atom) -> bool {
        atom.variables_refs().contains(self)
    }

    fn appears_in_atoms(&self, atoms: &[&Atom]) -> bool {
        atoms.iter().any(|atom| self.appears_in_atom(atom))
    }
}

/// This Trait gives checks for appearance of some Variable (at some / only at) Positions in some
/// Atom(s).
pub trait AtomPositionsAppearance {
    /// Checks if some Variable appears at some Positions in some Atom.
    fn appears_at_some_positions_in_atom(&self, positions: &Positions, atom: &Atom) -> bool;
    /// Checks if some Variable appears at some Positions in some Atoms.
    fn appears_at_some_positions_in_atoms(&self, positions: &Positions, atoms: &[&Atom]) -> bool;
    /// Checks if some Variable appears only at Positions in some Atom.
    fn appears_only_at_positions_in_atom(&self, positions: &Positions, atom: &Atom) -> bool;
    /// Checks if some Variable appears only at Positions in some Atoms.
    fn appears_only_at_positions_in_atoms(&self, positions: &Positions, atoms: &[&Atom]) -> bool;
}

impl AtomPositionsAppearance for Variable {
    fn appears_at_some_positions_in_atom(&self, positions: &Positions, atom: &Atom) -> bool {
        let positions_in_atom: Positions = self.positions_in_atom(atom);
        !positions_in_atom.is_disjoint(positions)
    }

    fn appears_at_some_positions_in_atoms(&self, positions: &Positions, atoms: &[&Atom]) -> bool {
        atoms
            .iter()
            .any(|atom| self.appears_at_some_positions_in_atom(positions, atom))
    }

    fn appears_only_at_positions_in_atom(&self, positions: &Positions, atom: &Atom) -> bool {
        let positions_in_atom: Positions = self.positions_in_atom(atom);
        positions.is_superset(&positions_in_atom)
    }

    fn appears_only_at_positions_in_atoms(&self, positions: &Positions, atoms: &[&Atom]) -> bool {
        atoms
            .iter()
            .all(|atom| self.appears_only_at_positions_in_atom(positions, atom))
    }
}

/// This Trait provides methods to get the Positions of a Variable in (Atom / Atom(s)).
pub trait AtomPositions<'a> {
    /// Returns the Positions where the Variable appears in the Atom.
    fn positions_in_atom(&self, atom: &'a Atom) -> Positions<'a>;
    /// Returns the Positions where the Variable appears in the Atom(s).
    fn positions_in_atoms(&self, atoms: &[&'a Atom]) -> Positions<'a>;
}

impl<'a> AtomPositions<'a> for Variable {
    fn positions_in_atom(&self, atom: &'a Atom) -> Positions<'a> {
        atom.variables()
            .enumerate()
            .filter(|(_, var)| self == *var)
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

    fn positions_in_atoms(&self, atoms: &[&'a Atom]) -> Positions<'a> {
        atoms
            .iter()
            .fold(Positions::new(), |positions_in_atoms, atom| {
                let positions_in_atom: Positions = self.positions_in_atom(atom);
                positions_in_atoms.insert_all_take_ret(positions_in_atom)
            })
    }
}

/// This Trait provides a method to get all the Variables of an Atom.
pub trait AtomRefs {
    /// Returns all the Variables of an Atom.
    fn variables_refs(&self) -> Variables;
}

impl AtomRefs for Atom {
    fn variables_refs(&self) -> Variables {
        self.variables().collect()
    }
}

/// This Trait provides methods to get the (frontier / universal (all)) attacked Variables of a
/// Rule.
pub trait Attacked {
    /// Returns the attacked frontier Variables of a Rule.
    fn is_attacked(&self, rule: &Rule, attacked_pos_by_vars: &PositionsByRuleIdxVariables) -> bool;
    /// Returns the attacked Variables of a Rule.
    fn is_attacked_by_positions_in_rule(
        &self,
        rule: &Rule,
        attacked_pos_of_var: &Positions,
    ) -> bool;
    /// Returns the attacked Variables out of some Variables of a Rule.
    fn is_attacked_by_variable(
        &self,
        attacking_rule_idx_var: &RuleIdxVariable,
        rule: &Rule,
        attacked_pos_by_rule_idx_vars: &PositionsByRuleIdxVariables,
    ) -> bool;
}

impl Attacked for Variable {
    fn is_attacked(
        &self,
        rule: &Rule,
        attacked_pos_by_rule_idx_vars: &PositionsByRuleIdxVariables,
    ) -> bool {
        let positions_of_variable_in_body: Positions = self.positions_in_positive_body(rule);
        attacked_pos_by_rule_idx_vars
            .values()
            .any(|att_pos| att_pos.is_superset(&positions_of_variable_in_body))
    }

    fn is_attacked_by_positions_in_rule(
        &self,
        rule: &Rule,
        attacked_pos_of_var: &Positions,
    ) -> bool {
        let positions_of_variable_in_body: Positions = self.positions_in_positive_body(rule);
        attacked_pos_of_var.is_superset(&positions_of_variable_in_body)
    }

    fn is_attacked_by_variable(
        &self,
        attacking_rule_idx_var: &RuleIdxVariable,
        rule: &Rule,
        attacked_pos_by_rule_idx_vars: &PositionsByRuleIdxVariables,
    ) -> bool {
        let attacked_pos_by_var: &Positions = attacked_pos_by_rule_idx_vars
            .get(attacking_rule_idx_var)
            .unwrap();
        self.is_attacked_by_positions_in_rule(rule, attacked_pos_by_var)
    }
}

/// This Trait provides methods to get the (frontier / universal (all)) attacked glut Variables of a
/// Rule.
pub trait AttackedGlutVariables<'a>: AttackedVariables<'a> {
    /// Returns the attacked frontier glut Variables of a Rule.
    fn attacked_frontier_glut_variables(
        &self,
        attacked_pos_by_cycle_rule_idx_vars: &PositionsByRuleIdxVariables,
    ) -> Variables;
    /// Returns the attacked glut Variables of a Rule.
    fn attacked_universal_glut_variables(
        &self,
        attacked_pos_by_cycle_rule_idx_vars: &PositionsByRuleIdxVariables,
    ) -> Variables;
}

impl AttackedGlutVariables<'_> for Rule {
    fn attacked_frontier_glut_variables(
        &self,
        attacked_pos_by_cycle_rule_idx_vars: &PositionsByRuleIdxVariables,
    ) -> Variables {
        let frontier_variables: Variables = self.frontier_variables();
        self.attacked_variables(frontier_variables, attacked_pos_by_cycle_rule_idx_vars)
    }

    fn attacked_universal_glut_variables(
        &self,
        attacked_pos_by_cycle_rule_idx_vars: &PositionsByRuleIdxVariables,
    ) -> Variables {
        let universal_variables: Variables = self.positive_variables();
        self.attacked_variables(universal_variables, attacked_pos_by_cycle_rule_idx_vars)
    }
}

/// This Trait offers methods to get the attacked Variable(s) of some Rule based on the
/// attacked positions by RuleIdxVariable(s) (PositionsByRuleIdxVariables).
pub trait AttackedVariables<'a> {
    /// Returns the attacked frontier Variables of some Rule based on the attacked positions by RuleIdxVariable(s) (PositionsByRuleIdxVariables).
    fn attacked_frontier_variables(
        &self,
        attacked_pos_by_vars: &PositionsByRuleIdxVariables,
    ) -> Variables;
    /// Returns the attacked Variables of some Rule based on the attacked positions by RuleIdxVariable(s) (PositionsByRuleIdxVariables).
    fn attacked_universal_variables(
        &self,
        attacked_pos_by_vars: &PositionsByRuleIdxVariables,
    ) -> Variables;
    /// Returns the attacked Variables out of some Variables of some Rule based on the attacked positions by RuleIdxVariable(s) (PositionsByRuleIdxVariables).
    fn attacked_variables(
        &self,
        variables: Variables<'a>,
        attacked_pos_by_vars: &PositionsByRuleIdxVariables,
    ) -> Variables<'a>;
}

impl<'a> AttackedVariables<'a> for Rule {
    fn attacked_frontier_variables(
        &self,
        attacked_pos_by_rule_idx_vars: &PositionsByRuleIdxVariables,
    ) -> Variables {
        let frontier_variables: Variables = self.frontier_variables();
        self.attacked_variables(frontier_variables, attacked_pos_by_rule_idx_vars)
    }

    /// Returns the attacked universal variables of the rule.
    fn attacked_universal_variables(
        &self,
        attacked_pos_by_rule_idx_vars: &PositionsByRuleIdxVariables,
    ) -> Variables {
        let universal_variables: Variables = self.positive_variables();
        self.attacked_variables(universal_variables, attacked_pos_by_rule_idx_vars)
    }

    fn attacked_variables(
        &self,
        variables: Variables<'a>,
        attacked_pos_by_rule_idx_vars: &PositionsByRuleIdxVariables,
    ) -> Variables<'a> {
        variables
            .iter()
            .filter(|var| var.is_attacked(self, attacked_pos_by_rule_idx_vars))
            .copied()
            .collect()
    }
}

/// This Trait provides methods to get all (ExtendedPositions / Positions) of some type.
pub trait AllPositivePositions<'a> {
    /// Returns all ExtenedPositions of some type.
    fn all_positive_extended_positions(&'a self) -> ExtendedPositions<'a>;
    /// Returns all Positions of some type.
    fn all_positive_positions(&'a self) -> Positions<'a>;
}

impl<'a> AllPositivePositions<'a> for RuleSet {
    fn all_positive_extended_positions(&'a self) -> ExtendedPositions<'a> {
        let all_positive_positions: Positions = self.all_positive_positions();
        ExtendedPositions::from_positions(all_positive_positions)
    }

    fn all_positive_positions(&'a self) -> Positions<'a> {
        self.iter().fold(Positions::new(), |all_pos, rule| {
            let all_pos_of_rule: Positions = rule.all_positive_positions();
            all_pos.insert_all_take_ret(all_pos_of_rule)
        })
    }
}

impl<'a> AllPositivePositions<'a> for Rule {
    fn all_positive_extended_positions(&'a self) -> ExtendedPositions<'a> {
        let all_positive_positions: Positions = self.all_positive_positions();
        ExtendedPositions::from_positions(all_positive_positions)
    }

    fn all_positive_positions(&'a self) -> Positions<'a> {
        let all_positive_body_positions: Positions = self.all_positions_of_positive_body();
        let all_head_positions: Positions = self.all_positions_of_head();
        all_positive_body_positions.insert_all_take_ret(all_head_positions)
    }
}

trait AllPositivePositionsRulePrivate<'a> {
    fn all_positions_of_atoms(&self, atoms: &[&'a Atom]) -> Positions<'a>;
    fn all_positions_of_head(&'a self) -> Positions<'a>;
    fn all_positions_of_positive_body(&'a self) -> Positions<'a>;
}

impl<'a> AllPositivePositionsRulePrivate<'a> for Rule {
    fn all_positions_of_atoms(&self, atoms: &[&'a Atom]) -> Positions<'a> {
        atoms.iter().fold(Positions::new(), |all_pos, atom| {
            let positions: Positions =
                HashMap::from([(atom.predicate_ref(), (0..atom.len()).collect())]);
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

/// This Trait provides a method to get all existential RuleIdxVariables of a RuleSet.
pub trait ExistentialRuleIdxVariables {
    /// Returns all existential RuleIdxVariables of a RuleSet.
    fn existential_rule_idx_variables(&self) -> RuleIdxVariables;
}

impl ExistentialRuleIdxVariables for RuleSet {
    // TODO: USE EXISTENTIALRULEIDXVARIABLES FOR RULEIDXRULE
    fn existential_rule_idx_variables(&self) -> RuleIdxVariables {
        self.iter()
            .enumerate()
            .fold(RuleIdxVariables::new(), |ex_vars, (index, rule)| {
                // let idx_ex_vars: RuleIdxVariables = r_idx_rule.existential_rule_idx_variables();
                let ex_vars_of_rule: Variables = rule.existential_variables();
                let idx_ex_vars: RuleIdxVariables = ex_vars_of_rule
                    .into_iter()
                    .map(|var| (index, var))
                    .collect();
                ex_vars.union(&idx_ex_vars).copied().collect()
            })
    }
}

// impl<'a> ExistentialRuleIdxVariables for RuleIdxRule<'a> {
//     fn existential_rule_idx_variables(&self) -> RuleIdxVariables<'a> {
//         let ex_vars_of_rule: Variables = self.1.existential_variables();
//         ex_vars_of_rule.iter().map(|var| (self.0, *var)).collect()
//     }
// }

/// This Trait provides a method to get all the existential Variables of some type.
pub trait ExistentialVariables {
    /// Returns all the existential Variables of some type.
    fn existential_variables(&self) -> Variables;
}

impl ExistentialVariables for RuleSet {
    fn existential_variables(&self) -> Variables {
        self.iter().fold(Variables::new(), |ex_vars, rule| {
            let ex_vars_of_rule: Variables = rule.existential_variables();
            ex_vars.union(&ex_vars_of_rule).copied().collect()
        })
    }
}

impl ExistentialVariables for Rule {
    fn existential_variables(&self) -> Variables {
        self.variables()
            .filter(|var| var.is_existential())
            .collect()
    }
}

impl ExistentialVariables for Atom {
    fn existential_variables(&self) -> Variables {
        self.variables()
            .filter(|var| var.is_existential())
            .collect()
    }
}

/// This Trait provides some methods to get the (ExtendedPositions / Positions) of the existential
/// Variables of some type.
pub trait ExistentialVariablesPositions {
    /// Returns the ExtendedPositions of the existential Variables of some type.
    fn extended_positions_of_existential_variables(&self) -> ExtendedPositions;
    /// Returns the Positions of the existential Variables of some type.
    fn positions_of_existential_variables(&self) -> Positions;
}

impl ExistentialVariablesPositions for Rule {
    fn extended_positions_of_existential_variables(&self) -> ExtendedPositions {
        let pos_of_ex_vars: Positions = self.positions_of_existential_variables();
        ExtendedPositions::from_positions(pos_of_ex_vars)
    }

    fn positions_of_existential_variables(&self) -> Positions {
        self.existential_variables()
            .iter()
            .fold(Positions::new(), |pos_of_ex_vars, var| {
                let pos_of_var_in_head: Positions = var.positions_in_head(self);
                pos_of_ex_vars.insert_all_take_ret(pos_of_var_in_head)
            })
    }
}

/// This Trait provides a method to get the frontier Variables of a Rule.
pub trait FrontierVariables {
    /// Returns the frontier Variables of a Rule.
    fn frontier_variables(&self) -> Variables;
}

impl FrontierVariables for Rule {
    fn frontier_variables(&self) -> Variables {
        let positive_body_variables: Variables = self.positive_variables();
        let universal_head_variables: Variables = self.universal_head_variables();
        positive_body_variables
            .intersection(&universal_head_variables)
            .copied()
            .collect()
    }
}

trait FrontierVariablesPrivate {
    fn universal_head_variables(&self) -> Variables;
}

impl FrontierVariablesPrivate for Rule {
    fn universal_head_variables(&self) -> Variables {
        self.head().iter().fold(
            Variables::new(),
            |universal_head_variables: Variables, atom| {
                let un_vars_of_atom: Variables = atom.universal_variables();
                universal_head_variables.insert_all_take_ret(un_vars_of_atom)
            },
        )
    }
}

/// This Trait provides a method to check whether the Rule is guarded for some Variables.
pub trait GuardedForVariables {
    /// Checks whether the Rule is gaurded for some Variables.
    fn is_guarded_for_variables(&self, variables: Variables) -> bool;
}

impl GuardedForVariables for Rule {
    fn is_guarded_for_variables(&self, variables: Variables) -> bool {
        if variables.is_empty() {
            return true;
        }
        self.body_positive_refs().iter().any(|atom| {
            let vars_of_atom: Variables = atom.variables_refs();
            vars_of_atom.is_superset(&variables)
        })
    }
}

/// This Trait provides a method to get the join Variables of a Rule.
pub trait JoinVariables {
    /// Returns the join Variables of a Rule.
    fn join_variables(&self) -> Variables;
}

impl JoinVariables for Rule {
    fn join_variables(&self) -> Variables {
        self.positive_variables()
            .iter()
            .filter(|var| var.is_join_variable(self))
            .copied()
            .collect()
    }
}

trait JoinVariablePrivate {
    fn is_join_variable(&self, rule: &Rule) -> bool;
}

impl JoinVariablePrivate for Variable {
    fn is_join_variable(&self, rule: &Rule) -> bool {
        let mut count: usize = 0;
        rule.positive_variables_as_vec().iter().any(|var| {
            if *var == self {
                count += 1;
            }
            if 2 == count {
                return true;
            }
            false
        })
    }
}

/// This Trait provides methods to get the (ExtendedPositions / Positions) of the join Variables of
/// some Rule.
#[allow(dead_code)]
pub trait JoinVariablesPositions {
    /// Returns the ExtendedPositions of the join Variables of some Rule.
    fn extended_positions_of_join_variables(&self) -> ExtendedPositions;
    /// Returns the Positions of the join Variables of some Rule.
    fn positions_of_join_variables(&self) -> Positions;
}

// impl JoinVariablesPositions for Rule {
//     fn extended_positions_of_join_variables(&self) -> ExtendedPositions {
//         let pos_of_join_vars: Positions = self.positions_of_join_variables();
//         ExtendedPositions::from_positions(pos_of_join_vars)
//     }
//
//     fn positions_of_join_variables(&self) -> Positions {
//         self.join_variables()
//             .iter()
//             .fold(Positions::new(), |pos_of_join_vars, var| {
//                 let pos_of_var_in_body: Positions = var.positions_in_positive_body(self);
//                 pos_of_join_vars.insert_all_take_ret(pos_of_var_in_body)
//             })
//     }
// }

/// This Trait provides methods to get all pairs of frontier Variables of a Rule excluding the
/// reflexive pairs.
pub trait FrontierVariablePairs {
    /// Returns all pairs of frontier Variables of a Rule excluding the reflexive pairs.
    fn frontier_variable_pairs(&self) -> VariablePairs;
}

impl FrontierVariablePairs for Rule {
    fn frontier_variable_pairs(&self) -> VariablePairs {
        self.frontier_variables()
            .iter()
            .fold(VariablePairs::new(), |mut pairs, var1| {
                self.frontier_variables()
                    .iter()
                    .filter(|var2| var1 != *var2)
                    .for_each(|var2| {
                        pairs.insert([var1, var2]);
                    });
                pairs
            })
    }
}

/// This trait gives different checks for appearance of some Variable in some rule.
pub trait RuleAppearance {
    /// Checks if some Variable appears at multiple atoms in the positive body of some rule.
    fn appears_in_multiple_positive_body_atoms(&self, rule: &Rule) -> bool;
}

impl RuleAppearance for Variable {
    fn appears_in_multiple_positive_body_atoms(&self, rule: &Rule) -> bool {
        rule.body_positive_refs()
            .iter()
            .filter(|atom| self.appears_in_atom(atom))
            .count()
            > 1
    }
}

/// This trait gives different checks for appearance of some VariablePair in some rule.
pub trait RuleAppearancePair {
    /// Checks if some Variables of a VariablePair appear at different atoms in the positive body
    /// of some rule.
    fn appear_in_different_positive_body_atoms(&self, rule: &Rule) -> bool;
}

impl RuleAppearancePair for VariablePair<'_> {
    // TODO: REEVALUATE FUNCTION BECAUSE '2 FRONTIER VARIABLE(S) APPEAR IN DIFFERENT ATOM(S)' IS
    // NOT A CLEAR DEFINITION
    // TODO: SHORTEN FUNCTION
    fn appear_in_different_positive_body_atoms(&self, rule: &Rule) -> bool {
        let found_var1_alone: bool = rule
            .body_positive_refs()
            .iter()
            .filter(|atom| self[0].appears_in_atom(atom) && !self[1].appears_in_atom(atom))
            .count()
            > 0;
        let found_var2_alone: bool = rule
            .body_positive_refs()
            .iter()
            .filter(|atom| !self[0].appears_in_atom(atom) && self[1].appears_in_atom(atom))
            .count()
            > 0;
        found_var1_alone && found_var2_alone
    }
}

/// This Trait provides methods to get the (ExtendedPositions / Positions) of a Variable in the (positive body
/// / head) of a Rule.
pub trait RulePositions<'a> {
    /// Returns the ExtendedPositions of a Variable in the head of some Rule.
    fn extended_positions_in_head(&self, rule: &'a Rule) -> ExtendedPositions<'a>;
    /// Returns the ExtendedPositions of a Variable in the positive body of some Rule.
    fn extended_positions_in_positive_body(&self, rule: &'a Rule) -> ExtendedPositions<'a>;
    /// Returns the Positions of a Variable in the head of some Rule.
    fn positions_in_head(&self, rule: &'a Rule) -> Positions<'a>;
    /// Returns the Positions of a Variable in the positive body of some Rule.
    fn positions_in_positive_body(&self, rule: &'a Rule) -> Positions<'a>;
}

impl<'a> RulePositions<'a> for Variable {
    fn extended_positions_in_positive_body(&self, rule: &'a Rule) -> ExtendedPositions<'a> {
        let body_pos_of_var: Positions = self.positions_in_positive_body(rule);
        ExtendedPositions::from_positions(body_pos_of_var)
    }

    fn extended_positions_in_head(&self, rule: &'a Rule) -> ExtendedPositions<'a> {
        let head_pos_of_var: Positions = self.positions_in_head(rule);
        ExtendedPositions::from_positions(head_pos_of_var)
    }

    fn positions_in_positive_body(&self, rule: &'a Rule) -> Positions<'a> {
        let positive_body_atoms: Vec<&Atom> = rule.body_positive_refs();
        self.positions_in_atoms(&positive_body_atoms)
    }

    fn positions_in_head(&self, rule: &'a Rule) -> Positions<'a> {
        let head_atoms: Vec<&Atom> = rule.head_refs();
        self.positions_in_atoms(&head_atoms)
    }
}

/// This Trait provides methods to get the (positive body / head) Atom(s) as references of a Rule.
pub trait RuleRefs {
    /// Returns the positive body Atom(s) as reference(s) of a Rule.
    fn body_positive_refs(&self) -> Vec<&Atom>;
    /// Returns the head Atom(s) as reference(s) of a Rule.
    fn head_refs(&self) -> Vec<&Atom>;
}

impl RuleRefs for Rule {
    fn body_positive_refs(&self) -> Vec<&Atom> {
        self.body_positive().collect()
    }

    fn head_refs(&self) -> Vec<&Atom> {
        self.head().iter().collect()
    }
}

/// This Trait provides methods to get (affected positions (Positions) /
/// attacked (all existential / cycle existential) positions (PositionsByRuleIdxVariables) /
/// marked (common / weakly) positions (Option<Positions>)) of a RuleSet.
pub trait SpecialPositionsConstructor {
    /// Returns the affected Positions of a RuleSet.
    fn affected_positions(&self) -> Positions;
    /// Returns the attacked Positions by existential Variables that appear in a Cycle of the
    /// JointAcyclicityGraph of a RuleSet.
    fn attacked_positions_by_cycle_rule_idx_variables(&self) -> PositionsByRuleIdxVariables;
    /// Returns the attacked Positions by all existential Variables of a RuleSet.
    fn attacked_positions_by_existential_rule_idx_variables(&self) -> PositionsByRuleIdxVariables;
    /// Returns the common marking of a RuleSet.
    fn build_and_check_marking(&self) -> Option<Positions>;
    /// Returns the weakly marking of a RuleSet.
    fn build_and_check_weakly_marking(&self) -> Option<Positions>;
}

impl SpecialPositionsConstructor for RuleSet {
    fn affected_positions(&self) -> Positions {
        Positions::build_positions(self)
    }

    fn attacked_positions_by_cycle_rule_idx_variables(&self) -> PositionsByRuleIdxVariables {
        PositionsByRuleIdxVariables::build_positions(AttackingType::Cycle, self)
    }

    fn attacked_positions_by_existential_rule_idx_variables(&self) -> PositionsByRuleIdxVariables {
        PositionsByRuleIdxVariables::build_positions(AttackingType::Existential, self)
    }

    fn build_and_check_marking(&self) -> Option<Positions> {
        Option::<Positions>::build_positions(MarkingType::Common, self)
    }

    fn build_and_check_weakly_marking(&self) -> Option<Positions> {
        Option::<Positions>::build_positions(MarkingType::Weakly, self)
    }
}

/// This Trait offers a method to get the universal Variables of an Atom.
pub trait UniversalVariables {
    /// Returns the universal Variables of an Atom.
    fn universal_variables(&self) -> Variables;
}

impl UniversalVariables for Atom {
    fn universal_variables(&self) -> Variables {
        self.variables().filter(|var| var.is_universal()).collect()
    }
}
